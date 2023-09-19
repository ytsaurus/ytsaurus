#include <yt/yt/server/lib/io/chunk_file_reader.h>
#include <yt/yt/server/lib/io/chunk_file_reader_adapter.h>
#include <yt/yt/server/lib/io/chunk_file_writer.h>
#include <yt/yt/server/lib/io/io_engine.h>

#include <yt/yt/ytlib/chunk_client/config.h>
#include <yt/yt/ytlib/chunk_client/deferred_chunk_meta.h>
#include <yt/yt/ytlib/chunk_client/erasure_repair.h>
#include <yt/yt/ytlib/chunk_client/erasure_writer.h>
#include <yt/yt/ytlib/chunk_client/erasure_reader.h>
#include <yt/yt/ytlib/chunk_client/session_id.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_options.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_statistics.h>

#include <yt/yt/library/erasure/impl/codec.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/checksum.h>

#include <util/stream/file.h>

#include <util/system/fs.h>

#include <util/random/shuffle.h>

#include <random>
#include <iostream>

namespace NYT::NIO {
namespace {

using namespace NConcurrency;
using namespace NYTree;
using namespace NChunkClient;
using namespace NErasure;

////////////////////////////////////////////////////////////////////////////////

class TFailingChunkFileReaderAdapter
    : public IChunkReaderAllowingRepair
{
public:
    TFailingChunkFileReaderAdapter(
        TChunkFileReaderPtr underlying,
        int period = 5,
        bool failMetaRequests = false)
        : Underlying_(std::move(underlying))
        , Period_(period)
        , FailMetaRequests_(failMetaRequests)
    { }

    TFuture<TRefCountedChunkMetaPtr> GetMeta(
        const TClientChunkReadOptions& options,
        std::optional<int> partitionTag,
        const std::optional<std::vector<int>>& /*extensionTags*/) override
    {
        if (FailMetaRequests_ && TryFail()) {
            return MakeFuture<TRefCountedChunkMetaPtr>(TError("Failed to read meta"));
        }
        return Underlying_->GetMeta(options, partitionTag);
    }

    TChunkId GetChunkId() const override
    {
        return Underlying_->GetChunkId();
    }

    TFuture<std::vector<TBlock>> ReadBlocks(
        const TReadBlocksOptions& options,
        const std::vector<int>& blockIndexes) override
    {
        if (TryFail()) {
            return MakeFuture<std::vector<TBlock>>(TError("Failed to read blocks"));
        }
        return Underlying_->ReadBlocks(options.ClientOptions, blockIndexes);
    }

    TFuture<std::vector<TBlock>> ReadBlocks(
        const TReadBlocksOptions& options,
        int firstBlockIndex,
        int blockCount) override
    {
        if (TryFail()) {
            return MakeFuture<std::vector<TBlock>>(TError("Failed to read blocks"));
        }
        return Underlying_->ReadBlocks(options.ClientOptions, firstBlockIndex, blockCount);
    }

    TInstant GetLastFailureTime() const override
    {
        return LastFailureTime_;
    }

    void SetSlownessChecker(TCallback<TError(i64, TDuration)>) override
    { }

private:
    const TChunkFileReaderPtr Underlying_;
    const int Period_;

    int Counter_ = 0;
    std::atomic<TInstant> LastFailureTime_ = {};
    const bool FailMetaRequests_;

    bool TryFail()
    {
        ++Counter_;
        if (Counter_ == Period_) {
            LastFailureTime_ = TInstant::Now();
        }
        return LastFailureTime_.load() != TInstant();
    }
};

////////////////////////////////////////////////////////////////////////////////

std::vector<TSharedRef> GetRandomTextBlocks(int blockCount, int minBlockSize, int maxBlockSize)
{
    std::vector<TSharedRef> result;
    for (int i = 0; i < blockCount; ++i) {
        int size = minBlockSize + (maxBlockSize > minBlockSize ? std::rand() % (maxBlockSize - minBlockSize) : 0);
        auto data = TBlob(GetRefCountedTypeCookie<TDefaultBlobTag>(), size);
        for (int i = 0; i < size; ++i) {
            data[i] = static_cast<char>('a' + (std::abs(std::rand()) % 26));
        }
        result.push_back(TSharedRef::FromBlob(std::move(data)));
    }
    return result;
}

std::vector<TString> GetRandomData(std::mt19937& gen, int blocksCount, int blockSize)
{
    std::vector<TString> result;
    result.reserve(blocksCount);
    std::uniform_int_distribution<int> dist('a', 'z');
    for (int i = 0; i < blocksCount; ++i) {
        TString curData;
        curData.resize(blockSize);
        for (int i = 0; i < blockSize; ++i)
            curData[i] = dist(gen);
        result.push_back(std::move(curData));
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

TEST(TErasureCodingTest, RandomText)
{
    std::map<ECodec, int> guaranteedRepairCount;
    guaranteedRepairCount[ECodec::IsaReedSolomon_6_3] = 3;
    guaranteedRepairCount[ECodec::IsaLrc_12_2_2] = 3;

    std::vector<char> data;
    for (int i = 0; i < 16 * 64; ++i) {
        data.push_back(static_cast<char>('a' + (std::abs(std::rand()) % 26)));
    }

    for (auto codecId : TEnumTraits<ECodec>::GetDomainValues()) {
        if (codecId == ECodec::None) {
            continue;
        }

        auto* codec = FindCodec(codecId);
        if (!codec) {
            continue;
        }

        int blocksCount = codec->GetTotalPartCount();
        YT_VERIFY(blocksCount <= 16);

        std::vector<TSharedRef> dataBlocks;
        for (int i = 0; i < codec->GetDataPartCount(); ++i) {
            char* begin = data.data() + i * 64;
            auto blob = TBlob(GetRefCountedTypeCookie<TDefaultBlobTag>(), TRef(begin, 64));
            dataBlocks.push_back(TSharedRef::FromBlob(std::move(blob)));
        }

        auto parityBlocks = codec->Encode(dataBlocks);

        std::vector<TSharedRef> allBlocks(dataBlocks);
        std::copy(parityBlocks.begin(), parityBlocks.end(), std::back_inserter(allBlocks));

        for (int mask = 0; mask < (1 << blocksCount); ++mask) {
            TPartIndexList erasedIndices;
            for (int i = 0; i < blocksCount; ++i) {
                if ((mask & (1 << i)) > 0) {
                    erasedIndices.push_back(i);
                }
            }

            if (erasedIndices.size() == 1)
                continue;

            auto repairIndices = codec->GetRepairIndices(erasedIndices);
            ASSERT_EQ(static_cast<bool>(repairIndices), codec->CanRepair(erasedIndices));
            if (std::ssize(erasedIndices) <= guaranteedRepairCount[codecId]) {
                EXPECT_TRUE(repairIndices);
            }

            if (repairIndices) {
                std::vector<TSharedRef> aliveBlocks;
                for (int i = 0; i < std::ssize(*repairIndices); ++i) {
                    aliveBlocks.push_back(allBlocks[(*repairIndices)[i]]);
                }
                std::vector<TSharedRef> recoveredBlocks = codec->Decode(aliveBlocks, erasedIndices);
                EXPECT_TRUE(recoveredBlocks.size() == erasedIndices.size());
                for (int i = 0; i < std::ssize(erasedIndices); ++i) {
                    EXPECT_EQ(
                        ToString(allBlocks[erasedIndices[i]]),
                        ToString(recoveredBlocks[i]));
                }
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ETestPartInfo,
    ((OK)       (0))
    ((Failing)  (1))
    ((Erased)   (2))
);

class TErasureMixtureTest
    : public ::testing::Test
    , public ::testing::WithParamInterface<ECodec>
{
public:
    static std::vector<TSharedRef> ToSharedRefs(const std::vector<TString>& strings)
    {
        std::vector<TSharedRef> refs;
        for (const auto& str : strings) {
            refs.push_back(TSharedRef::FromString(str));
        }
        return refs;
    }

    static void WriteErasureChunk(
        ECodec codecId,
        ICodec* codec,
        std::vector<TSharedRef> data,
        int erasureWindowSize = 64,
        bool storeBlockChecksums = false,
        std::optional<i64> erasureStripeSize = std::nullopt)
    {
        auto config = New<TErasureWriterConfig>();
        config->ErasureWindowSize = erasureWindowSize;
        config->ErasureStoreOriginalBlockChecksums = storeBlockChecksums;
        config->ErasureStripeSize = erasureStripeSize;

        std::vector<IChunkWriterPtr> writers;
        auto ioEngine = CreateIOEngine(EIOEngineType::ThreadPool, INodePtr());
        for (int i = 0; i < codec->GetTotalPartCount(); ++i) {
            auto filename = "part" + ToString(i + 1);
            writers.push_back(New<TChunkFileWriter>(ioEngine, NullChunkId, filename));
        }

        auto meta = New<TDeferredChunkMeta>();
        meta->set_type(1);
        meta->set_format(1);

        i64 dataSize = 0;
        auto erasureWriter = CreateErasureWriter(
            config,
            TSessionId(),
            codecId,
            writers,
            TWorkloadDescriptor(EWorkloadCategory::UserBatch));
        EXPECT_TRUE(erasureWriter->Open().Get().IsOK());

        for (const auto& ref : data) {
            erasureWriter->WriteBlock(TWorkloadDescriptor(), TBlock(ref, GetChecksum(ref)));
            dataSize += ref.Size();
        }
        EXPECT_TRUE(erasureWriter->Close(TWorkloadDescriptor(), meta).Get().IsOK());
        EXPECT_TRUE(erasureWriter->GetChunkInfo().disk_space() >= dataSize);
    }

    static void RemoveErasedParts(const TPartIndexList& erasedIndices)
    {
        for (int i = 0; i < std::ssize(erasedIndices); ++i) {
            auto filename = "part" + ToString(erasedIndices[i] + 1);
            NFs::Remove(filename);
            NFs::Remove(filename + ".meta");
        }
    }

    static void PrepareReadersAndWriters(
        ICodec* codec,
        TPartIndexList erasedIndices,
        std::vector<IChunkReaderAllowingRepairPtr>* allReaders,
        std::vector<IChunkReaderAllowingRepairPtr>* repairReaders,
        std::vector<IChunkWriterPtr>* repairWriters)
    {
        std::set<int> erasedIndicesSet(erasedIndices.begin(), erasedIndices.end());
        auto repairIndices = *codec->GetRepairIndices(erasedIndices);
        std::set<int> repairIndicesSet(repairIndices.begin(), repairIndices.end());

        auto ioEngine = CreateIOEngine(EIOEngineType::ThreadPool, INodePtr());
        for (int i = 0; i < codec->GetTotalPartCount(); ++i) {
            auto filename = "part" + ToString(i + 1);
            if (repairWriters && erasedIndicesSet.find(i) != erasedIndicesSet.end()) {
                repairWriters->push_back(New<TChunkFileWriter>(ioEngine, NullChunkId, filename));
            }
            if (repairReaders && repairIndicesSet.find(i) != repairIndicesSet.end()) {
                auto reader = CreateChunkFileReaderAdapter(New<TChunkFileReader>(
                    ioEngine,
                    NullChunkId,
                    filename));
                repairReaders->push_back(reader);
            }

            if (allReaders &&
                erasedIndicesSet.find(i) == erasedIndicesSet.end() &&
                (i < codec->GetDataPartCount() || repairIndicesSet.find(i) != repairIndicesSet.end()))
            {
                auto reader = CreateChunkFileReaderAdapter(New<TChunkFileReader>(
                    ioEngine,
                    NullChunkId,
                    filename));
                allReaders->push_back(reader);
            }
        }
    }

    static void PrepareAdaptiveReadersAndWriters(
        ICodec* codec,
        const std::vector<ETestPartInfo>& parts,
        std::vector<IChunkReaderAllowingRepairPtr>* allReaders,
        TPartWriterFactory* writerFactory,
        bool failMetaRequests)
    {
        YT_VERIFY(codec->GetTotalPartCount() == std::ssize(parts));

        auto ioEngine = CreateIOEngine(EIOEngineType::ThreadPool, INodePtr());

        for (int index = 0; index < codec->GetTotalPartCount(); ++index) {
            auto filename = "part" + ToString(index + 1);
            auto partInfo = parts[index];
            if (partInfo != ETestPartInfo::OK) {
                auto reader = New<TChunkFileReader>(
                    ioEngine,
                    NullChunkId,
                    filename);
                allReaders->push_back(New<TFailingChunkFileReaderAdapter>(
                    reader,
                    /*period*/ 1,
                    failMetaRequests));
            } else {
                auto reader = CreateChunkFileReaderAdapter(New<TChunkFileReader>(
                    ioEngine,
                    NullChunkId,
                    filename));
                allReaders->push_back(reader);
            }
        }

        *writerFactory = [=] (int index) {
            YT_VERIFY(parts[index] == ETestPartInfo::Erased);
            auto filename = "part" + ToString(index + 1);
            return New<TChunkFileWriter>(ioEngine, NullChunkId, filename);
        };
    }

    static std::vector<IChunkReaderAllowingRepairPtr> GetChunkFileReaders(int partCount)
    {
        std::vector<IChunkReaderAllowingRepairPtr> readers;
        auto ioEngine = CreateIOEngine(EIOEngineType::ThreadPool, INodePtr());
        readers.reserve(partCount);
        for (int i = 0; i < partCount; ++i) {
            auto filename = "part" + ToString(i + 1);
            auto reader = CreateChunkFileReaderAdapter(New<TChunkFileReader>(
                ioEngine,
                NullChunkId,
                filename));
            readers.push_back(reader);
        }
        return readers;
    }

    static IChunkReaderPtr CreateErasureReader(ICodec* codec)
    {
        auto config = CreateErasureConfig();
        config->EnableAutoRepair = false;
        return CreateAdaptiveRepairingErasureReader(
            NullChunkId,
            codec,
            config,
            GetChunkFileReaders(codec->GetDataPartCount()),
            /*testingOptions*/ std::nullopt);
    }

    static TErasureReaderConfigPtr CreateErasureConfig()
    {
        return New<TErasureReaderConfig>();
    }

    static IChunkReaderPtr CreateOkRepairingReader(ICodec *codec)
    {
        return CreateAdaptiveRepairingErasureReader(
            NullChunkId,
            codec,
            CreateErasureConfig(),
            GetChunkFileReaders(codec->GetTotalPartCount()),
            /*testingOptions*/ std::nullopt);
    }

    static IChunkReaderPtr CreateRepairingErasureReader(
        NErasure::ICodec* codec,
        const NErasure::TPartIndexList& erasedIndices,
        const std::vector<IChunkReaderAllowingRepairPtr>& readers)
    {
        auto config = CreateErasureConfig();
        config->EnableAutoRepair = false;
        return CreateAdaptiveRepairingErasureReader(
            NullChunkId,
            codec,
            config,
            readers,
            TRepairingErasureReaderTestingOptions{
                .ErasedIndices = erasedIndices,
            });
    }

    static void CheckRepairReader(
        IChunkReaderPtr reparingReader,
        const std::vector<TSharedRef>& dataRefs,
        std::optional<int> maskCount)
    {
        auto check = [&] (std::vector<int> indexes) {
            Shuffle(indexes.begin(), indexes.end());
            auto result = WaitFor(reparingReader->ReadBlocks(
                /*options*/ {},
                indexes))
                .ValueOrThrow();
            EXPECT_EQ(result.size(), indexes.size());
            for (int i = 0; i < std::ssize(indexes); ++i) {
                auto resultRef = result[i];
                auto dataRef = dataRefs[indexes[i]];
                EXPECT_EQ(dataRef.Size(), resultRef.Size());
                EXPECT_EQ(ToString(dataRef), ToString(resultRef.Data));
            }
        };

        if (dataRefs.size() <= 30) {
            bool useRandom = true;
            if (!maskCount) {
                YT_VERIFY(dataRefs.size() <= 15);
                useRandom = false;
                maskCount = (1 << dataRefs.size());
            }

            for (int iter = 0; iter < *maskCount; ++iter) {
                int mask = useRandom ? (rand() % (1 << dataRefs.size())) : iter;

                std::vector<int> indexes;
                for (int i = 0; i < std::ssize(dataRefs); ++i) {
                    if (((1 << i) & mask) != 0) {
                        indexes.push_back(i);
                    }
                }

                check(indexes);
            }
        } else {
            YT_VERIFY(maskCount);
            for (int iter = 0; iter < *maskCount; ++iter) {
                std::vector<int> indexes;
                for (int i = 0; i < std::ssize(dataRefs); ++i) {
                    indexes.push_back(i);
                }
                Shuffle(indexes.begin(), indexes.end());
                indexes.resize(1 + rand() % (dataRefs.size() - 1));

                check(indexes);
            }
        }
    }

    static void CheckRepairResult(
        IChunkReaderPtr erasureReader,
        const std::vector<TSharedRef>& dataRefs)
    {
        int index = 0;
        for (const auto& ref : dataRefs) {
            auto result = erasureReader->ReadBlocks(
                /*options*/ {},
                std::vector<int>(1, index++))
                .Get();
            EXPECT_TRUE(result.IsOK());
            auto resultRef = result.ValueOrThrow().front();

            ASSERT_EQ(ToString(ref), ToString(resultRef.Data));
        }
    }

    static void Cleanup(ICodec* codec)
    {
        for (int i = 0; i < codec->GetTotalPartCount(); ++i) {
            auto filename = "part" + ToString(i + 1);
            NFs::Remove(filename.c_str());
            NFs::Remove((filename + ".meta").c_str());
        }
    }

    static std::vector<IChunkReaderAllowingRepairPtr> CreateFailingReaders(
        ICodec* codec,
        ECodec codecId,
        const std::vector<TSharedRef>& dataRefs,
        const std::vector<int>& failingTimes)
    {
        int partCount = codec->GetTotalPartCount();
        YT_VERIFY(std::ssize(failingTimes) == partCount);

        WriteErasureChunk(codecId, codec, dataRefs);

        std::vector<IChunkReaderAllowingRepairPtr> readers;
        readers.reserve(partCount);
        auto ioEngine = CreateIOEngine(EIOEngineType::ThreadPool, INodePtr());
        for (int i = 0; i < partCount; ++i) {
            auto filename = "part" + ToString(i + 1);
            if (failingTimes[i] == 0) {
                readers.push_back(CreateChunkFileReaderAdapter(New<TChunkFileReader>(
                    ioEngine,
                    NullChunkId,
                    filename)));
            } else {
                readers.push_back(New<TFailingChunkFileReaderAdapter>(New<TChunkFileReader>(
                    ioEngine,
                    NullChunkId,
                    filename),
                    failingTimes[i]));
            }
        }
        return readers;
    }

    static void ExecAdaptiveRepairTest(
        NErasure::ICodec* codec,
        const std::vector<TSharedRef>& dataRefs,
        const TPartIndexList& erasedIndices,
        const TPartIndexList& failingIndices,
        bool failMetaRequests = false);

    static std::mt19937 Gen_;
};

std::mt19937 TErasureMixtureTest::Gen_(7657457);

TEST_P(TErasureMixtureTest, Writer)
{
    auto codecId = GetParam();
    auto* codec = FindCodec(codecId);
    if (!codec) {
        return;
    }

    // Prepare data
    std::vector<TString> dataStrings = {
        "a",
        "b",
        "",
        "Hello world"};
    auto dataRefs = ToSharedRefs(dataStrings);

    WriteErasureChunk(codecId, codec, dataRefs);

    // Manually check that data in files is correct
    for (int i = 0; i < codec->GetTotalPartCount(); ++i) {
        auto filename = "part" + ToString(i + 1);
        if (i == 0) {
            EXPECT_EQ(TString("ab"), TUnbufferedFileInput("part" + ToString(i + 1)).ReadAll());
        } else if (i == 1) {
            EXPECT_EQ(TString("Hello world"), TUnbufferedFileInput("part" + ToString(i + 1)).ReadAll());
        } else if (i < 12) {
            EXPECT_EQ("", TUnbufferedFileInput("part" + ToString(i + 1)).ReadAll());
        } else {
            EXPECT_EQ(64u, TUnbufferedFileInput("part" + ToString(i + 1)).ReadAll().Size());
        }
    }

    Cleanup(codec);
}

TEST_P(TErasureMixtureTest, WriterStriped)
{
    auto codecId = GetParam();
    auto* codec = FindCodec(codecId);
    if (!codec) {
        return;
    }

    // Prepare data
    std::vector<TString> dataStrings = {
        // Stripe 0
        "0123",
        "a",
        "b"
        "",
        "012345678",
        // Stripe 1
        "x12",
        "x34"};
    auto dataRefs = ToSharedRefs(dataStrings);

    WriteErasureChunk(codecId, codec, dataRefs, 64, false, 1);

    // Manually check that data in files is correct
    for (int i = 0; i < codec->GetTotalPartCount(); ++i) {
        auto filename = "part" + ToString(i + 1);
        auto data = TUnbufferedFileInput(filename).ReadAll();
        if (i == 0) {
            EXPECT_EQ(TString("0123x12"), data);
        } else if (i == 3) {
            EXPECT_EQ(TString("a"), data);
        } else if (i == 4) {
            EXPECT_EQ(TString("b012345678"), data);
        } else if (i == 6) {
            EXPECT_EQ(TString("x34"), data);
        } else if (i < 12) {
            EXPECT_EQ("", data);
        } else {
            EXPECT_EQ(128u, data.Size());
        }
    }

    Cleanup(codec);
}

TEST_P(TErasureMixtureTest, Reader)
{
    auto codecId = GetParam();
    auto* codec = FindCodec(codecId);
    if (!codec) {
        return;
    }

    // Prepare data
    std::vector<TString> dataStrings = {
        "a",
        "b",
        "",
        "Hello world"};
    auto dataRefs = ToSharedRefs(dataStrings);

    WriteErasureChunk(codecId, codec, dataRefs);

    auto erasureReader = CreateErasureReader(codec);

    {
        // Check blocks separately
        int index = 0;
        for (const auto& ref : dataRefs) {
            auto result = erasureReader->ReadBlocks(
                /*options*/ {},
                std::vector<int>(1, index++))
                .Get();
            EXPECT_TRUE(result.IsOK());
            auto resultRef = TBlock::Unwrap(result.ValueOrThrow()).front();

            EXPECT_EQ(ToString(ref), ToString(resultRef));
        }
    }

    {
        // Check some non-trivial read request
        std::vector<int> indices;
        indices.push_back(1);
        indices.push_back(3);
        auto result = erasureReader->ReadBlocks(
            /*options*/ {},
            indices)
            .Get();
        EXPECT_TRUE(result.IsOK());
        auto resultRef = TBlock::Unwrap(result.ValueOrThrow());
        EXPECT_EQ(ToString(dataRefs[1]), ToString(resultRef[0]));
        EXPECT_EQ(ToString(dataRefs[3]), ToString(resultRef[1]));
    }

    Cleanup(codec);
}

TEST_P(TErasureMixtureTest, ReaderStriped)
{
    auto codecId = GetParam();
    auto* codec = FindCodec(codecId);
    if (!codec) {
        return;
    }

    // Prepare data
    std::vector<TString> dataStrings = {
        // Stripe 0
        "0123",
        "a",
        "b"
        "",
        "012345678",
        // Stripe 1
        "x12",
        "x34"};
    auto dataRefs = ToSharedRefs(dataStrings);

    WriteErasureChunk(codecId, codec, dataRefs, 64, false, 1);

    auto erasureReader = CreateErasureReader(codec);

    {
        // Check blocks separately
        int index = 0;
        for (const auto& ref : dataRefs) {
            auto result = erasureReader->ReadBlocks(
                /*options*/ {},
                std::vector<int>(1, index++))
                .Get();
            EXPECT_TRUE(result.IsOK());
            auto resultRef = TBlock::Unwrap(result.ValueOrThrow()).front();

            EXPECT_EQ(ToString(ref), ToString(resultRef));
        }
    }

    {
        // Check some non-trivial read request
        std::vector<int> indices;
        indices.push_back(1);
        indices.push_back(3);
        indices.push_back(5);
        auto result = erasureReader->ReadBlocks(
            /*options*/ {},
            indices)
            .Get();
        EXPECT_TRUE(result.IsOK());
        auto resultRef = TBlock::Unwrap(result.ValueOrThrow());
        EXPECT_EQ(ToString(dataRefs[1]), ToString(resultRef[0]));
        EXPECT_EQ(ToString(dataRefs[3]), ToString(resultRef[1]));
        EXPECT_EQ(ToString(dataRefs[5]), ToString(resultRef[2]));
    }

    Cleanup(codec);
}

TEST_F(TErasureMixtureTest, Repair1)
{
    auto codecId = ECodec::IsaReedSolomon_6_3;
    auto* codec = GetCodec(codecId);

    // Prepare data
    std::vector<TString> dataStrings({"a"});
    auto dataRefs = ToSharedRefs(dataStrings);

    WriteErasureChunk(codecId, codec, dataRefs);

    TPartIndexList erasedIndices;
    erasedIndices.push_back(2);

    RemoveErasedParts(erasedIndices);

    std::vector<IChunkReaderAllowingRepairPtr> allReaders;
    std::vector<IChunkReaderAllowingRepairPtr> readers;
    std::vector<IChunkWriterPtr> writers;
    PrepareReadersAndWriters(codec, erasedIndices, &allReaders, &readers, &writers);

    auto reparingReader = CreateRepairingErasureReader(codec, erasedIndices, allReaders);
    CheckRepairReader(reparingReader, dataRefs, std::nullopt);

    auto repairResult = RepairErasedParts(codec, erasedIndices, readers, writers, /*options*/ {}).Get();
    EXPECT_TRUE(repairResult.IsOK());

    auto erasureReader = CreateErasureReader(codec);
    CheckRepairResult(erasureReader, dataRefs);

    Cleanup(codec);
}

TEST_P(TErasureMixtureTest, Repair2)
{
    auto codecId = GetParam();
    auto* codec = FindCodec(codecId);
    if (!codec) {
        return;
    }

    // Prepare data
    std::vector<TString> dataStrings = {
        "a",
        "b",
        "",
        "Hello world"};
    auto dataRefs = ToSharedRefs(dataStrings);

    WriteErasureChunk(codecId, codec, dataRefs);

    TPartIndexList erasedIndices;
    erasedIndices.push_back(0);
    erasedIndices.push_back(13);

    RemoveErasedParts(erasedIndices);

    std::vector<IChunkReaderAllowingRepairPtr> allReaders;
    std::vector<IChunkReaderAllowingRepairPtr> readers;
    std::vector<IChunkWriterPtr> writers;
    PrepareReadersAndWriters(codec, erasedIndices, &allReaders, &readers, &writers);

    auto reparingReader = CreateRepairingErasureReader(codec, erasedIndices, allReaders);
    CheckRepairReader(reparingReader, dataRefs, std::nullopt);

    auto repairResult = RepairErasedParts(codec, erasedIndices, readers, writers, /*options*/ {}).Get();
    ASSERT_TRUE(repairResult.IsOK());

    auto erasureReader = CreateErasureReader(codec);
    CheckRepairResult(erasureReader, dataRefs);

    Cleanup(codec);
}

TEST_P(TErasureMixtureTest, Repair3)
{
    auto codecId = GetParam();
    auto* codec = FindCodec(codecId);
    if (!codec) {
        return;
    }

    // Prepare data (in this test we have multiple erasure windows).
    auto dataRefs = GetRandomTextBlocks(20, 100, 100);

    WriteErasureChunk(codecId, codec, dataRefs);

    {
        auto erasureReader = CreateErasureReader(codec);
        CheckRepairResult(erasureReader, dataRefs);
    }

    TPartIndexList erasedIndices;
    erasedIndices.push_back(1);
    erasedIndices.push_back(8);
    erasedIndices.push_back(13);
    erasedIndices.push_back(15);

    RemoveErasedParts(erasedIndices);

    std::vector<IChunkReaderAllowingRepairPtr> allReaders;
    std::vector<IChunkReaderAllowingRepairPtr> readers;
    std::vector<IChunkWriterPtr> writers;
    PrepareReadersAndWriters(codec, erasedIndices, &allReaders, &readers, &writers);

    auto reparingReader = CreateRepairingErasureReader(codec, erasedIndices, allReaders);
    CheckRepairReader(reparingReader, dataRefs, 100);

    RepairErasedParts(codec, erasedIndices, readers, writers, /*options*/ {}).Get();
    {
        auto erasureReader = CreateErasureReader(codec);
        CheckRepairResult(erasureReader, dataRefs);
    }

    Cleanup(codec);
}

TEST_P(TErasureMixtureTest, Repair4)
{
    auto codecId = GetParam();
    auto* codec = FindCodec(codecId);
    if (!codec) {
        return;
    }

    auto dataRefs = GetRandomTextBlocks(20, 100, 200);
    WriteErasureChunk(codecId, codec, dataRefs);

    {
        auto erasureReader = CreateErasureReader(codec);
        CheckRepairResult(erasureReader, dataRefs);
    }

    // In this test repair readers and all readers are different sets of readers.
    TPartIndexList erasedIndices;
    erasedIndices.push_back(6);

    RemoveErasedParts(erasedIndices);

    std::vector<IChunkReaderAllowingRepairPtr> allReaders;
    std::vector<IChunkReaderAllowingRepairPtr> readers;
    std::vector<IChunkWriterPtr> writers;
    PrepareReadersAndWriters(codec, erasedIndices, &allReaders, &readers, &writers);

    auto reparingReader = CreateRepairingErasureReader(codec, erasedIndices, allReaders);
    CheckRepairReader(reparingReader, dataRefs, 100);

    RepairErasedParts(codec, erasedIndices, readers, writers, /*options*/ {}).Get();
    {
        auto erasureReader = CreateErasureReader(codec);
        CheckRepairResult(erasureReader, dataRefs);
    }

    Cleanup(codec);
}

TEST_P(TErasureMixtureTest, Repair5)
{
    auto codecId = GetParam();
    auto* codec = FindCodec(codecId);
    if (!codec) {
        return;
    }

    // Prepare data (in this test we have multiple erasure windows).
    auto dataRefs = GetRandomTextBlocks(2000, 100, 100);
    WriteErasureChunk(codecId, codec, dataRefs, 256);

    {
        auto erasureReader = CreateErasureReader(codec);
        CheckRepairResult(erasureReader, dataRefs);
    }

    TPartIndexList erasedIndices;
    erasedIndices.push_back(1);
    erasedIndices.push_back(8);
    erasedIndices.push_back(13);
    erasedIndices.push_back(15);

    RemoveErasedParts(erasedIndices);

    std::vector<IChunkReaderAllowingRepairPtr> allReaders;
    std::vector<IChunkReaderAllowingRepairPtr> readers;
    std::vector<IChunkWriterPtr> writers;
    PrepareReadersAndWriters(codec, erasedIndices, &allReaders, &readers, &writers);

    auto reparingReader = CreateRepairingErasureReader(codec, erasedIndices, allReaders);
    CheckRepairReader(reparingReader, dataRefs, 40);

    RepairErasedParts(codec, erasedIndices, readers, writers, /*options*/ {}).Get();
    {
        auto erasureReader = CreateErasureReader(codec);
        CheckRepairResult(erasureReader, dataRefs);
    }

    Cleanup(codec);
}

TEST_P(TErasureMixtureTest, Repair6)
{
    auto codecId = GetParam();
    auto* codec = FindCodec(codecId);
    if (!codec) {
        return;
    }

    // Prepare data (in this test we have multiple erasure windows).
    auto dataRefs = GetRandomTextBlocks(2000, 20, 120);
    WriteErasureChunk(codecId, codec, dataRefs, 256);

    {
        auto erasureReader = CreateErasureReader(codec);
        CheckRepairResult(erasureReader, dataRefs);
    }

    TPartIndexList erasedIndices;
    erasedIndices.push_back(1);
    erasedIndices.push_back(8);
    erasedIndices.push_back(13);
    erasedIndices.push_back(15);

    RemoveErasedParts(erasedIndices);

    std::vector<IChunkReaderAllowingRepairPtr> allReaders;
    std::vector<IChunkReaderAllowingRepairPtr> readers;
    std::vector<IChunkWriterPtr> writers;
    PrepareReadersAndWriters(codec, erasedIndices, &allReaders, &readers, &writers);

    auto reparingReader = CreateRepairingErasureReader(codec, erasedIndices, allReaders);
    CheckRepairReader(reparingReader, dataRefs, 40);

    RepairErasedParts(codec, erasedIndices, readers, writers, /*options*/ {}).Get();
    {
        auto erasureReader = CreateErasureReader(codec);
        CheckRepairResult(erasureReader, dataRefs);
    }

    Cleanup(codec);
}

TEST_P(TErasureMixtureTest, RepairingReaderChecksums)
{
    auto codecId = GetParam();
    auto* codec = FindCodec(codecId);
    if (!codec) {
        return;
    }

    auto dataRefs = GetRandomTextBlocks(2000, 20, 120);

    WriteErasureChunk(codecId, codec, dataRefs, 64, true);

    {
        auto erasureReader = CreateErasureReader(codec);
        CheckRepairResult(erasureReader, dataRefs);
    }

    TPartIndexList erasedIndices;
    erasedIndices.push_back(1);
    erasedIndices.push_back(8);
    erasedIndices.push_back(13);
    erasedIndices.push_back(15);

    RemoveErasedParts(erasedIndices);

    std::vector<IChunkReaderAllowingRepairPtr> allReaders;
    std::vector<IChunkReaderAllowingRepairPtr> readers;
    std::vector<IChunkWriterPtr> writers;
    PrepareReadersAndWriters(codec, erasedIndices, &allReaders, &readers, &writers);

    auto reparingReader = CreateRepairingErasureReader(codec, erasedIndices, allReaders);
    CheckRepairReader(reparingReader, dataRefs, 20);

    Cleanup(codec);
}

TEST_P(TErasureMixtureTest, RepairStriped1)
{
    auto codecId = GetParam();
    auto* codec = FindCodec(codecId);
    if (!codec) {
        return;
    }

    // Prepare data
    std::vector<TString> dataStrings = {
        // Stripe 0
        "0123",
        "a",
        "b"
        "",
        "012345678",
        // Stripe 1
        "x12",
        "x34"};
    auto dataRefs = ToSharedRefs(dataStrings);

    WriteErasureChunk(codecId, codec, dataRefs, 64, true, 1);

    TPartIndexList erasedIndices;
    erasedIndices.push_back(0);
    erasedIndices.push_back(13);

    RemoveErasedParts(erasedIndices);

    std::vector<IChunkReaderAllowingRepairPtr> allReaders;
    std::vector<IChunkReaderAllowingRepairPtr> readers;
    std::vector<IChunkWriterPtr> writers;
    PrepareReadersAndWriters(codec, erasedIndices, &allReaders, &readers, &writers);

    auto reparingReader = CreateRepairingErasureReader(codec, erasedIndices, allReaders);
    CheckRepairReader(reparingReader, dataRefs, std::nullopt);

    auto repairResult = RepairErasedParts(codec, erasedIndices, readers, writers, /*options*/ {}).Get();
    ASSERT_TRUE(repairResult.IsOK());

    auto erasureReader = CreateErasureReader(codec);
    CheckRepairResult(erasureReader, dataRefs);

    Cleanup(codec);
}

TEST_P(TErasureMixtureTest, RepairStriped2)
{
    auto codecId = GetParam();
    auto* codec = FindCodec(codecId);
    if (!codec) {
        return;
    }

    auto dataRefs = GetRandomTextBlocks(2000, 20, 120);

    WriteErasureChunk(codecId, codec, dataRefs, 256, true, 2048);

    {
        auto erasureReader = CreateErasureReader(codec);
        CheckRepairResult(erasureReader, dataRefs);
    }

    TPartIndexList erasedIndices;
    erasedIndices.push_back(1);
    erasedIndices.push_back(8);
    erasedIndices.push_back(13);
    erasedIndices.push_back(15);

    RemoveErasedParts(erasedIndices);

    std::vector<IChunkReaderAllowingRepairPtr> allReaders;
    std::vector<IChunkReaderAllowingRepairPtr> readers;
    std::vector<IChunkWriterPtr> writers;
    PrepareReadersAndWriters(codec, erasedIndices, &allReaders, &readers, &writers);

    auto reparingReader = CreateRepairingErasureReader(codec, erasedIndices, allReaders);
    CheckRepairReader(reparingReader, dataRefs, 40);

    RepairErasedParts(codec, erasedIndices, readers, writers, /*options*/ {}).Get();

    {
        auto erasureReader = CreateErasureReader(codec);
        CheckRepairResult(erasureReader, dataRefs);
    }

    Cleanup(codec);
}

TEST_F(TErasureMixtureTest, RepairingReaderAllCorrect)
{
    auto codecId = ECodec::IsaReedSolomon_6_3;
    auto* codec = GetCodec(codecId);

    auto data = GetRandomData(Gen_, 20, 100);

    auto dataRefs = ToSharedRefs(data);
    auto reader = CreateOkRepairingReader(codec);
    WriteErasureChunk(codecId, codec, dataRefs);

    CheckRepairResult(reader, dataRefs);

    Cleanup(codec);
}

TEST_F(TErasureMixtureTest, RepairingReaderSimultaneousFail)
{
    auto codecId = ECodec::IsaReedSolomon_6_3;
    auto* codec = GetCodec(codecId);

    auto data = GetRandomData(Gen_, 20, 100);

    auto dataRefs = ToSharedRefs(data);
    WriteErasureChunk(codecId, codec, dataRefs);

    auto config = CreateErasureConfig();

    for (int i = 0; i < 10; ++i) {
        std::vector<int> failingTimes(9);
        failingTimes[0] = failingTimes[1] = failingTimes[2] = 1;
        std::shuffle(failingTimes.begin(), failingTimes.end(), Gen_);
        auto readers = CreateFailingReaders(codec, codecId, dataRefs, failingTimes);
        auto reader = CreateAdaptiveRepairingErasureReader(
            NullChunkId,
            codec,
            config,
            readers,
            /*testingOptions*/ std::nullopt);

        CheckRepairResult(reader, dataRefs);
    }

    Cleanup(codec);
}

TEST_P(TErasureMixtureTest, RepairingReaderSequenceFail)
{
    auto codecId = GetParam();
    auto* codec = FindCodec(codecId);
    if (!codec) {
        return;
    }

    auto data = GetRandomData(Gen_, 50, 5);
    auto dataRefs = ToSharedRefs(data);
    WriteErasureChunk(codecId, codec, dataRefs);

    std::vector<int> failingTimes(16);
    failingTimes[0] = 1;
    failingTimes[3] = 2;
    failingTimes[12] = 3;

    auto readers = CreateFailingReaders(codec, codecId, dataRefs, failingTimes);
    auto reader = CreateAdaptiveRepairingErasureReader(
        NullChunkId,
        codec,
        CreateErasureConfig(),
        readers,
        /*testingOptions*/ std::nullopt);

    CheckRepairResult(reader, dataRefs);

    Cleanup(codec);
}

TEST_P(TErasureMixtureTest, RepairingReaderUnrecoverable)
{
    auto codecId = ECodec::IsaReedSolomon_6_3;
    auto* codec = GetCodec(codecId);

    auto data = GetRandomData(Gen_, 20, 100);
    auto dataRefs = ToSharedRefs(data);
    WriteErasureChunk(codecId, codec, dataRefs);

    std::vector<int> failingTimes(9);
    failingTimes[1] = 1;
    failingTimes[2] = 2;
    failingTimes[3] = 3;
    failingTimes[4] = 4;

    auto readers = CreateFailingReaders(codec, codecId, dataRefs, failingTimes);
    auto reader = CreateAdaptiveRepairingErasureReader(
        NullChunkId,
        codec,
        CreateErasureConfig(),
        readers,
        /*testingOptions*/ std::nullopt);

    std::vector<int> indexes(dataRefs.size());
    std::iota(indexes.begin(), indexes.end(), 0);

    auto result = reader->ReadBlocks(/*options*/ {}, indexes).Get();
    ASSERT_FALSE(result.IsOK());

    Cleanup(codec);
}

void TErasureMixtureTest::ExecAdaptiveRepairTest(
    NErasure::ICodec* codec,
    const std::vector<TSharedRef>& dataRefs,
    const TPartIndexList& erasedIndices,
    const TPartIndexList& failingIndices,
    bool failMetaRequests)
{
    WriteErasureChunk(codec->GetId(), codec, dataRefs);
    RemoveErasedParts(erasedIndices);

    std::vector<ETestPartInfo> parts(codec->GetTotalPartCount(), ETestPartInfo::OK);
    for (auto index : erasedIndices) {
        parts[index] = ETestPartInfo::Erased;
    }
    for (auto index : failingIndices) {
        parts[index] = ETestPartInfo::Failing;
    }

    std::vector<IChunkReaderAllowingRepairPtr> allReaders;
    TPartWriterFactory writerFactory;
    PrepareAdaptiveReadersAndWriters(
        codec,
        parts,
        &allReaders,
        &writerFactory,
        failMetaRequests);

    auto repairFuture = AdaptiveRepairErasedParts(
        NullChunkId,
        codec,
        CreateErasureConfig(),
        erasedIndices,
        allReaders,
        writerFactory,
        {});
    EXPECT_TRUE(repairFuture.Get().IsOK());

    auto erasureReader = CreateErasureReader(codec);
    CheckRepairResult(erasureReader, dataRefs);

    Cleanup(codec);
}

TEST_P(TErasureMixtureTest, TestAdaptiveRepair1)
{
    auto* codec = GetCodec(ECodec::IsaReedSolomon_6_3);

    // Prepare data.
    auto data = ToSharedRefs(GetRandomData(Gen_, 20, 100));
    TPartIndexList erasedIndices = {2};
    YT_VERIFY(erasedIndices.size() == 1 && erasedIndices.front() == 2);
    TPartIndexList failingIndices = {1, 4};

    ExecAdaptiveRepairTest(codec, data, erasedIndices, failingIndices);
}

TEST_P(TErasureMixtureTest, TestAdaptiveRepair2)
{
    auto* codec = FindCodec(GetParam());
    if (!codec) {
        return;
    }

    // Prepare data.
    std::vector<TString> dataStrings = {
        "a",
        "b",
        "",
        "Hello world"};
    TPartIndexList erasedIndices = {0, 13};
    TPartIndexList failingIndices = {1, 14};

    ExecAdaptiveRepairTest(codec, ToSharedRefs(dataStrings), erasedIndices, failingIndices);
}

TEST_P(TErasureMixtureTest, TestAdaptiveRepair3)
{
    auto* codec = FindCodec(GetParam());
    if (!codec) {
        return;
    }

    auto data = GetRandomTextBlocks(20, 100, 100);

    TPartIndexList erasedIndices = {1, 8, 13};
    TPartIndexList failingIndices = {15};

    ExecAdaptiveRepairTest(codec, data, erasedIndices, failingIndices);
}

TEST_P(TErasureMixtureTest, TestAdaptiveRepair4)
{
    auto* codec = FindCodec(GetParam());
    if (!codec) {
        return;
    }

    auto data = GetRandomTextBlocks(2000, 100, 100);

    TPartIndexList erasedIndices = {1};
    TPartIndexList failingIndices = {8, 13, 15};

    ExecAdaptiveRepairTest(codec, data, erasedIndices, failingIndices);
}

TEST_P(TErasureMixtureTest, TestAdaptiveRepair5)
{
    auto* codec = FindCodec(GetParam());
    if (!codec) {
        return;
    }

    auto data = GetRandomTextBlocks(2000, 100, 100);

    TPartIndexList erasedIndices = {1};
    TPartIndexList failingIndices = {};

    ExecAdaptiveRepairTest(codec, data, erasedIndices, failingIndices);
}

TEST_P(TErasureMixtureTest, TestAdaptiveRepair6)
{
    auto* codec = FindCodec(GetParam());
    if (!codec) {
        return;
    }

    auto data = GetRandomTextBlocks(2000, 100, 100);

    TPartIndexList erasedIndices = {};
    TPartIndexList failingIndices = {8, 13, 15};

    ExecAdaptiveRepairTest(codec, data, erasedIndices, failingIndices);
}

TEST_P(TErasureMixtureTest, TestAdaptiveRepairFailingMeta)
{
    auto* codec = FindCodec(GetParam());
    if (!codec) {
        return;
    }

    auto data = GetRandomTextBlocks(2000, 100, 100);

    static const bool FailMeta = true;

    {
        TPartIndexList erasedIndices = {1, 13};
        TPartIndexList failingIndices = {0, 14};

        ExecAdaptiveRepairTest(codec, data, erasedIndices, failingIndices, FailMeta);
    }

    {
        TPartIndexList erasedIndices = {};
        TPartIndexList failingIndices = {8, 13, 15};

        ExecAdaptiveRepairTest(codec, data, erasedIndices, failingIndices, FailMeta);
    }

    {
        TPartIndexList erasedIndices = {2};
        TPartIndexList failingIndices = {1, 4};

        ExecAdaptiveRepairTest(codec, data, erasedIndices, failingIndices, FailMeta);
    }

    {
        TPartIndexList erasedIndices = {1, 8, 13};
        TPartIndexList failingIndices = {15};

        ExecAdaptiveRepairTest(codec, data, erasedIndices, failingIndices, FailMeta);
    }

    {
        TPartIndexList erasedIndices = {1};
        TPartIndexList failingIndices = {8, 13, 15};

        ExecAdaptiveRepairTest(codec, data, erasedIndices, failingIndices, FailMeta);
    }
}

INSTANTIATE_TEST_SUITE_P(
    TErasureMixtureTest,
    TErasureMixtureTest,
    ::testing::Values(ECodec::Lrc_12_2_2, ECodec::IsaLrc_12_2_2));

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NIO
