#include <util/random/shuffle.h>
#include <yt/core/test_framework/framework.h>

#include <yt/ytlib/chunk_client/config.h>
#include <yt/ytlib/chunk_client/erasure_repair.h>
#include <yt/ytlib/chunk_client/erasure_writer.h>
#include <yt/ytlib/chunk_client/repairing_reader.h>
#include <yt/ytlib/chunk_client/file_reader.h>
#include <yt/ytlib/chunk_client/file_writer.h>
#include <yt/ytlib/chunk_client/session_id.h>

#include <yt/core/erasure/codec.h>
#include <yt/core/misc/checksum.h>

#include <util/stream/file.h>

#include <util/system/fs.h>

#include <random>

#include <iostream>

namespace NYT {
namespace NErasure {
namespace {

using namespace NConcurrency;
using namespace NChunkClient;
using ::ToString;

class TTestingFileReader
    : public TFileReader
{
public:
    TTestingFileReader(
        const IIOEnginePtr& ioEngine,
        const TChunkId& chunkId,
        const TString& fileName,
        bool validateBlocksChecksums = true)
        : TFileReader(ioEngine, chunkId, fileName, validateBlocksChecksums)
    { }

    virtual void SetSlownessChecker(TCallback<TError(i64, TDuration)>) override
    { }
};

class TFailingFileReader
    : public TFileReader
{
public:
    TFailingFileReader(
        const IIOEnginePtr& ioEngine,
        const TChunkId& chunkId,
        const TString& fileName,
        int period = 5,
        bool validateBlocksChecksums = true)
        : TFileReader(ioEngine, chunkId, fileName, validateBlocksChecksums)
        , IsFailed_(false)
        , Period_(period)
        , Counter_(0)
    { }

    virtual TFuture<std::vector<TBlock>> ReadBlocks(
        const TWorkloadDescriptor& workloadDescriptor,
        const TReadSessionId& readSessionId,
        const std::vector<int>& blockIndexes) override
    {
        if (TryFail()) {
            return MakeFuture(MakeError());
        }
        return TFileReader::ReadBlocks(workloadDescriptor, readSessionId, blockIndexes);
    }

    virtual TFuture<std::vector<TBlock>> ReadBlocks(
        const TWorkloadDescriptor& workloadDescriptor,
        const TReadSessionId& readSessionId,
        int firstBlockIndex,
        int blockCount) override
    {
        if (TryFail()) {
            return MakeFuture(MakeError());
        }
        return TFileReader::ReadBlocks(workloadDescriptor, readSessionId, firstBlockIndex, blockCount);
    }

    virtual bool IsValid() const override
    {
        return !IsFailed_;
    }

    virtual void SetSlownessChecker(TCallback<TError(i64, TDuration)>) override
    { }

private:
    bool IsFailed_;
    int Period_;
    int Counter_;

    bool TryFail()
    {
        ++Counter_;
        IsFailed_ = IsFailed_ || Counter_ == Period_;
        return IsFailed_;
    }

    TErrorOr<std::vector<TBlock>> MakeError()
    {
        return TError("Shit happens");
    }
};

////////////////////////////////////////////////////////////////////////////////

std::vector<TString> GetRandomData(std::mt19937& gen, int blocksCount, int blockSize)
{
    std::vector<TString> result;
    result.reserve(blocksCount);
    std::uniform_int_distribution<char> dist('a', 'z');
    for (int i = 0; i < blocksCount; ++i) {
        TString curData;
        curData.resize(blockSize);
        for (int i = 0; i < blockSize; ++i)
            curData[i] = dist(gen);
        result.push_back(std::move(curData));
    }
    return result;
}

TEST(TErasureCodingTest, RandomText)
{
    std::map<ECodec, int> guaranteedRepairCount;
    guaranteedRepairCount[ECodec::ReedSolomon_6_3] = 3;
    guaranteedRepairCount[ECodec::Lrc_12_2_2] = 3;

    std::vector<char> data;
    for (int i = 0; i < 16 * 64; ++i) {
        data.push_back(static_cast<char>('a' + (std::abs(std::rand()) % 26)));
    }

    for (auto codecId : TEnumTraits<ECodec>::GetDomainValues()) {
        if (codecId == ECodec::None) {
            continue;
        }

        auto codec = GetCodec(codecId);

        int blocksCount = codec->GetTotalPartCount();
        YCHECK(blocksCount <= 16);

        std::vector<TSharedRef> dataBlocks;
        for (int i = 0; i < codec->GetDataPartCount(); ++i) {
            char* begin = data.data() + i * 64;
            auto blob = NYT::TBlob(NYT::TDefaultBlobTag(), begin, 64);
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
            if (erasedIndices.size() <= guaranteedRepairCount[codecId]) {
                EXPECT_TRUE(repairIndices.HasValue());
            }

            if (repairIndices) {
                std::vector<TSharedRef> aliveBlocks;
                for (int i = 0; i < repairIndices->size(); ++i) {
                    aliveBlocks.push_back(allBlocks[(*repairIndices)[i]]);
                }
                std::vector<TSharedRef> recoveredBlocks = codec->Decode(aliveBlocks, erasedIndices);
                EXPECT_TRUE(recoveredBlocks.size() == erasedIndices.size());
                for (int i = 0; i < erasedIndices.size(); ++i) {
                    EXPECT_EQ(
                        ToString(allBlocks[erasedIndices[i]]),
                        ToString(recoveredBlocks[i]));
                }
            }
        }

    }
}

class TErasureMixture
    : public ::testing::Test
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

    static void WriteErasureChunk(ECodec codecId, ICodec* codec, std::vector<TSharedRef> data, int erasureWindowSize = 64)
    {
        auto config = NYT::New<TErasureWriterConfig>();
        config->ErasureWindowSize = erasureWindowSize;

        std::vector<IChunkWriterPtr> writers;
        for (int i = 0; i < codec->GetTotalPartCount(); ++i) {
            auto filename = "part" + ToString(i + 1);
            writers.push_back(NYT::New<TFileWriter>(NullChunkId, filename));
        }

        NChunkClient::NProto::TChunkMeta meta;
        meta.set_type(1);
        meta.set_version(1);

        i64 dataSize = 0;
        auto erasureWriter = CreateErasureWriter(
            config,
            TSessionId(),
            codecId,
            codec,
            writers,
            TWorkloadDescriptor(EWorkloadCategory::UserBatch));
        EXPECT_TRUE(erasureWriter->Open().Get().IsOK());

        for (const auto& ref : data) {
            erasureWriter->WriteBlock(TBlock(ref, GetChecksum(ref)));
            dataSize += ref.Size();
        }
        EXPECT_TRUE(erasureWriter->Close(meta).Get().IsOK());
        EXPECT_TRUE(erasureWriter->GetChunkInfo().disk_space() >= dataSize);
    }

    static void RemoveErasedParts(const TPartIndexList& erasedIndices)
    {
        for (int i = 0; i < erasedIndices.size(); ++i) {
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

        auto ioEngine = CreateIOEngine(EIOEngineType::ThreadPool, NYTree::INodePtr());
        for (int i = 0; i < codec->GetTotalPartCount(); ++i) {
            auto filename = "part" + ToString(i + 1);
            if (repairWriters && erasedIndicesSet.find(i) != erasedIndicesSet.end()) {
                repairWriters->push_back(NYT::New<TFileWriter>(NullChunkId, filename));
            }
            if (repairReaders && repairIndicesSet.find(i) != repairIndicesSet.end()) {
                auto reader = NYT::New<TTestingFileReader>(ioEngine, NullChunkId, filename);
                repairReaders->push_back(reader);
            }

            if (allReaders &&
                erasedIndicesSet.find(i) == erasedIndicesSet.end() &&
                (i < codec->GetDataPartCount() || repairIndicesSet.find(i) != repairIndicesSet.end()))
            {
                auto reader = NYT::New<TTestingFileReader>(ioEngine, NullChunkId, filename);
                allReaders->push_back(reader);
            }
        }
    }

    static std::vector<IChunkReaderAllowingRepairPtr> GetFileReaders(int partCount)
    {
        std::vector<IChunkReaderAllowingRepairPtr> readers;
        auto ioEngine = CreateIOEngine(EIOEngineType::ThreadPool, NYTree::INodePtr());
        readers.reserve(partCount);
        for (int i = 0; i < partCount; ++i) {
            auto filename = "part" + ToString(i + 1);
            auto reader = NYT::New<TTestingFileReader>(ioEngine, NullChunkId, filename);
            readers.push_back(reader);
        }
        return readers;
    }

    static IChunkReaderPtr CreateErasureReader(ICodec* codec)
    {
        auto config = CreateErasureConfig();
        config->EnableAutoRepair = false;
        return CreateRepairingReader(codec, config, GetFileReaders(codec->GetDataPartCount()));
    }

    static TErasureReaderConfigPtr CreateErasureConfig()
    {
        return New<TErasureReaderConfig>();
    }

    static IChunkReaderPtr CreateOkRepairingReader(ICodec *codec)
    {
        return CreateRepairingReader(codec, CreateErasureConfig(), GetFileReaders(codec->GetTotalPartCount()));
    }

    static void CheckRepairReader(
        IChunkReaderPtr repairReader,
        const std::vector<TSharedRef>& dataRefs,
        TNullable<int> maskCount)
    {
        auto check = [&] (std::vector<int> indexes) {
            Shuffle(indexes.begin(), indexes.end());
            auto result = WaitFor(repairReader->ReadBlocks(TWorkloadDescriptor(), TReadSessionId(), indexes))
                .ValueOrThrow();
            EXPECT_EQ(result.size(), indexes.size());
            for (int i = 0; i < indexes.size(); ++i) {
                auto resultRef = result[i];
                auto dataRef = dataRefs[indexes[i]];
                EXPECT_EQ(dataRef.Size(), resultRef.Size());
                EXPECT_EQ(ToString(dataRef), ToString(resultRef.Data));
            }
        };

        if (dataRefs.size() <= 30) {
            bool useRandom = true;
            if (!maskCount) {
                YCHECK(dataRefs.size() <= 15);
                useRandom = false;
                maskCount = (1 << dataRefs.size());
            }

            for (int iter = 0; iter < *maskCount; ++iter) {
                int mask = useRandom ? (rand() % (1 << dataRefs.size())) : iter;

                std::vector<int> indexes;
                for (int i = 0; i < dataRefs.size(); ++i) {
                    if (((1 << i) & mask) != 0) {
                        indexes.push_back(i);
                    }
                }

                check(indexes);
            }
        } else {
            YCHECK(maskCount);
            for (int iter = 0; iter < *maskCount; ++iter) {
                std::vector<int> indexes;
                for (int i = 0; i < dataRefs.size(); ++i) {
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
                TWorkloadDescriptor(),
                TReadSessionId(),
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
        YCHECK(failingTimes.size() == partCount);

        WriteErasureChunk(codecId, codec, dataRefs);

        std::vector<IChunkReaderAllowingRepairPtr> readers;
        readers.reserve(partCount);
        auto ioEngine = CreateIOEngine(EIOEngineType::ThreadPool, NYTree::INodePtr());
        for (int i = 0; i < partCount; ++i) {
            auto filename = "part" + ToString(i + 1);
            if (failingTimes[i] == 0) {
                readers.push_back(NYT::New<TTestingFileReader>(ioEngine, NullChunkId, filename));
            } else {
                readers.push_back(NYT::New<TFailingFileReader>(ioEngine, NullChunkId, filename, failingTimes[i]));
            }
        }
        return readers;
    }

    static std::mt19937 Gen_;
};
std::mt19937 TErasureMixture::Gen_(7657457);

TEST_F(TErasureMixture, WriterTest)
{
    auto codecId = ECodec::Lrc_12_2_2;
    auto codec = GetCodec(codecId);

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
            EXPECT_EQ(64, TUnbufferedFileInput("part" + ToString(i + 1)).ReadAll().Size());
        }
    }

    Cleanup(codec);
}

TEST_F(TErasureMixture, ReaderTest)
{
    auto codecId = ECodec::Lrc_12_2_2;
    auto codec = GetCodec(codecId);

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
                TWorkloadDescriptor(),
                TReadSessionId(),
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
            TWorkloadDescriptor(),
            TReadSessionId(),
            indices)
            .Get();
        EXPECT_TRUE(result.IsOK());
        auto resultRef = TBlock::Unwrap(result.ValueOrThrow());
        EXPECT_EQ(ToString(dataRefs[1]), ToString(resultRef[0]));
        EXPECT_EQ(ToString(dataRefs[3]), ToString(resultRef[1]));
    }

    Cleanup(codec);
}

TEST_F(TErasureMixture, RepairTest1)
{
    auto codecId = ECodec::ReedSolomon_6_3;
    auto codec = GetCodec(codecId);

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

    auto repairReader = CreateRepairingErasureReader(codec, erasedIndices, allReaders);
    CheckRepairReader(repairReader, dataRefs, Null);

    auto repairResult = RepairErasedParts(codec, erasedIndices, readers, writers, TWorkloadDescriptor()).Get();
    EXPECT_TRUE(repairResult.IsOK());

    auto erasureReader = CreateErasureReader(codec);
    CheckRepairResult(erasureReader, dataRefs);

    Cleanup(codec);
}

TEST_F(TErasureMixture, RepairTest2)
{
    auto codecId = ECodec::Lrc_12_2_2;
    auto codec = GetCodec(ECodec::Lrc_12_2_2);

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

    auto repairReader = CreateRepairingErasureReader(codec, erasedIndices, allReaders);
    CheckRepairReader(repairReader, dataRefs, Null);

    auto repairResult = RepairErasedParts(codec, erasedIndices, readers, writers, TWorkloadDescriptor()).Get();
    ASSERT_TRUE(repairResult.IsOK());

    auto erasureReader = CreateErasureReader(codec);
    CheckRepairResult(erasureReader, dataRefs);

    Cleanup(codec);
}

TEST_F(TErasureMixture, RepairTest3)
{
    auto codecId = ECodec::Lrc_12_2_2;
    auto codec = GetCodec(codecId);

    // Prepare data (in this test we have multiple erasure windows).
    std::vector<TSharedRef> dataRefs;
    for (int i = 0; i < 20; ++i) {
        auto data = NYT::TBlob(NYT::TDefaultBlobTag(), 100);
        for (int i = 0; i < 100; ++i) {
            data[i] = static_cast<char>('a' + (std::abs(std::rand()) % 26));
        }
        dataRefs.push_back(TSharedRef::FromBlob(std::move(data)));
    }
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

    auto repairReader = CreateRepairingErasureReader(codec, erasedIndices, allReaders);
    CheckRepairReader(repairReader, dataRefs, 100);

    RepairErasedParts(codec, erasedIndices, readers, writers, TWorkloadDescriptor()).Get();
    {
        auto erasureReader = CreateErasureReader(codec);
        CheckRepairResult(erasureReader, dataRefs);
    }

    Cleanup(codec);
}

TEST_F(TErasureMixture, RepairTest4)
{
    auto codecId = ECodec::Lrc_12_2_2;
    auto codec = GetCodec(codecId);

    // Prepare data
    std::vector<TSharedRef> dataRefs;
    for (int i = 0; i < 20; ++i) {
        int size = 100 + (std::rand() % 100);
        auto data = NYT::TBlob(NYT::TDefaultBlobTag(), size);
        for (int i = 0; i < size; ++i) {
            data[i] = static_cast<char>('a' + (std::abs(std::rand()) % 26));
        }
        dataRefs.push_back(TSharedRef::FromBlob(std::move(data)));
    }
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

    auto repairReader = CreateRepairingErasureReader(codec, erasedIndices, allReaders);
    CheckRepairReader(repairReader, dataRefs, 100);

    RepairErasedParts(codec, erasedIndices, readers, writers, TWorkloadDescriptor()).Get();
    {
        auto erasureReader = CreateErasureReader(codec);
        CheckRepairResult(erasureReader, dataRefs);
    }

    Cleanup(codec);
}

TEST_F(TErasureMixture, RepairTest5)
{
    auto codecId = ECodec::Lrc_12_2_2;
    auto codec = GetCodec(codecId);

    // Prepare data (in this test we have multiple erasure windows).
    std::vector<TSharedRef> dataRefs;
    for (int i = 0; i < 2000; ++i) {
        auto data = NYT::TBlob(NYT::TDefaultBlobTag(), 100);
        for (int i = 0; i < 100; ++i) {
            data[i] = static_cast<char>('a' + (std::abs(std::rand()) % 26));
        }
        dataRefs.push_back(TSharedRef::FromBlob(std::move(data)));
    }
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

    auto repairReader = CreateRepairingErasureReader(codec, erasedIndices, allReaders);
    CheckRepairReader(repairReader, dataRefs, 40);

    RepairErasedParts(codec, erasedIndices, readers, writers, TWorkloadDescriptor()).Get();
    {
        auto erasureReader = CreateErasureReader(codec);
        CheckRepairResult(erasureReader, dataRefs);
    }

    Cleanup(codec);
}

TEST_F(TErasureMixture, RepairTest6)
{
    auto codecId = ECodec::Lrc_12_2_2;
    auto codec = GetCodec(codecId);

    // Prepare data (in this test we have multiple erasure windows).
    std::vector<TSharedRef> dataRefs;
    for (int i = 0; i < 2000; ++i) {
        auto data = NYT::TBlob(NYT::TDefaultBlobTag(), 20 + (std::rand() % 100));
        for (int i = 0; i < data.Size(); ++i) {
            data[i] = static_cast<char>('a' + (std::abs(std::rand()) % 26));
        }
        dataRefs.push_back(TSharedRef::FromBlob(std::move(data)));
    }
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

    auto repairReader = CreateRepairingErasureReader(codec, erasedIndices, allReaders);
    CheckRepairReader(repairReader, dataRefs, 40);

    RepairErasedParts(codec, erasedIndices, readers, writers, TWorkloadDescriptor()).Get();
    {
        auto erasureReader = CreateErasureReader(codec);
        CheckRepairResult(erasureReader, dataRefs);
    }

    Cleanup(codec);
}

TEST_F(TErasureMixture, RepairingReaderAllCorrect)
{
    auto codecId = ECodec::ReedSolomon_6_3;
    auto codec = GetCodec(codecId);
    auto data = GetRandomData(Gen_, 20, 100);

    auto dataRefs = ToSharedRefs(data);
    auto reader = CreateOkRepairingReader(codec);
    WriteErasureChunk(codecId, codec, dataRefs);

    CheckRepairResult(reader, dataRefs);

    Cleanup(codec);
}

TEST_F(TErasureMixture, RepairingReaderSimultaneousFail)
{
    auto codecId = ECodec::ReedSolomon_6_3;
    auto codec = GetCodec(codecId);
    auto data = GetRandomData(Gen_, 20, 100);

    auto dataRefs = ToSharedRefs(data);
    WriteErasureChunk(codecId, codec, dataRefs);

    auto config = CreateErasureConfig();

    for (int i = 0; i < 10; ++i) {
        std::vector<int> failingTimes(9);
        failingTimes[0] = failingTimes[1] = failingTimes[2] = 1;
        std::shuffle(failingTimes.begin(), failingTimes.end(), Gen_);
        auto readers = CreateFailingReaders(codec, codecId, dataRefs, failingTimes);
        auto reader = CreateRepairingReader(codec, config, readers);

        CheckRepairResult(reader, dataRefs);
    }

    Cleanup(codec);
}

TEST_F(TErasureMixture, RepairingReaderSequenceFail)
{
    auto codecId = ECodec::Lrc_12_2_2;
    auto codec = GetCodec(codecId);
    auto data = GetRandomData(Gen_, 50, 5);
    auto dataRefs = ToSharedRefs(data);
    WriteErasureChunk(codecId, codec, dataRefs);

    std::vector<int> failingTimes(16);
    failingTimes[0] = 1;
    failingTimes[3] = 2;
    failingTimes[12] = 3;

    auto readers = CreateFailingReaders(codec, codecId, dataRefs, failingTimes);
    auto reader = CreateRepairingReader(codec, CreateErasureConfig(), readers);

    CheckRepairResult(reader, dataRefs);

    Cleanup(codec);
}

TEST_F(TErasureMixture, RepairingReaderUnrecoverable)
{
    auto codecId = ECodec::ReedSolomon_6_3;
    auto codec = GetCodec(codecId);
    auto data = GetRandomData(Gen_, 20, 100);
    auto dataRefs = ToSharedRefs(data);
    WriteErasureChunk(codecId, codec, dataRefs);

    std::vector<int> failingTimes(9);
    failingTimes[1] = 1;
    failingTimes[2] = 2;
    failingTimes[3] = 3;
    failingTimes[4] = 4;

    auto readers = CreateFailingReaders(codec, codecId, dataRefs, failingTimes);
    auto reader = CreateRepairingReader(codec, CreateErasureConfig(), readers);

    std::vector<int> indexes(dataRefs.size());
    std::iota(indexes.begin(), indexes.end(), 0);

    auto result = reader->ReadBlocks(TWorkloadDescriptor(), TReadSessionId(), indexes).Get();
    ASSERT_FALSE(result.IsOK());

    Cleanup(codec);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NErasure
} // namespace NYT
