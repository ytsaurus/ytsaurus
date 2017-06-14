#include "framework.h"

#include <yt/ytlib/chunk_client/config.h>
#include <yt/ytlib/chunk_client/erasure_reader.h>
#include <yt/ytlib/chunk_client/erasure_writer.h>
#include <yt/ytlib/chunk_client/file_reader.h>
#include <yt/ytlib/chunk_client/file_writer.h>

#include <yt/core/erasure/codec.h>
#include <yt/core/misc/checksum.h>

#include <util/stream/file.h>

#include <util/system/fs.h>

#include <random>

namespace NYT {
namespace NErasure {
namespace {

using namespace NChunkClient;
using namespace NChunkClient::NProto;
using ::ToString;

////////////////////////////////////////////////////////////////////////////////

TEST(TErasureCodingTest, RandomText)
{
    std::map<ECodec, int> guaranteedRecoveryCount;
    guaranteedRecoveryCount[ECodec::ReedSolomon_6_3] = 3;
    guaranteedRecoveryCount[ECodec::Lrc_12_2_2] = 3;

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

            auto recoveryIndices = codec->GetRepairIndices(erasedIndices);
            ASSERT_EQ(static_cast<bool>(recoveryIndices), codec->CanRepair(erasedIndices));
            if (erasedIndices.size() <= guaranteedRecoveryCount[codecId]) {
                EXPECT_TRUE(recoveryIndices.HasValue());
            }

            if (recoveryIndices) {
                std::vector<TSharedRef> aliveBlocks;
                for (int i = 0; i < recoveryIndices->size(); ++i) {
                    aliveBlocks.push_back(allBlocks[(*recoveryIndices)[i]]);
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

    static void WriteErasureChunk(ECodec codecId, ICodec* codec, std::vector<TSharedRef> data)
    {
        auto config = NYT::New<TErasureWriterConfig>();
        config->ErasureWindowSize = 64;

        std::vector<IChunkWriterPtr> writers;
        for (int i = 0; i < codec->GetTotalPartCount(); ++i) {
            auto filename = "part" + ToString(i + 1);
            writers.push_back(NYT::New<TFileWriter>(NullChunkId, filename));
        }

        TChunkMeta meta;
        meta.set_type(1);
        meta.set_version(1);

        i64 dataSize = 0;
        auto erasureWriter = CreateErasureWriter(config, NullChunkId, codecId, codec, writers);
        EXPECT_TRUE(erasureWriter->Open().Get().IsOK());

        for (const auto& ref : data) {
            erasureWriter->WriteBlock(TBlock(ref, GetChecksum(ref)));
            dataSize += ref.Size();
        }
        EXPECT_TRUE(erasureWriter->Close(meta).Get().IsOK());
        EXPECT_TRUE(erasureWriter->GetChunkInfo().disk_space() >= dataSize);
    }

    static IChunkReaderPtr CreateErasureReader(ICodec* codec)
    {
        std::vector<IChunkReaderPtr> readers;
        for (int i = 0; i < codec->GetDataPartCount(); ++i) {
            auto filename = "part" + ToString(i + 1);
            auto reader = NYT::New<TFileReader>(NullChunkId, filename);
            readers.push_back(reader);
        }
        return CreateNonRepairingErasureReader(readers);
    }

    static void Cleanup(ICodec* codec)
    {
        for (int i = 0; i < codec->GetTotalPartCount(); ++i) {
            auto filename = "part" + ToString(i + 1);
            NFs::Remove(filename.c_str());
            NFs::Remove((filename + ".meta").c_str());
        }
    }
};

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
            EXPECT_EQ(TString("ab"), TFileInput("part" + ToString(i + 1)).ReadAll());
        } else if (i == 1) {
            EXPECT_EQ(TString("Hello world"), TFileInput("part" + ToString(i + 1)).ReadAll());
        } else if (i < 12) {
            EXPECT_EQ("", TFileInput("part" + ToString(i + 1)).ReadAll());
        } else {
            EXPECT_EQ(64, TFileInput("part" + ToString(i + 1)).ReadAll().Size());
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
            auto result = erasureReader->ReadBlocks(TWorkloadDescriptor(), std::vector<int>(1, index++)).Get();
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
        auto result = erasureReader->ReadBlocks(TWorkloadDescriptor(), indices).Get();
        EXPECT_TRUE(result.IsOK());
        auto resultRef = TBlock::Unwrap(result.ValueOrThrow());
        EXPECT_EQ(ToString(dataRefs[1]), ToString(resultRef[0]));
        EXPECT_EQ(ToString(dataRefs[3]), ToString(resultRef[1]));
    }

    Cleanup(codec);
}

// TODO(ignat): refactor this tests to eliminate copy-paste
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

    std::set<int> erasedIndicesSet(erasedIndices.begin(), erasedIndices.end());

    auto repairIndices = *codec->GetRepairIndices(erasedIndices);
    std::set<int> repairIndicesSet(repairIndices.begin(), repairIndices.end());

    for (int i = 0; i < erasedIndices.size(); ++i) {
        auto filename = "part" + ToString(erasedIndices[i] + 1);
        NFs::Remove(filename.c_str());
        NFs::Remove((filename + ".meta").c_str());
    }

    std::vector<IChunkReaderPtr> readers;
    std::vector<IChunkWriterPtr> writers;
    for (int i = 0; i < codec->GetTotalPartCount(); ++i) {
        auto filename = "part" + ToString(i + 1);
        if (erasedIndicesSet.find(i) != erasedIndicesSet.end()) {
            writers.push_back(NYT::New<TFileWriter>(NullChunkId, filename));
        }
        if (repairIndicesSet.find(i) != repairIndicesSet.end()) {
            auto reader = NYT::New<TFileReader>(NullChunkId, filename);
            readers.push_back(reader);
        }
    }

    auto repairResult = RepairErasedParts(codec, erasedIndices, readers, writers, TWorkloadDescriptor()).Get();
    EXPECT_TRUE(repairResult.IsOK());

    auto erasureReader = CreateErasureReader(codec);

    int index = 0;
    for (const auto& ref : dataRefs) {
        auto result = erasureReader->ReadBlocks(TWorkloadDescriptor(), std::vector<int>(1, index++)).Get();
        EXPECT_TRUE(result.IsOK());
        auto resultRef = result.ValueOrThrow().front();

        EXPECT_EQ(ToString(ref), ToString(resultRef.Data));
    }

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

    std::set<int> erasedIndicesSet(erasedIndices.begin(), erasedIndices.end());

    auto repairIndices = *codec->GetRepairIndices(erasedIndices);
    std::set<int> repairIndicesSet(repairIndices.begin(), repairIndices.end());

    for (int i = 0; i < erasedIndices.size(); ++i) {
        auto filename = "part" + ToString(erasedIndices[i] + 1);
        NFs::Remove(filename.c_str());
        NFs::Remove((filename + ".meta").c_str());
    }

    std::vector<IChunkReaderPtr> readers;
    std::vector<IChunkWriterPtr> writers;
    for (int i = 0; i < codec->GetTotalPartCount(); ++i) {
        auto filename = "part" + ToString(i + 1);
        if (erasedIndicesSet.find(i) != erasedIndicesSet.end()) {
            writers.push_back(NYT::New<TFileWriter>(NullChunkId, filename));
        }
        if (repairIndicesSet.find(i) != repairIndicesSet.end()) {
            auto reader = NYT::New<TFileReader>(NullChunkId, filename);
            readers.push_back(reader);
        }
    }

    auto repairResult = RepairErasedParts(codec, erasedIndices, readers, writers, TWorkloadDescriptor()).Get();
    EXPECT_TRUE(repairResult.IsOK());

    auto erasureReader = CreateErasureReader(codec);

    int index = 0;
    for (const auto& ref : dataRefs) {
        auto result = erasureReader->ReadBlocks(TWorkloadDescriptor(), std::vector<int>(1, index++)).Get();
        EXPECT_TRUE(result.IsOK());
        auto resultRef = result.ValueOrThrow().front();

        EXPECT_EQ(ToString(ref), ToString(resultRef.Data));
    }

    Cleanup(codec);
}

TEST_F(TErasureMixture, RepairTestWithSeveralWindows)
{
    auto codecId = ECodec::Lrc_12_2_2;
    auto codec = GetCodec(codecId);

    // Prepare data
    std::vector<TSharedRef> dataRefs;
    for (int i = 0; i < 20; ++i) {
        auto data = NYT::TBlob(NYT::TDefaultBlobTag(), 100);
        for (int i = 0; i < 100; ++i) {
            data[i] = static_cast<char>('a' + (std::abs(std::rand()) % 26));
        }
        dataRefs.push_back(TSharedRef::FromBlob(std::move(data)));
    }
    WriteErasureChunk(codecId, codec, dataRefs);

    { // Check reader
        auto erasureReader = CreateErasureReader(codec);
        for (int i = 0; i < dataRefs.size(); ++i ) {
            auto result = erasureReader->ReadBlocks(TWorkloadDescriptor(), std::vector<int>(1, i)).Get();
            EXPECT_TRUE(result.IsOK());

            auto resultRef = result.Value().front();
            EXPECT_EQ(ToString(dataRefs[i]), ToString(resultRef.Data));
        }
    }

    TPartIndexList erasedIndices;
    erasedIndices.push_back(1);
    erasedIndices.push_back(8);
    erasedIndices.push_back(13);
    erasedIndices.push_back(15);
    std::set<int> erasedIndicesSet(erasedIndices.begin(), erasedIndices.end());

    auto repairIndices = *codec->GetRepairIndices(erasedIndices);
    std::set<int> repairIndicesSet(repairIndices.begin(), repairIndices.end());

    for (int i = 0; i < erasedIndices.size(); ++i) {
        auto filename = "part" + ToString(erasedIndices[i] + 1);
        NFs::Remove(filename.c_str());
        NFs::Remove((filename + ".meta").c_str());
    }

    std::vector<IChunkReaderPtr> readers;
    std::vector<IChunkWriterPtr> writers;
    for (int i = 0; i < codec->GetTotalPartCount(); ++i) {
        auto filename = "part" + ToString(i + 1);
        if (erasedIndicesSet.find(i) != erasedIndicesSet.end()) {
            writers.push_back(NYT::New<TFileWriter>(NullChunkId, filename));
        }
        if (repairIndicesSet.find(i) != repairIndicesSet.end()) {
            auto reader = NYT::New<TFileReader>(NullChunkId, filename);
            readers.push_back(reader);
        }
    }

    RepairErasedParts(codec, erasedIndices, readers, writers, TWorkloadDescriptor()).Get();

    { // Check reader
        auto erasureReader = CreateErasureReader(codec);
        for (int i = 0; i < dataRefs.size(); ++i ) {
            auto result = erasureReader->ReadBlocks(TWorkloadDescriptor(), std::vector<int>(1, i)).Get();
            EXPECT_TRUE(result.IsOK());

            auto resultRef = result.Value().front();
            EXPECT_EQ(dataRefs[i].Size(), resultRef.Size());
            EXPECT_EQ(ToString(dataRefs[i]), ToString(resultRef.Data));
        }
    }

    Cleanup(codec);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NErasure
} // namespace NYT
