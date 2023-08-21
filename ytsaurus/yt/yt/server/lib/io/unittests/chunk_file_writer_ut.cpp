#include <yt/yt/server/lib/io/chunk_file_writer.h>
#include <yt/yt/server/lib/io/io_engine.h>

#include <yt/yt/ytlib/chunk_client/block.h>
#include <yt/yt/ytlib/chunk_client/deferred_chunk_meta.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/core/misc/fs.h>

#include <util/system/fs.h>

#include <util/random/random.h>

namespace NYT::NIO {
namespace {

using namespace NChunkClient;

////////////////////////////////////////////////////////////////////////////////

using TChunkFileWriterTestParams = std::tuple<
    EIOEngineType,
    const char*
>;

class TChunkFileWriterTest
    : public ::testing::Test
    , public ::testing::WithParamInterface<TChunkFileWriterTestParams>
{
protected:
    EIOEngineType GetIOEngineType()
    {
        return std::get<0>(GetParam());
    }

    TChunkFileWriterPtr CreateWriter(const TChunkFileWriterTestParams& /*params*/)
    {
        auto type = GetIOEngineType();
        auto config = NYTree::ConvertTo<NYTree::INodePtr>(
            NYson::TYsonString(TString(std::get<1>(GetParam()))));

        auto engine = CreateIOEngine(type, config);
        auto fileName = GenerateRandomFileName("TChunkFileWriterTest");
        auto tmpFileName = fileName + NFS::TempFileSuffix;

        return New<TChunkFileWriter>(engine, TGuid::Create(), fileName);
    }


    void SetUp() override
    {
        auto supportedTypes = GetSupportedIOEngineTypes();
        auto type = GetIOEngineType();
        if (std::find(supportedTypes.begin(), supportedTypes.end(), type) == supportedTypes.end()) {
            GTEST_SKIP() << Format("Skipping Test: IOEngine %v is not supported.", type);
        }
    }

    std::unique_ptr<TFile> OpenDataFile(const TChunkFileWriterPtr& writer)
    {
        return std::make_unique<TFile>(writer->GetFileName(), RdOnly);
    }

    std::unique_ptr<TFile> OpenTempDataFile(const TChunkFileWriterPtr& writer)
    {
        return std::make_unique<TFile>(writer->GetFileName() + NFS::TempFileSuffix, RdOnly);
    }

    TBlock MakeRandomBlock(ssize_t size)
    {
        auto data = TSharedMutableRef::Allocate(size, {.InitializeStorage = false});
        for (int i = 0; i < size; ++i) {
            data[i] = RandomNumber<ui8>();
        }
        return TBlock(data);
    }

    void CheckBlock(TFile& file, const TBlock& block)
    {
        auto data = TSharedMutableRef::Allocate(block.Data.Size(), {.InitializeStorage = false});
        file.Load(data.Begin(), data.Size());
        EXPECT_EQ(0, ::memcmp(block.Data.Begin(), data.Begin(), data.Size()));
    }

    void WriteBlock(const IChunkWriterPtr& writer, const TBlock& block)
    {
        EXPECT_FALSE(writer->WriteBlock(TWorkloadDescriptor(), block));
        EXPECT_TRUE(writer->GetReadyEvent().Get().IsOK());
    }

    void WriteBlocks(const IChunkWriterPtr& writer, const std::vector<TBlock>& blocks)
    {
        EXPECT_FALSE(writer->WriteBlocks(TWorkloadDescriptor(), blocks));
        EXPECT_TRUE(writer->GetReadyEvent().Get().IsOK());
    }

    static i64 GetTotalSize(const std::vector<TBlock>& blocks)
    {
        i64 result = 0;
        for (const auto& block : blocks) {
            result += block.Data.Size();
        }
        return result;
    }
};

TEST_P(TChunkFileWriterTest, SingleWrite)
{
    auto writer = CreateWriter(GetParam());

    writer->Open()
        .Get()
        .ThrowOnError();

    auto tmpFile = OpenTempDataFile(writer);

    std::vector<TBlock> blocks{
        MakeRandomBlock(10),
        MakeRandomBlock(10),
        MakeRandomBlock(4096),
        MakeRandomBlock(1_MB + 1),
        MakeRandomBlock(5_MB + 1)
    };

    for (const auto& block : blocks) {
        WriteBlock(writer, block);
    }

    EXPECT_EQ(GetTotalSize(blocks), writer->GetDataSize());

    for (const auto& block : blocks) {
        CheckBlock(*tmpFile, block);
    }

    writer->Close(TWorkloadDescriptor(), New<NChunkClient::TDeferredChunkMeta>())
        .Get()
        .ThrowOnError();

    auto file = OpenDataFile(writer);
    EXPECT_EQ(GetTotalSize(blocks), file->GetLength());
}

TEST_P(TChunkFileWriterTest, MultiWrite)
{
    auto writer = CreateWriter(GetParam());

    writer->Open()
        .Get()
        .ThrowOnError();

    auto tmpFile = OpenTempDataFile(writer);

    std::vector<TBlock> blocks{
        MakeRandomBlock(10),
        MakeRandomBlock(10),
        MakeRandomBlock(4096),
        MakeRandomBlock(1_MB + 1),
        MakeRandomBlock(5_MB + 1)
    };

    WriteBlocks(writer, blocks);

    EXPECT_EQ(GetTotalSize(blocks), writer->GetDataSize());

    for (const auto& block : blocks) {
        CheckBlock(*tmpFile, block);
    }

    writer->Close(TWorkloadDescriptor(), New<NChunkClient::TDeferredChunkMeta>())
        .Get()
        .ThrowOnError();

    auto file = OpenDataFile(writer);
    EXPECT_EQ(GetTotalSize(blocks), file->GetLength());
}

TEST_P(TChunkFileWriterTest, Specific)
{
    auto writer = CreateWriter(GetParam());

    writer->Open()
        .Get()
        .ThrowOnError();

    auto tmpFile = OpenTempDataFile(writer);

    constexpr int BlockCount = 3;
    std::vector<TBlock> blocks;
    blocks.reserve(BlockCount);

    int sizes[] = {1338, 1495, 1457};

    for (int i = 0; i < BlockCount; ++i) {
        int blockSize = sizes[i];
        auto block = MakeRandomBlock(blockSize);
        blocks.push_back(block);
        WriteBlock(writer, block);

        tmpFile->Seek(0, sSet);
        for (int j = 0; j < std::ssize(blocks); ++j) {
            CheckBlock(*tmpFile, blocks[j]);
        }
    }
}

TEST_P(TChunkFileWriterTest, Random)
{
    auto writer = CreateWriter(GetParam());

    writer->Open()
        .Get()
        .ThrowOnError();

    auto tmpFile = OpenTempDataFile(writer);

    std::vector<TBlock> blocks;
    constexpr int BlockCount = 10;
    blocks.reserve(BlockCount);

    for (int i = 0; i < BlockCount; ++i) {
        int blockSize = 1_MB * (1 + RandomNumber<double>());
        auto block = MakeRandomBlock(blockSize);
        blocks.push_back(block);
        WriteBlock(writer, block);

        tmpFile->Seek(0, sSet);
        for (int j = 0; j < std::ssize(blocks); ++j) {
            CheckBlock(*tmpFile, blocks[j]);
        }
    }
}

INSTANTIATE_TEST_SUITE_P(
    TChunkFileWriterTest,
    TChunkFileWriterTest,
    ::testing::Values(
        std::make_tuple(EIOEngineType::ThreadPool, "{ max_bytes_per_write = 65536; }"),
        std::make_tuple(EIOEngineType::ThreadPool, "{ max_bytes_per_write = 65536; enable_pwritev = %false; }"),
        std::make_tuple(EIOEngineType::Uring, "{ }")
    )
);

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NIO
