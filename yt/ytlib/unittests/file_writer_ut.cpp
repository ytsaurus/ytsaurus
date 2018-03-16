#include <yt/core/test_framework/framework.h>

#include <yt/core/ytree/public.h>
#include <yt/core/ytree/convert.h>

#include <yt/ytlib/chunk_client/io_engine.h>
#include <yt/ytlib/chunk_client/file_writer.h>
#include <yt/ytlib/chunk_client/block.h>

#include <yt/core/misc/fs.h>

#include <util/system/fs.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

class TWriteTest
    : public ::testing::Test
        , public ::testing::WithParamInterface<std::tuple<
        EIOEngineType,
        const char*>>
{
protected:
    virtual void SetUp() override
    { }

    TBlock RandomBlock(size_t size)
    {
        auto data = TSharedMutableRef::Allocate(size, false);
        for (int i = 0; i < size; ++i) {
            data[i] = rand();
        }
        return TBlock(data);
    }

    bool CheckBlock(TFile& file, const TBlock& block)
    {
        auto data = TSharedMutableRef::Allocate(block.Data.Size(), false);
        file.Load(data.Begin(), data.Size());
        return ::memcmp(block.Data.Begin(), data.Begin(), data.Size()) == 0;
    }
};

TEST_P(TWriteTest, Simple)
{
    const auto& args = GetParam();
    const auto& type = std::get<0>(args);
    const auto config = NYTree::ConvertTo<NYTree::INodePtr>(
        NYson::TYsonString(std::get<1>(args), NYson::EYsonType::Node));

    auto engine = CreateIOEngine(type, config);
    TString fileName = GenerateRandomFileName("TFileWriterTest");
    TString tmpFileName = fileName + NFS::TempFileSuffix;

    TFileWriter writer(engine, TGuid::Create(), fileName);

    writer.Open().Get().ThrowOnError();

    TFile tmpFile(tmpFileName, RdOnly);

    TBlock block1(RandomBlock(10));
    EXPECT_EQ(writer.WriteBlock(block1), true);
    TBlock block2(RandomBlock(10));
    EXPECT_EQ(writer.WriteBlock(block2), true);
    TBlock block3(RandomBlock(4096));
    EXPECT_EQ(writer.WriteBlock(block3), true);
    TBlock block4(RandomBlock(1_MB+1));
    EXPECT_EQ(writer.WriteBlock(block4), true);
    TBlock block5(RandomBlock(5_MB+1));
    EXPECT_EQ(writer.WriteBlock(block5), true);

    EXPECT_EQ(tmpFile.GetLength(), 6_MB + 2*4096);

    EXPECT_TRUE(CheckBlock(tmpFile, block1));
    EXPECT_TRUE(CheckBlock(tmpFile, block2));
    EXPECT_TRUE(CheckBlock(tmpFile, block3));
    EXPECT_TRUE(CheckBlock(tmpFile, block4));
    EXPECT_TRUE(CheckBlock(tmpFile, block5));

    writer.Close(NChunkClient::NProto::TChunkMeta());

    TFile file(fileName, RdOnly);
    EXPECT_EQ(file.GetLength(), block1.Size() + block2.Size() + block3.Size() + block4.Size() + block5.Size());
}

INSTANTIATE_TEST_CASE_P(
    TWriteTest,
    TWriteTest,
    ::testing::Values(
        std::make_tuple(EIOEngineType::ThreadPool, "{ }"),
        std::make_tuple(EIOEngineType::ThreadPool, "{ use_direct_io = true; }"),
        std::make_tuple(EIOEngineType::Aio, "{ }")
    )
);


////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
