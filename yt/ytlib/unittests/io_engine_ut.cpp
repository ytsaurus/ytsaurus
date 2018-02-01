#include <yt/core/test_framework/framework.h>

#include <yt/core/ytree/public.h>
#include <yt/core/ytree/convert.h>

#include <yt/ytlib/chunk_client/io_engine.h>
#include <yt/ytlib/chunk_client/public.h>

#include <yt/server/data_node/public.h>

#include <util/system/fs.h>

namespace NYT {
namespace NChunkClient {

using namespace NDataNode;

////////////////////////////////////////////////////////////////////////////////

class TReadWriteTest
    : public ::testing::Test
    , public ::testing::WithParamInterface<std::tuple<
        EIOEngineType,
        const char*>>
{
protected:
    virtual void SetUp() override
    { }

    TSharedMutableRef Allocate(size_t size)
    {
        const int alignment = 4096;
        auto data = TSharedMutableRef::Allocate(size + 3 * alignment, false);
        data = data.Slice(AlignUp(data.Begin(), alignment), data.End());
        return data.Slice(data.Begin(), data.Begin() + size);
    }
};

TEST_P(TReadWriteTest, Simple)
{
    const auto& args = GetParam();
    const auto& type = std::get<0>(args);
    const auto config = NYTree::ConvertTo<NYTree::INodePtr>(
        NYson::TYsonString(std::get<1>(args), NYson::EYsonType::Node));

    auto engine = CreateIOEngine(type, config);
    TString fName = GenerateRandomFileName("IOEngineTestWrite");
    auto file = engine->Open(fName, RdWr | CreateAlways);

    auto data = Allocate(4096);
    ::memset(data.Begin(), 0xdeadbeaf, data.Size());
    engine->Pwrite(file, data, 0).Get().ThrowOnError();

    auto readData = engine->Pread(file, data.Size(), 0).Get().ValueOrThrow();
    EXPECT_EQ(readData.Size(), data.Size());
    EXPECT_EQ(::memcmp(readData.Begin(), data.Begin(), data.Size()), 0);

    readData = engine->Pread(file, data.Size() - 7, 0).Get().ValueOrThrow();
    EXPECT_EQ(readData.Size(), data.Size() - 7);
    EXPECT_EQ(::memcmp(readData.Begin(), data.Begin(), readData.Size()), 0);

    readData = engine->Pread(file, data.Size(), 7).Get().ValueOrThrow();
    EXPECT_EQ(readData.Size(), data.Size() - 7);
    EXPECT_EQ(::memcmp(readData.Begin(), data.Begin() + 7, readData.Size()), 0);

    data = Allocate((1<<30) + 4096);
    ::memset(data.Begin(), 0xdeadbeaf, data.Size());
    engine->Pwrite(file, data, 4096).Get().ThrowOnError();

    readData = engine->Pread(file, data.Size(), 4096).Get().ValueOrThrow();
    EXPECT_EQ(readData.Size(), data.Size());
    EXPECT_EQ(::memcmp(readData.Begin(), data.Begin(), data.Size()), 0);

    readData = engine->Pread(file, data.Size() - 7, 4096).Get().ValueOrThrow();
    EXPECT_EQ(readData.Size(), data.Size() - 7);
    EXPECT_EQ(::memcmp(readData.Begin(), data.Begin(), readData.Size()), 0);

    readData = engine->Pread(file, data.Size(), 4096 + 7).Get().ValueOrThrow();
    EXPECT_EQ(readData.Size(), data.Size() - 7);
    EXPECT_EQ(::memcmp(readData.Begin(), data.Begin() + 7, readData.Size()), 0);
}

INSTANTIATE_TEST_CASE_P(
    TReadWriteTest,
    TReadWriteTest,
    ::testing::Values(
        std::make_tuple(EIOEngineType::ThreadPool, "{ }"),
        std::make_tuple(EIOEngineType::ThreadPool, "{ use_direct_io = true; }"),
        std::make_tuple(EIOEngineType::Aio, "{ }")
    )
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
