#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/ytree/public.h>
#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/ytlib/chunk_client/io_engine.h>

#include <util/system/fs.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

class TIOEngineTest
    : public ::testing::Test
    , public ::testing::WithParamInterface<std::tuple<
        EIOEngineType,
        const char*>>
{
protected:
    TSharedMutableRef GenerateRandomBlob(i64 size)
    {
        auto data = TSharedMutableRef::Allocate(size, false);
        ::memset(data.Begin(), 0xdeadbeaf, data.Size());
        return data;
    }
};

TEST_P(TIOEngineTest, ReadWrite)
{
    const auto& args = GetParam();
    const auto& type = std::get<0>(args);
    const auto config = NYTree::ConvertTo<NYTree::INodePtr>(
        NYson::TYsonString(TString(std::get<1>(args))));

    auto engine = CreateIOEngine(type, config);
    auto fileName = GenerateRandomFileName("IOEngineTestWrite");
    auto file = engine->Open(fileName, RdWr | CreateAlways)
        .Get()
        .ValueOrThrow();

    constexpr auto S = 4_KB;
    auto data = GenerateRandomBlob(S);

    auto write = [&] {
        engine->Write(IIOEngine::TWriteRequest{*file, 0, data})
            .Get()
            .ThrowOnError();
    };

    auto read = [&] (i64 offset, i64 size) {
        auto dataRead = TSharedMutableRef::Allocate(size);
        engine->Read(IIOEngine::TReadRequest{*file, offset, dataRead})
            .Get()
            .ThrowOnError();
        EXPECT_TRUE(TRef::AreBitwiseEqual(dataRead, data.Slice(offset, offset + size)));
    };

    write();
    file->Resize(17);
    read(0, 17);
    write();
    read(0, S);
    read(0, S - 7);
    read(7, S - 7);
    read(100, 0);
    read(S, 0);
    EXPECT_THROW({
        read(S, 10);
        read(S - 10, 20);
    }, TErrorException);
}

TEST_P(TIOEngineTest, ReadAll)
{
    const auto& args = GetParam();
    const auto& type = std::get<0>(args);
    const auto config = NYTree::ConvertTo<NYTree::INodePtr>(
        NYson::TYsonString(TString(std::get<1>(args))));

    auto engine = CreateIOEngine(type, config);
    auto fileName = GenerateRandomFileName("IOEngineTestReadAll");

    auto data = GenerateRandomBlob(124097);

    {
        TFile file(fileName, WrOnly | CreateAlways);
        file.Pwrite(data.Begin(), data.Size(), 0);
    }

    auto readData = engine->ReadAll(fileName)
        .Get()
        .ValueOrThrow();

    EXPECT_TRUE(TRef::AreBitwiseEqual(readData, data));
}

INSTANTIATE_TEST_SUITE_P(
    TIOEngineTest,
    TIOEngineTest,
    ::testing::Values(
        std::make_tuple(EIOEngineType::ThreadPool, "{ }")
    )
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
