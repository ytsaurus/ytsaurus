#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/server/lib/io/io_engine.h>

#include <util/system/fs.h>
#include <util/system/tempfile.h>

namespace NYT::NIO {
namespace {

////////////////////////////////////////////////////////////////////////////////

class TIOEngineTest
    : public ::testing::Test
    , public ::testing::WithParamInterface<std::tuple<
        EIOEngineType,
        const char*
    >>
{
protected:
    TSharedMutableRef GenerateRandomBlob(i64 size)
    {
        auto data = TSharedMutableRef::Allocate(size);
        for (ui32 index = 0; index < data.Size(); ++index) {
            data[index] = ((index + size) * 1103515245L) >> 24;
        }
        return data;
    }

    void WriteFile(const TString& fileName, TRef data)
    {
        TFile file(fileName, WrOnly | CreateAlways);
        file.Pwrite(data.Begin(), data.Size(), 0);
    }

    EIOEngineType GetIOEngineType()
    {
        return std::get<0>(GetParam());
    }

    IIOEnginePtr CreateIOEngine()
    {
        auto type = GetIOEngineType();
        auto config = NYTree::ConvertTo<NYTree::INodePtr>(
            NYson::TYsonString(TString(std::get<1>(GetParam()))));
        return NIO::CreateIOEngine(type, config);
    }

    virtual void SetUp()
    {
        auto supportedTypes = GetSupportedIOEngineTypes();
        auto type = GetIOEngineType();
        if (std::find(supportedTypes.begin(), supportedTypes.end(), type) == supportedTypes.end()) {
            GTEST_SKIP();
        }
    }
};

TEST_P(TIOEngineTest, ReadWrite)
{
    auto engine = CreateIOEngine();

    auto fileName = GenerateRandomFileName("IOEngine");
    TTempFile tempFile(fileName);

    auto file = engine->Open({fileName, RdWr | CreateAlways})
        .Get()
        .ValueOrThrow();

    constexpr auto S = 64_KB;
    auto data = GenerateRandomBlob(S);

    auto write = [&] {
        engine->Write({file, 0, {data}})
            .Get()
            .ThrowOnError();
    };

    auto flush = [&] {
        engine->FlushFile({file, EFlushFileMode::Data})
            .Get()
            .ThrowOnError();
    };

    auto read = [&] (i64 offset, i64 size) {
        auto result = engine->Read({{file, offset, size}})
            .Get()
            .ValueOrThrow();
        EXPECT_TRUE(result.OutputBuffers.size() == 1);
        EXPECT_TRUE(TRef::AreBitwiseEqual(result.OutputBuffers[0], data.Slice(offset, offset + size)));
    };

    write();
    flush();
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
    }, TErrorException);
    EXPECT_THROW({
        read(S - 10, 20);
    }, TErrorException);
}

TEST_P(TIOEngineTest, ReadAll)
{
    auto engine = CreateIOEngine();

    auto fileName = GenerateRandomFileName("IOEngine");
    TTempFile tempFile(fileName);

    constexpr auto S = 124097;
    auto data = GenerateRandomBlob(S);

    WriteFile(fileName, data);

    auto readData = engine->ReadAll(fileName)
        .Get()
        .ValueOrThrow();

    EXPECT_TRUE(TRef::AreBitwiseEqual(readData, data));
}

TEST_P(TIOEngineTest, DirectIO)
{
    // TODO(babenko): direct IO is only supported by uring engine.
    if (GetIOEngineType() != EIOEngineType::Uring) {
        GTEST_SKIP();
    }

    auto engine = CreateIOEngine();

    auto fileName = GenerateRandomFileName("IOEngine");
    TTempFile tempFile(fileName);

    constexpr auto S = 64_KB;
    auto data = GenerateRandomBlob(S);

    WriteFile(fileName, data);

    auto file = engine->Open({fileName, RdOnly | DirectAligned})
        .Get()
        .ValueOrThrow();

    auto read = [&] (i64 offset, i64 size) {
        auto result = engine->Read({{file, offset, size}})
            .Get()
            .ValueOrThrow();
        EXPECT_TRUE(result.OutputBuffers.size() == 1);
        EXPECT_TRUE(TRef::AreBitwiseEqual(result.OutputBuffers[0], data.Slice(offset, offset + size)));
    };

    read(1, S - 2);
    read(0, S);
    read(4_KB, 8_KB);

    read(0, S);
    read(100, 200);
    read(4_KB - 1, 2);
    read(4_KB - 1, 4_KB + 2);
}

TEST_P(TIOEngineTest, ManyConcurrentDirectIOReads)
{
    // TODO(babenko): direct IO is only supported by uring engine.
    if (GetIOEngineType() != EIOEngineType::Uring) {
        GTEST_SKIP();
    }

    auto engine = CreateIOEngine();

    auto fileName = GenerateRandomFileName("IOEngine");
    TTempFile tempFile(fileName);

    constexpr auto S = 4_KB;
    auto data = GenerateRandomBlob(S);

    WriteFile(fileName, data);

    auto file = engine->Open({fileName, RdOnly | DirectAligned})
        .Get()
        .ValueOrThrow();

    std::vector<TFuture<IIOEngine::TReadResponse>> futures;
    constexpr auto N = 100;

    for (int i = 0; i < N; ++i) {
        futures.push_back(engine->Read({{file, 10, 20}}));
    }

    AllSucceeded(std::move(futures))
        .Get()
        .ThrowOnError();
}

const char DefaultConfig[] =
    "{"
    "}";

const char CustomConfig[] =
    "{"
    "    simulated_max_bytes_per_read = 4096;"
    "    simulated_max_bytes_per_write = 4096;"
    "    large_unaligned_direct_io_read_size = 16384;"
    "}";

INSTANTIATE_TEST_SUITE_P(
    TIOEngineTest,
    TIOEngineTest,
    ::testing::Values(
        std::make_tuple(EIOEngineType::ThreadPool, DefaultConfig),
        std::make_tuple(EIOEngineType::ThreadPool, CustomConfig),
        std::make_tuple(EIOEngineType::Uring, DefaultConfig),
        std::make_tuple(EIOEngineType::Uring, CustomConfig)
    )
);

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NIO
