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
        const char*,
        bool
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

    void SetUp() override
    {
        auto supportedTypes = GetSupportedIOEngineTypes();
        auto type = GetIOEngineType();
        if (std::find(supportedTypes.begin(), supportedTypes.end(), type) == supportedTypes.end()) {
            GTEST_SKIP() << Format("Skipping Test: IOEngine %v is not supported.", type);
        }
    }

    bool UseDedicatedAllocations()
    {
        return std::get<2>(GetParam());
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
        auto result = engine->Read({{file, offset, size}}, EWorkloadCategory::Idle, {}, {}, UseDedicatedAllocations())
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
    if (GetIOEngineType() != EIOEngineType::Uring) {
        GTEST_SKIP() << "Skipping Test: Unaligned direct IO is only supported by uring engine.";
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
        auto result = engine->Read({{file, offset, size}}, EWorkloadCategory::Idle, {}, {}, UseDedicatedAllocations())
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
    if (GetIOEngineType() != EIOEngineType::Uring) {
        GTEST_SKIP() << "Skipping Test: Unaligned direct IO is only supported by uring engine.";
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
        futures.push_back(engine->Read({{file, 10, 20}}, EWorkloadCategory::Idle, {}, {}, UseDedicatedAllocations()));
    }

    AllSucceeded(std::move(futures))
        .Get()
        .ThrowOnError();
}

TEST_P(TIOEngineTest, DirectIOAligned)
{
    auto engine = CreateIOEngine();

    auto fileName = GenerateRandomFileName("IOEngine");
    TTempFile tempFile(fileName);

    constexpr auto S = 4_MB;
    auto data = GenerateRandomBlob(S);

    WriteFile(fileName, data);

    auto file = engine->Open({fileName, RdOnly | DirectAligned})
        .Get()
        .ValueOrThrow();

    auto read = [&] (std::vector<IIOEngine::TReadRequest> requests) {
        for (auto& request : requests) {
            request.Handle = file;
        }
        auto result = engine->Read(requests, EWorkloadCategory::Idle, {}, {}, UseDedicatedAllocations())
            .Get()
            .ValueOrThrow();

        EXPECT_TRUE(result.OutputBuffers.size() == requests.size());
        for (int index = 0; index < std::ssize(requests); ++index) {
            const auto& request = requests[index];
            EXPECT_TRUE(TRef::AreBitwiseEqual(
                result.OutputBuffers[index],
                data.Slice(request.Offset, request.Offset + request.Size)));
        }
    };

    read({
        {.Offset=16_KB, .Size=32_KB},
        {.Offset=0, .Size=4_KB},
        {.Offset=4_KB, .Size=32_KB},
        {.Offset=S-4_KB, .Size=4_KB},
    });

    read({
        {.Offset=0, .Size=S},
    });

    read({
        {.Offset=0, .Size=1_MB},
        {.Offset=2_MB, .Size=2_MB},
    });
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

bool AllocatorBehaviourCollocate(false);
bool AllocatorBehaviourSeparate(true);

INSTANTIATE_TEST_SUITE_P(
    TIOEngineTest,
    TIOEngineTest,
    ::testing::Values(
        std::make_tuple(EIOEngineType::ThreadPool, DefaultConfig, AllocatorBehaviourCollocate),
        std::make_tuple(EIOEngineType::ThreadPool, CustomConfig, AllocatorBehaviourCollocate),
        std::make_tuple(EIOEngineType::ThreadPool, DefaultConfig, AllocatorBehaviourSeparate),

        std::make_tuple(EIOEngineType::FairShareThreadPool, DefaultConfig, AllocatorBehaviourCollocate),
        std::make_tuple(EIOEngineType::FairShareThreadPool, CustomConfig, AllocatorBehaviourCollocate),
        std::make_tuple(EIOEngineType::FairShareThreadPool, DefaultConfig, AllocatorBehaviourSeparate),

        std::make_tuple(EIOEngineType::Uring, DefaultConfig, AllocatorBehaviourCollocate),
        std::make_tuple(EIOEngineType::Uring, CustomConfig, AllocatorBehaviourCollocate),
        std::make_tuple(EIOEngineType::Uring, DefaultConfig, AllocatorBehaviourSeparate)
    )
);

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NIO
