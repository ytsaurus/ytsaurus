#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/misc/proc.h>

#include <yt/yt/core/net/connection.h>

#include <yt/yt/library/backtrace_introspector/introspect.h>

#include <yt/yt/library/pipe_io/pipe.h>

#include <yt/yt/library/program/program.h>

#include <yt/yt/library/signals/signal_blocking.h>

#include <library/cpp/yt/memory/blob.h>

#include <random>


namespace NYT::NPipeIO {

////////////////////////////////////////////////////////////////////////////////

using namespace NConcurrency;
using namespace NNet;

#ifndef _win_

//! NB: You can't set size smaller than that of a page.
constexpr int SmallPipeCapacity = 4096;

TEST(TPipeIOHolderTest, CanInstantiate)
{
    auto pipe = TPipeFactory().Create();

    auto readerHolder = pipe.CreateAsyncReader();
    auto writerHolder = pipe.CreateAsyncWriter();

    readerHolder->Abort().Get();
    writerHolder->Abort().Get();
}

TEST(TPipeTest, PrematureEOF)
{
    auto pipe = TNamedPipe::Create("./namedpipe");
    auto reader = pipe->CreateAsyncReader();

    auto buffer = TSharedMutableRef::Allocate(1024 * 1024);
    EXPECT_THROW(reader->Read(buffer).WithTimeout(TDuration::Seconds(1)).Get().ValueOrThrow(), TErrorException);
}

////////////////////////////////////////////////////////////////////////////////

TBlob ReadAll(IConnectionReaderPtr reader, bool useWaitFor)
{
    auto buffer = TSharedMutableRef::Allocate(1_MB, {.InitializeStorage = false});
    auto whole = TBlob(GetRefCountedTypeCookie<TDefaultBlobTag>());

    while (true)  {
        TErrorOr<size_t> result;
        auto future = reader->Read(buffer);
        if (useWaitFor) {
            result = WaitFor(future);
        } else {
            result = future.Get();
        }

        if (result.ValueOrThrow() == 0) {
            break;
        }

        whole.Append(buffer.Begin(), result.Value());
    }
    return whole;
}

void WriteAll(IConnectionWriterPtr writer, const char* data, size_t size, size_t blockSize)
{
    while (size > 0) {
        const size_t currentBlockSize = std::min(blockSize, size);
        auto buffer = TSharedRef(data, currentBlockSize, nullptr);
        auto error = WaitFor(writer->Write(buffer));
        THROW_ERROR_EXCEPTION_IF_FAILED(error);
        size -= currentBlockSize;
        data += currentBlockSize;
    }

    {
        auto error = WaitFor(writer->Close());
        THROW_ERROR_EXCEPTION_IF_FAILED(error);
    }
}

TEST(TAsyncWriterTest, AsyncCloseFail)
{
    auto pipe = TPipeFactory().Create();

    auto reader = pipe.CreateAsyncReader();
    auto writer = pipe.CreateAsyncWriter();

    auto queue = New<NConcurrency::TActionQueue>();
    auto readFromPipe =
        BIND(&ReadAll, reader, false)
            .AsyncVia(queue->GetInvoker())
            .Run();

    int length = 200*1024;
    auto buffer = TSharedMutableRef::Allocate(length);
    ::memset(buffer.Begin(), 'a', buffer.Size());

    auto writeResult = writer->Write(buffer).Get();

    EXPECT_TRUE(writeResult.IsOK())
        << ToString(writeResult);

    auto error = writer->Close();

    auto readResult = readFromPipe.Get();
    ASSERT_TRUE(readResult.IsOK())
        << ToString(readResult);

    auto closeStatus = error.Get();
}

TEST(TAsyncWriterTest, WriteFailed)
{
    auto pipe = TPipeFactory().Create();
    auto reader = pipe.CreateAsyncReader();
    auto writer = pipe.CreateAsyncWriter();

    int length = 200*1024;
    auto buffer = TSharedMutableRef::Allocate(length);
    ::memset(buffer.Begin(), 'a', buffer.Size());

    auto asyncWriteResult = writer->Write(buffer);
    YT_UNUSED_FUTURE(reader->Abort());

    EXPECT_FALSE(asyncWriteResult.Get().IsOK())
        << ToString(asyncWriteResult.Get());
}

////////////////////////////////////////////////////////////////////////////////

class TPipeReadWriteTest
    : public ::testing::Test
{
protected:
    void SetUp() override
    {
        auto pipe = TPipeFactory().Create();

        Reader = pipe.CreateAsyncReader();
        Writer = pipe.CreateAsyncWriter();
    }

    void TearDown() override
    { }

    IConnectionReaderPtr Reader;
    IConnectionWriterPtr Writer;
};

class TNamedPipeReadWriteTest
    : public ::testing::Test
{
protected:
    void SetUp() override
    { }

    void TearDown() override
    { }

    void SetUpPipes()
    {
        auto pipe = TNamedPipe::Create("./namedpipe");
        Reader = pipe->CreateAsyncReader();
        Writer = pipe->CreateAsyncWriter();
    }

    void SetUpWithCapacity(int capacity)
    {
        auto pipe = TNamedPipe::Create("./namedpipewcap", 0660, capacity);
        Reader = pipe->CreateAsyncReader();
        Writer = pipe->CreateAsyncWriter();
    }

    void SetUpWithDeliveryFence()
    {
        auto pipe = TNamedPipe::Create("./namedpipedelfenced", 0660);
        Reader = pipe->CreateAsyncReader();
        Writer = pipe->CreateAsyncWriter(/*deliveryFenceMode*/ EDeliveryFencedMode::Old);
    }

    IConnectionReaderPtr Reader;
    IConnectionWriterPtr Writer;
};

TEST_F(TPipeReadWriteTest, ReadSomethingSpin)
{
    std::string message("Hello pipe!\n");
    auto buffer = TSharedRef::FromString(message);
    Writer->Write(buffer).Get().ThrowOnError();
    Writer->Close().Get().ThrowOnError();

    auto data = TSharedMutableRef::Allocate(1);
    auto whole = TBlob(GetRefCountedTypeCookie<TDefaultBlobTag>());

    while (true) {
        auto result = Reader->Read(data).Get();
        if (result.ValueOrThrow() == 0) {
            break;
        }
        whole.Append(data.Begin(), result.Value());
    }

    EXPECT_EQ(message, std::string(whole.Begin(), whole.End()));
}

TEST_F(TNamedPipeReadWriteTest, ReadSomethingSpin)
{
    SetUpPipes();

    std::string message("Hello pipe!\n");
    auto buffer = TSharedRef::FromString(message);

    Writer->Write(buffer).Get().ThrowOnError();
    Writer->Close().Get().ThrowOnError();

    auto data = TSharedMutableRef::Allocate(1);
    auto whole = TBlob(GetRefCountedTypeCookie<TDefaultBlobTag>());

    while (true) {
        auto result = Reader->Read(data).Get();
        if (result.ValueOrThrow() == 0) {
            break;
        }
        whole.Append(data.Begin(), result.Value());
    }
    EXPECT_EQ(message, std::string(whole.Begin(), whole.End()));
}


TEST_F(TPipeReadWriteTest, ReadSomethingWait)
{
    std::string message("Hello pipe!\n");
    auto buffer = TSharedRef::FromString(message);
    EXPECT_TRUE(Writer->Write(buffer).Get().IsOK());
    WaitFor(Writer->Close())
        .ThrowOnError();
    auto whole = ReadAll(Reader, false);
    EXPECT_EQ(message, std::string(whole.Begin(), whole.End()));
}

TEST_F(TNamedPipeReadWriteTest, ReadSomethingWait)
{
    SetUpPipes();

    std::string message("Hello pipe!\n");
    auto buffer = TSharedRef::FromString(message);
    EXPECT_TRUE(Writer->Write(buffer).Get().IsOK());
    WaitFor(Writer->Close())
        .ThrowOnError();
    auto whole = ReadAll(Reader, false);
    EXPECT_EQ(message, std::string(whole.Begin(), whole.End()));
}

TEST_F(TPipeReadWriteTest, ReadWrite)
{
    std::string text("Hello cruel world!\n");
    auto buffer = TSharedRef::FromString(text);
    Writer->Write(buffer).Get();
    auto errorsOnClose = Writer->Close();

    auto textFromPipe = ReadAll(Reader, false);

    auto error = errorsOnClose.Get();
    EXPECT_TRUE(error.IsOK()) << error.GetMessage();
    EXPECT_EQ(text, std::string(textFromPipe.Begin(), textFromPipe.End()));
}

TEST_F(TNamedPipeReadWriteTest, ReadWrite)
{
    SetUpPipes();

    std::string text("Hello cruel world!\n");
    auto buffer = TSharedRef::FromString(text);
    Writer->Write(buffer).Get();
    auto errorsOnClose = Writer->Close();

    auto textFromPipe = ReadAll(Reader, false);

    auto error = errorsOnClose.Get();
    EXPECT_TRUE(error.IsOK()) << error.GetMessage();
    EXPECT_EQ(text, std::string(textFromPipe.Begin(), textFromPipe.End()));
}

TEST_F(TNamedPipeReadWriteTest, CapacityJustWorks)
{
    SetUpWithCapacity(SmallPipeCapacity);

    std::string text(5, 'a');
    text.push_back('\n');
    auto writeBuffer = TSharedRef::FromString(text);

    auto writeFuture = Writer->Write(writeBuffer);
    EXPECT_TRUE(writeFuture.Get().IsOK());

    auto readBuffer = TSharedMutableRef::Allocate(5000, {.InitializeStorage = false});
    auto readResult = Reader->Read(readBuffer).Get();

    EXPECT_EQ(text, std::string(readBuffer.Begin(), readResult.Value()));
}

TEST_F(TNamedPipeReadWriteTest, CapacityOverflow)
{
    SetUpWithCapacity(SmallPipeCapacity);
    auto readerQueue = New<NConcurrency::TActionQueue>("Reader");

    std::string text(5000, 'a');
    text.push_back('\n');
    auto writeBuffer = TSharedRef::FromString(text);
    auto writeFuture = Writer->Write(writeBuffer);

    TDelayedExecutor::WaitForDuration(TDuration::Seconds(1));
    EXPECT_FALSE(writeFuture.IsSet());

    auto readFuture = BIND([&] {
        auto readBuffer = TSharedMutableRef::Allocate(6000, {.InitializeStorage = false});
        auto readResult = Reader->Read(readBuffer).Get();

        EXPECT_TRUE(readResult.IsOK());
        EXPECT_EQ(text.substr(0, 4096), TStringBuf(readBuffer.Begin(), readResult.Value()));
    })
        .AsyncVia(readerQueue->GetInvoker())
        .Run();

    EXPECT_TRUE(readFuture.Get().IsOK());
    EXPECT_TRUE(writeFuture.Get().IsOK());
}

TEST_F(TNamedPipeReadWriteTest, CapacityDontDiscardSurplus)
{
    SetUpWithCapacity(SmallPipeCapacity);
    auto readerQueue = New<NConcurrency::TActionQueue>("Reader");
    auto writerQueue = New<NConcurrency::TActionQueue>("Writer");

    std::string text(5000, 'a');
    text.push_back('\n');

    auto writeFuture = BIND(&WriteAll, Writer, text.data(), text.size(), text.size())
        .AsyncVia(writerQueue->GetInvoker())
        .Run();

    TDelayedExecutor::WaitForDuration(TDuration::Seconds(1));
    EXPECT_FALSE(writeFuture.IsSet());

    auto readFuture = BIND(&ReadAll, Reader, false)
        .AsyncVia(readerQueue->GetInvoker())
        .Run();

    auto readResult = readFuture.Get().ValueOrThrow();
    EXPECT_EQ(text, TStringBuf(readResult.Begin(), readResult.End()));

    EXPECT_TRUE(writeFuture.Get().IsOK());
}

#if defined(_linux_)

// Just does not work :(
TEST_F(TNamedPipeReadWriteTest, DISABLED_DeliveryFencedWriteOldJustWorks)
{
    SetUpWithDeliveryFence();

    std::string text("aabbb");
    auto writeBuffer = TSharedRef::FromString(text);
    auto writeFuture = Writer->Write(writeBuffer);

    auto readBuffer = TSharedMutableRef::Allocate(2, {.InitializeStorage = false});
    auto readResult = Reader->Read(readBuffer).Get();
    EXPECT_EQ("aa", TStringBuf(readBuffer.Begin(), readResult.Value()));

    EXPECT_FALSE(writeFuture.IsSet());

    readBuffer = TSharedMutableRef::Allocate(10, {.InitializeStorage = false});
    readResult = Reader->Read(readBuffer).Get();
    EXPECT_EQ("bbb", TStringBuf(readBuffer.Begin(), readResult.Value()));

    // Future is set only after the entire buffer is read.
    EXPECT_TRUE(writeFuture.Get().IsOK());
}

class TNewDeliveryFencedWriteTestFixture
    : public ::testing::TestWithParam<bool>
{
protected:
    void SetUp() override
    {
        static const NLogging::TLogger Logger("TNewDeliveryFencedWriteTestFixture");

        auto version = ParseLinuxKernelVersion();
        YT_VERIFY(!version.empty());

        YT_LOG_DEBUG("Parsed kernel version: (Release: %v, versionTuple: %v)", version[0], version);

        if (version < std::vector{5, 15}) {
            GTEST_SKIP()
                << "Kernel version is too old. Version: "
                << version[0]
                << ", parsed_version: "
                << ToString(version)
                << ", it should be at least 5.15";
        }

        auto readerFirst = GetParam();
        auto pipe = TNamedPipe::Create("./namedpipedelfenced", 0660);
        if (!readerFirst) {
            Reader = pipe->CreateAsyncReader();
        }
        Writer = pipe->CreateAsyncWriter(/*deliveryFenceMode*/ EDeliveryFencedMode::New);
        if (readerFirst) {
            Reader = pipe->CreateAsyncReader();
        }
    }

    void TearDown() override
    { }

    IConnectionReaderPtr Reader;
    IConnectionWriterPtr Writer;
};

YT_TRY_BLOCK_SIGNAL_FOR_PROCESS(SIGRTMIN, [] (bool ok, int threadCount) {
    if (!ok) {
        NLogging::TLogger Logger("SignalBlocking");
        YT_LOG_WARNING("Thread count is not 1, trying to get thread infos (ThreadCount: %v)", threadCount);
        auto threadInfos = NYT::NBacktraceIntrospector::IntrospectThreads();
        auto descripion = NYT::NBacktraceIntrospector::FormatIntrospectionInfos(threadInfos);
        AbortProcessDramatically(
            EProcessExitCode::GenericError,
            Format(
                "Thread count is not 1, threadCount: %v, threadInfos: %v",
                threadCount,
                descripion));
    }
});

#define EXPECT_ERROR_IS_OK(...) do { \
        auto error = __VA_ARGS__; \
        if (!error.IsOK()) { \
            Cerr << "Error: " << ToString(error) << Endl; \
        } \
        EXPECT_TRUE(error.IsOK()); \
    } while (false);

TEST_P(TNewDeliveryFencedWriteTestFixture, JustWorks)
{
    constexpr TDuration ReadDelay = TDuration::MilliSeconds(10);

    std::string text("aabbb");
    auto writeBuffer = TSharedRef::FromString(text);
    auto writeFuture = Writer->Write(writeBuffer);

    auto readBuffer = TSharedMutableRef::Allocate(2, {.InitializeStorage = false});
    auto readResult = Reader->Read(readBuffer).WithTimeout(TDuration::Seconds(5)).Get();
    EXPECT_ERROR_IS_OK(readResult);

    EXPECT_EQ("aa", TStringBuf(readBuffer.Begin(), readResult.ValueOrThrow()));

    Sleep(ReadDelay);

    EXPECT_FALSE(writeFuture.IsSet());

    readBuffer = TSharedMutableRef::Allocate(10, {.InitializeStorage = false});
    readResult = Reader->Read(readBuffer).Get();
    EXPECT_EQ("bbb", TStringBuf(readBuffer.Begin(), readResult.Value()));

    // Future is set only after the entire buffer is read.
    EXPECT_ERROR_IS_OK(writeFuture.WithTimeout(TDuration::Seconds(5)).Get());
}

TEST_P(TNewDeliveryFencedWriteTestFixture, MultipleTimes)
{
    constexpr TDuration ReadDelay = TDuration::MilliSeconds(10);

    for (int i = 0; i < 10; ++i) {
        std::string text("aabbb");

        auto writeBuffer = TSharedRef::FromString(text);
        auto writeFuture = Writer->Write(writeBuffer);

        for (int index = 0; index < ssize(text); ++index) {
            Sleep(ReadDelay);

            EXPECT_FALSE(writeFuture.IsSet());

            auto readBuffer = TSharedMutableRef::Allocate(1, {.InitializeStorage = false});
            auto readResult = Reader->Read(readBuffer).WithTimeout(TDuration::Seconds(5)).Get();
            EXPECT_ERROR_IS_OK(readResult);

            EXPECT_EQ(std::string(1, text[index]), std::string(readBuffer.Begin(), readResult.Value()));
        }

        EXPECT_ERROR_IS_OK(writeFuture.WithTimeout(TDuration::Seconds(5)).Get());
    }
}

TEST_P(TNewDeliveryFencedWriteTestFixture, ReadBeforeWrite)
{
    auto readBuffer = TSharedMutableRef::Allocate(10, {.InitializeStorage = false});
    auto readFuture = Reader->Read(readBuffer).WithTimeout(TDuration::Seconds(5));

    EXPECT_FALSE(readFuture.IsSet());

    std::string text("aabbb");

    auto writeBuffer = TSharedRef::FromString(text);
    auto writeResult = Writer->Write(writeBuffer).WithTimeout(TDuration::Seconds(5)).Get();
    EXPECT_ERROR_IS_OK(writeResult);

    auto readResult = readFuture.Get();
    EXPECT_ERROR_IS_OK(readResult);
    EXPECT_EQ("aabbb", TStringBuf(readBuffer.Begin(), readResult.Value()));
}

TEST_P(TNewDeliveryFencedWriteTestFixture, HugeData)
{
    constexpr size_t LargeDataSize = 256_KB;
    constexpr size_t ChunkSize = 1_KB;
    constexpr TDuration ReadDelay = TDuration::MilliSeconds(10);

    std::string largeData(LargeDataSize, 'X');

    auto writeFuture = Writer->Write(TSharedRef::FromString(largeData));

    std::string receivedData;
    size_t totalRead = 0;

    while (totalRead < LargeDataSize) {
        EXPECT_FALSE(writeFuture.IsSet());

        auto readBuffer = TSharedMutableRef::Allocate(ChunkSize);

        auto readResult = Reader->Read(readBuffer)
            .WithTimeout(TDuration::Seconds(10))
            .Get();

        EXPECT_ERROR_IS_OK(readResult);

        size_t bytesRead = readResult.Value();
        receivedData.append(readBuffer.Begin(), bytesRead);
        totalRead += bytesRead;

        Sleep(ReadDelay);
    }

    EXPECT_EQ(LargeDataSize, totalRead);
    EXPECT_EQ(largeData, receivedData);
    EXPECT_ERROR_IS_OK(writeFuture.WithTimeout(TDuration::Seconds(5)).Get());
}

INSTANTIATE_TEST_SUITE_P(
    TNewDeliveryFencedWriteTests,
    TNewDeliveryFencedWriteTestFixture,
    ::testing::Bool());

#endif

////////////////////////////////////////////////////////////////////////////////

class TPipeBigReadWriteTest
    : public TPipeReadWriteTest
    , public ::testing::WithParamInterface<std::pair<size_t, size_t>>
{ };

TEST_P(TPipeBigReadWriteTest, RealReadWrite)
{
    size_t dataSize, blockSize;
    std::tie(dataSize, blockSize) = GetParam();

    auto queue = New<NConcurrency::TActionQueue>();

    std::vector<char> data(dataSize, 'a');

    YT_UNUSED_FUTURE(BIND([&] {
        auto dice = std::bind(
            std::uniform_int_distribution<int>(0, 127),
            std::default_random_engine());
        for (size_t i = 0; i < data.size(); ++i) {
            data[i] = dice();
        }
    })
    .AsyncVia(queue->GetInvoker()).Run());

    auto writeError =  BIND(&WriteAll, Writer, data.data(), data.size(), blockSize)
        .AsyncVia(queue->GetInvoker())
        .Run();
    auto readFromPipe = BIND(&ReadAll, Reader, true)
        .AsyncVia(queue->GetInvoker())
        .Run();

    auto textFromPipe = readFromPipe.Get().ValueOrThrow();
    EXPECT_EQ(data.size(), textFromPipe.Size());
    auto result = std::mismatch(textFromPipe.Begin(), textFromPipe.End(), data.begin());
    EXPECT_TRUE(std::equal(textFromPipe.Begin(), textFromPipe.End(), data.begin())) <<
        (result.first - textFromPipe.Begin()) << " " << static_cast<int>(*result.first);
}

INSTANTIATE_TEST_SUITE_P(
    ValueParametrized,
    TPipeBigReadWriteTest,
    ::testing::Values(
        std::pair(2000 * 4096, 4096),
        std::pair(100 * 4096, 10000),
        std::pair(100 * 4096, 100),
        std::pair(100, 4096)));

#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPipeIO
