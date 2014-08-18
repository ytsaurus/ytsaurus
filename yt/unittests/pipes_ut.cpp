#include "stdafx.h"
#include "framework.h"

#include <core/concurrency/action_queue.h>
#include <core/concurrency/scheduler.h>

#include <ytlib/pipes/io_dispatcher.h>
#include <ytlib/pipes/non_block_reader.h>
#include <ytlib/pipes/async_reader.h>
#include <ytlib/pipes/async_writer.h>
#include <server/job_proxy/pipes.h>

#include <random>

namespace NYT {
namespace NPipes {

////////////////////////////////////////////////////////////////////////////////

using namespace NConcurrency;

#ifndef _win_

TEST(TPipeIODispatcher, StartStop)
{
    TIODispatcher dispatcher;
    dispatcher.Shutdown();
}

void SafeMakeNonblockingPipes(int fds[2])
{
    NJobProxy::SafePipe(fds);
    NJobProxy::SafeMakeNonblocking(fds[0]);
    NJobProxy::SafeMakeNonblocking(fds[1]);
}

TEST(TPipeNonblockingReader, BrandNew)
{
    int pipefds[2];
    SafeMakeNonblockingPipes(pipefds);

    NDetail::TNonblockingReader reader(pipefds[0]);
    EXPECT_FALSE(reader.IsClosed());
}

TEST(TPipeNonblockingReader, Failed)
{
    int pipefds[2];
    SafeMakeNonblockingPipes(pipefds);

    NDetail::TNonblockingReader reader(pipefds[0]);

    ASSERT_TRUE(close(pipefds[0]) == 0);

    char buffer[10];
    EXPECT_FALSE(reader.Read(buffer, 10).IsOK());
}

TEST(TPipeIOHolder, CanInstantiate)
{
    int pipefds[2];
    SafeMakeNonblockingPipes(pipefds);

    auto readerHolder = New<TAsyncReader>(pipefds[0]);
    auto writerHolder = New<TAsyncWriter>(pipefds[1]);
}

//////////////////////////////////////////////////////////////////////////////////////////////////

TBlob ReadAll(TAsyncReaderPtr reader, bool useWaitFor)
{
    TBlob buffer(1024 * 1024);
    TBlob whole;

    while (true)  {
        TErrorOr<size_t> result;
        auto future = reader->Read(buffer.Begin(), buffer.Size());
        if (useWaitFor) {
            result = WaitFor(future);
        } else {
            result = future.Get();
        }

        if (result.Value() == 0) {
            break;
        }

        whole.Append(buffer.Begin(), result.Value());
    }
    return whole;
}

TEST(TAsyncWriterTest, AsyncCloseFail)
{
    int pipefds[2];
    SafeMakeNonblockingPipes(pipefds);

    auto reader = New<TAsyncReader>(pipefds[0]);
    auto writer = New<TAsyncWriter>(pipefds[1]);

    auto queue = New<NConcurrency::TActionQueue>();
    auto readFromPipe =
        BIND(&ReadAll, reader, false)
            .AsyncVia(queue->GetInvoker())
            .Run();


    std::vector<char> buffer(200*1024, 'a');
    ASSERT_TRUE(writer->Write(&buffer[0], buffer.size()).Get().IsOK());

    auto error = writer->Close();
    auto closeStatus = error.Get();

    ASSERT_EQ(-1, close(pipefds[1]));
}

//////////////////////////////////////////////////////////////////////////////////////////////////

class TPipeReadWriteTest
    : public ::testing::Test
{
protected:
    virtual void SetUp() override
    {
        int pipefds[2];
        SafeMakeNonblockingPipes(pipefds);

        Reader = New<TAsyncReader>(pipefds[0]);
        Writer = New<TAsyncWriter>(pipefds[1]);
    }

    virtual void TearDown() override
    { }

    TAsyncReaderPtr Reader;
    TAsyncWriterPtr Writer;
};


TEST_F(TPipeReadWriteTest, ReadSomethingSpin)
{
    std::string message("Hello pipe!\n");
    Writer->Write(message.c_str(), message.size()).Get();
    Writer->Close();

    TBlob data(1), whole;

    while (true)
    {
        auto result = Reader->Read(data.Begin(), data.Size()).Get();
        if (result.Value() == 0) {
            break;
        }
        whole.Append(data.Begin(), result.Value());
    }

    EXPECT_EQ(message, std::string(whole.Begin(), whole.End()));
}

TEST_F(TPipeReadWriteTest, ReadSomethingWait)
{
    std::string message("Hello pipe!\n");
    Writer->Write(message.c_str(), message.size()).Get();
    Writer->Close();

    auto whole = ReadAll(Reader, false);

    EXPECT_EQ(message, std::string(whole.Begin(), whole.End()));
}

TEST_F(TPipeReadWriteTest, ReadWrite)
{
    const std::string text("Hello cruel world!\n");
    Writer->Write(text.c_str(), text.size()).Get();
    auto errorsOnClose = Writer->Close();

    auto textFromPipe = ReadAll(Reader, false);

    auto error = errorsOnClose.Get();
    EXPECT_TRUE(error.IsOK()) << error.GetMessage();
    EXPECT_EQ(text, std::string(textFromPipe.Begin(), textFromPipe.End()));
}

void WriteAll(TAsyncWriterPtr writer, const char* data, size_t size, size_t blockSize)
{
    while (size > 0) {
        const size_t currentBlockSize = std::min(blockSize, size);
        auto error = WaitFor(writer->Write(data, currentBlockSize));
        THROW_ERROR_EXCEPTION_IF_FAILED(error);
        size -= currentBlockSize;
        data += currentBlockSize;
    }

    {
        auto error = WaitFor(writer->Close());
        THROW_ERROR_EXCEPTION_IF_FAILED(error);
    }
}

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

    BIND([&] () {
        auto dice = std::bind(
            std::uniform_int_distribution<char>(0, 128),
            std::default_random_engine());
        for (size_t i = 0; i < data.size(); ++i) {
            data[i] = dice();
        }
    })
        .AsyncVia(queue->GetInvoker()).Run();

    auto writeError =  BIND(&WriteAll, Writer, data.data(), data.size(), blockSize)
        .Guarded()
        .AsyncVia(queue->GetInvoker())
        .Run();
    auto readFromPipe = BIND(&ReadAll, Reader, true)
        .AsyncVia(queue->GetInvoker())
        .Run();

    auto textFromPipe = readFromPipe.Get();
    EXPECT_EQ(data.size(), textFromPipe.Size());
    auto result = std::mismatch(textFromPipe.Begin(), textFromPipe.End(), data.begin());
    EXPECT_TRUE(std::equal(textFromPipe.Begin(), textFromPipe.End(), data.begin())) <<
        (result.first - textFromPipe.Begin()) << " " << (int)(*result.first);
}

INSTANTIATE_TEST_CASE_P(
    ValueParametrized,
    TPipeBigReadWriteTest,
    ::testing::Values(
        std::make_pair(2000 * 4096, 4096),
        std::make_pair(100 * 4096, 10000),
        std::make_pair(100 * 4096, 100),
        std::make_pair(100, 4096)));

#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace NPipes
} // namespace NYT
