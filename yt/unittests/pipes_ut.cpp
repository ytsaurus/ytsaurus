#include "stdafx.h"
#include "framework.h"

#include <core/concurrency/action_queue.h>
#include <core/concurrency/fiber.h>

#include <ytlib/pipes/io_dispatcher.h>
#include <ytlib/pipes/io_dispatcher_impl.h>
#include <ytlib/pipes/non_block_reader.h>
#include <ytlib/pipes/async_reader.h>
#include <ytlib/pipes/async_writer.h>

#include <random>

namespace NYT {
namespace NPipes {

////////////////////////////////////////////////////////////////////////////////

using namespace NConcurrency;

#ifndef _win_

TEST(TIODispatcher, StartStop)
{
    TIODispatcher dispatcher;
    dispatcher.Shutdown();
}

TEST(TNonBlockReader, BrandNew)
{
    int pipefds[2];
    int err = pipe2(pipefds, O_NONBLOCK);

    ASSERT_TRUE(err == 0);

    NDetail::TNonBlockReader reader(pipefds[0]);
    EXPECT_TRUE(reader.IsBufferEmpty());

    EXPECT_FALSE(reader.IsBufferFull());
    EXPECT_FALSE(reader.IsReady());
    EXPECT_FALSE(reader.InFailedState());
}

TEST(TNonBlockReader, TryReadNeverBlocks)
{
    int pipefds[2];
    int err = pipe2(pipefds, O_NONBLOCK);

    ASSERT_TRUE(err == 0);

    NDetail::TNonBlockReader reader(pipefds[0]);
    reader.TryReadInBuffer();

    EXPECT_FALSE(reader.IsReady());
}

TEST(TNonBlockReader, Failed)
{
    int pipefds[2];
    int err = pipe2(pipefds, O_NONBLOCK);

    ASSERT_TRUE(err == 0);
    NDetail::TNonBlockReader reader(pipefds[0]);

    ASSERT_TRUE(close(pipefds[0]) == 0);
    reader.TryReadInBuffer();

    EXPECT_TRUE(reader.InFailedState());
}

//////////////////////////////////////////////////////////////////////////////////////////////////

class TReadWriteTest : public ::testing::Test
{
protected:
    void SetUp()
    {
        int pipefds[2];
        int err = pipe2(pipefds, O_NONBLOCK);

        ASSERT_TRUE(err == 0);
        Reader = New<TAsyncReader>(pipefds[0]);
        Writer = New<TAsyncWriter>(pipefds[1]);
    }

    TIntrusivePtr<TAsyncReader> Reader;
    TIntrusivePtr<TAsyncWriter> Writer;
};


TEST_F(TReadWriteTest, ReadSomethingSpin)
{
    std::string message("Hello pipe!\n");
    Writer->Write(message.c_str(), message.size());
    Writer->AsyncClose();

    bool isClosed = false;
    TBlob data, whole;

    while (!isClosed)
    {
        std::tie(data, isClosed) = Reader->Read(TBlob());
        whole.Append(data.Begin(), data.Size());
    }

    EXPECT_EQ(std::string(whole.Begin(), whole.End()), message);
}

TBlob ReadAll(TIntrusivePtr<TAsyncReader> reader, bool useWaitFor)
{
    bool isClosed = false;
    TBlob data, whole;

    while (!isClosed)
    {
        std::tie(data, isClosed) = reader->Read(TBlob());
        whole.Append(data.Begin(), data.Size());

        if ((!isClosed) && (data.Size() == 0)) {
            if (useWaitFor) {
                auto error = WaitFor(reader->GetReadyEvent());
            } else {
                auto error = reader->GetReadyEvent().Get();
            }
        }
    }
    return whole;
}

TEST_F(TReadWriteTest, ReadSomethingWait)
{
    std::string message("Hello pipe!\n");
    Writer->Write(message.c_str(), message.size());
    Writer->AsyncClose();

    auto whole = ReadAll(Reader, false);

    EXPECT_EQ(std::string(whole.Begin(), whole.End()), message);
}

TEST_F(TReadWriteTest, ReadWrite)
{
    const std::string text("Hello cruel world!\n");
    Writer->Write(text.c_str(), text.size());
    auto errorsOnClose = Writer->AsyncClose();

    auto textFromPipe = ReadAll(Reader, false);

    auto error = errorsOnClose.Get();
    EXPECT_TRUE(error.IsOK()) << error.GetMessage();
    EXPECT_EQ(std::string(textFromPipe.Begin(), textFromPipe.End()), text);
}

TError WriteAll(TIntrusivePtr<TAsyncWriter> writer, const char* data, size_t size, size_t blockSize)
{
    while (size > 0) {
        bool enough = false;
        while (!enough) {
            const size_t currentBlockSize = std::min(blockSize, size);

            if (currentBlockSize == 0) {
                break;
            }
            enough = writer->Write(data, currentBlockSize);
            size -= currentBlockSize;
            data += currentBlockSize;
        }
        if (enough) {
            auto error = WaitFor(writer->GetReadyEvent());
            RETURN_IF_ERROR(error);
        }
    }
    {
        auto error = WaitFor(writer->AsyncClose());
        return error;
    }
}

////////////////////////////////////////////////////////////////////////////////

class TBigReadWriteTest
    : public TReadWriteTest
    , public ::testing::WithParamInterface<std::pair<size_t, size_t>>
{ };

TEST_P(TBigReadWriteTest, RealReadWrite)
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
    }).AsyncVia(queue->GetInvoker()).Run();

    auto writeError =
        BIND(&WriteAll, Writer, data.data(), data.size(), blockSize)
            .AsyncVia(queue->GetInvoker())
            .Run();
    auto readFromPipe =
        BIND(&ReadAll, Reader, true)
            .AsyncVia(queue->GetInvoker())
            .Run();

    auto textFromPipe = readFromPipe.Get();
    EXPECT_EQ(textFromPipe.Size(), data.size());
    auto result = std::mismatch(textFromPipe.Begin(), textFromPipe.End(), data.begin());
    EXPECT_TRUE(std::equal(textFromPipe.Begin(), textFromPipe.End(), data.begin())) <<
        (result.first - textFromPipe.Begin()) << " " << (int)(*result.first);
}

INSTANTIATE_TEST_CASE_P(
    ValueParametrized,
    TBigReadWriteTest,
    ::testing::Values(
        std::make_pair(2000*4096, 4096),
        std::make_pair(100*4096, 10000),
        std::make_pair(100*4096, 100),
        std::make_pair(100, 4096)));

#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace NPipes
} // namespace NYT
