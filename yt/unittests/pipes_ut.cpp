#include "stdafx.h"

#include <core/fileio/file_io_dispatcher.h>
#include <core/fileio/file_io_dispatcher_impl.h>
#include <core/fileio/async_reader.h>
#include <core/fileio/async_writer.h>

#include <core/concurrency/action_queue.h>
#include <core/concurrency/fiber.h>

#include <contrib/testing/framework.h>

namespace NYT {
namespace NFileIO {

////////////////////////////////////////////////////////////////////////////////

using NConcurrency::WaitFor;

struct TNopFDWatcher : public IFDWatcher
{
    virtual void Start(ev::dynamic_loop& eventLoop)
    {
    }
};

struct TFailFDWatcher : public IFDWatcher
{
    virtual void Start(ev::dynamic_loop& eventLoop)
    {
        throw std::exception();
    }
};

TEST(TFileIODispatcher, StartStop)
{
    TFileIODispatcher dispatcher;
    dispatcher.Shutdown();
}

TEST(TFileIODispatcher, RegisterSucess)
{
    TFileIODispatcher dispatcher;

    auto watcher = New<TNopFDWatcher>();

    auto error = dispatcher.AsyncRegister(watcher);
    EXPECT_TRUE(error.Get().IsOK());
}

TEST(TFileIODispatcher, RegisterFail)
{
    TFileIODispatcher dispatcher;

    auto watcher = New<TFailFDWatcher>();

    auto error = dispatcher.AsyncRegister(watcher);
    EXPECT_FALSE(error.Get().IsOK());
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

        {
            auto error = Dispatcher.AsyncRegister(Reader).Get();
            ASSERT_TRUE(error.IsOK());
        }
        {
            auto error = Dispatcher.AsyncRegister(Writer).Get();
            ASSERT_TRUE(error.IsOK());
        }
    }

    TFileIODispatcher Dispatcher;
    TIntrusivePtr<TAsyncReader> Reader;
    TIntrusivePtr<TAsyncWriter> Writer;
};


TEST_F(TReadWriteTest, ReadSomethingSpin)
{
    std::string message("Hello pipe!\n");
    Writer->Write(message.c_str(), message.size());
    Writer->Close();

    bool isClosed = false;
    TBlob data, whole;

    while (!isClosed)
    {
        std::tie(data, isClosed) = Reader->Read();
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
        std::tie(data, isClosed) = reader->Read();
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
    Writer->Close();

    TBlob whole = ReadAll(Reader, false);

    EXPECT_EQ(std::string(whole.Begin(), whole.End()), message);
}

TEST_F(TReadWriteTest, ReadWrite)
{
    const std::string text("Hello cruel world!\n");
    Writer->Write(text.c_str(), text.size());
    TAsyncError errorsOnClose = Writer->Close();

    TBlob textFromPipe = ReadAll(Reader, false);

    EXPECT_TRUE(errorsOnClose.Get().IsOK());
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
            std::cerr << currentBlockSize << " bytes has been written\n";
        }
        if (enough) {
            std::cerr << "Enough!\n";
            TError error = WaitFor(writer->GetReadyEvent());
            if (!error.IsOK()) {
                std::cerr << error.GetMessage() << std::endl;
                return error;
            }
        }
    }
    {
        TError error = WaitFor(writer->Close());
        return error;
    }
}

TEST_F(TReadWriteTest, RealReadWrite)
{
    auto queue = New<NConcurrency::TActionQueue>();

    std::vector<char> data(1000 * 4096, 'a');
    auto dice = std::bind(std::uniform_int_distribution<char>(0, 128),
                          std::default_random_engine());

    for (auto& x: data) {
        x = dice();
    }

    auto writeError = BIND(&WriteAll, Writer, data.data(), data.size(), 4096).AsyncVia(queue->GetInvoker()).Run();
    auto readFromPipe = BIND(&ReadAll, Reader, true).AsyncVia(queue->GetInvoker()).Run();

    auto textFromPipe = readFromPipe.Get();
    EXPECT_TRUE(equal(textFromPipe.Begin(), textFromPipe.End(), data.begin()));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileIO
} // namespace NYT
