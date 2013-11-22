#include "stdafx.h"

#include <core/fileio/file_io_dispatcher.h>
#include <core/fileio/file_io_dispatcher_impl.h>
#include <core/fileio/async_reader.h>
#include <core/fileio/async_writer.h>

#include <core/concurrency/action_queue.h>

#include <contrib/testing/framework.h>

namespace NYT {
namespace NFileIO {

////////////////////////////////////////////////////////////////////////////////

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

TEST(TFileIODispatcher, ReadSomethingSpin)
{
    TFileIODispatcher dispatcher;

    int pipefds[2];
    int err = pipe2(pipefds, O_NONBLOCK);

    ASSERT_TRUE(err == 0);

    auto reader = New<TAsyncReader>(pipefds[0]);
    auto error = dispatcher.AsyncRegister(reader);

    ASSERT_TRUE(error.Get().IsOK());

    std::string message("Hello pipe!\n");
    ASSERT_TRUE(write(pipefds[1], message.c_str(), message.size()) == message.size());
    ASSERT_TRUE(close(pipefds[1]) == 0);

    bool isClosed = false;
    TBlob data, whole;

    while (!isClosed)
    {
        std::tie(data, isClosed) = reader->Read();
        whole.Append(data.Begin(), data.Size());
    }

    EXPECT_EQ(std::string(whole.Begin(), whole.End()), message);

    ASSERT_TRUE(close(pipefds[0]) == -1);
}

TBlob ReadAll(TIntrusivePtr<TAsyncReader> reader)
{
    bool isClosed = false;
    TBlob data, whole;

    while (!isClosed)
    {
        std::tie(data, isClosed) = reader->Read();
        whole.Append(data.Begin(), data.Size());

        if ((!isClosed) && (data.Size() == 0)) {
            auto error = reader->GetReadyEvent().Get();
        }
    }
    return whole;
}

TEST(TFileIODispatcher, ReadSomethingWait)
{
    TFileIODispatcher dispatcher;

    int pipefds[2];
    int err = pipe2(pipefds, O_NONBLOCK);

    ASSERT_TRUE(err == 0);

    auto reader = New<TAsyncReader>(pipefds[0]);
    auto error = dispatcher.AsyncRegister(reader);

    ASSERT_TRUE(error.Get().IsOK());

    std::string message("Hello pipe!\n");
    ASSERT_EQ(write(pipefds[1], message.c_str(), message.size()), message.size());
    ASSERT_EQ(close(pipefds[1]), 0);

    TBlob whole = ReadAll(reader);

    EXPECT_EQ(std::string(whole.Begin(), whole.End()), message);
}

TEST(TFileIODispatcher, ReadWrite)
{
    TFileIODispatcher dispatcher;

    int pipefds[2];
    int err = pipe2(pipefds, O_NONBLOCK);

    ASSERT_TRUE(err == 0);

    auto reader = New<TAsyncReader>(pipefds[0]);
    auto writer = New<TAsyncWriter>(pipefds[1]);

    {
        auto error = dispatcher.AsyncRegister(reader);
        ASSERT_TRUE(error.Get().IsOK());
    }
    {
        auto error = dispatcher.AsyncRegister(writer);
        ASSERT_TRUE(error.Get().IsOK());
    }

    const std::string text("Hello cruel world!\n");
    writer->Write(text.c_str(), text.size());
    TAsyncError errorsOnClose = writer->Close();

    TBlob textFromPipe = ReadAll(reader);

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
            TError error = writer->GetReadyEvent().Get();
            if (!error.IsOK()) {
                std::cerr << error.GetMessage() << std::endl;
                return error;
            }
        }
    }
    {
        TError error = writer->Close().Get();
        return error;
    }
}

TEST(TFileIODispatcher, RealReadWrite)
{
    auto queue = New<NConcurrency::TActionQueue>();

    TFileIODispatcher dispatcher;

    int pipefds[2];
    int err = pipe2(pipefds, O_NONBLOCK);

    ASSERT_TRUE(err == 0);

    auto reader = New<TAsyncReader>(pipefds[0]);
    auto writer = New<TAsyncWriter>(pipefds[1]);

    {
        auto error = dispatcher.AsyncRegister(reader);
        ASSERT_TRUE(error.Get().IsOK());
    }
    {
        auto error = dispatcher.AsyncRegister(writer);
        ASSERT_TRUE(error.Get().IsOK());
    }

    std::vector<char> data(10 * 4096, 'a');

    WriteAll(writer, &data[0], data.size(), 4096);
    TFuture<TBlob> readFromPipe = BIND(&ReadAll, reader).AsyncVia(queue->GetInvoker()).Run();

    auto textFromPipe = readFromPipe.Get();
    EXPECT_EQ(textFromPipe.Size(), 10 * 4096);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileIO
} // namespace NYT
