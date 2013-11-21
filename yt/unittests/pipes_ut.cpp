#include "stdafx.h"

#include <core/fileio/file_io_dispatcher.h>
#include <core/fileio/file_io_dispatcher_impl.h>
#include <core/fileio/async_reader.h>
#include <core/fileio/async_writer.h>

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
    write(pipefds[1], message.c_str(), message.size());
    close(pipefds[1]);

    bool isClosed = false;
    TBlob data, whole;

    while (!isClosed)
    {
        std::tie(data, isClosed) = reader->Read();
        whole.Append(data.Begin(), data.Size());

    }

    EXPECT_EQ(std::string(whole.Begin(), whole.End()), message);

    close(pipefds[0]);
}

TBlob readAll(TAsyncReader& reader)
{
    bool isClosed = false;
    TBlob data, whole;

    while (!isClosed)
    {
        std::tie(data, isClosed) = reader.Read();
        whole.Append(data.Begin(), data.Size());

        if ((!isClosed) && (data.Size() == 0)) {
            TError error = reader.GetReadState().Get();
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
    write(pipefds[1], message.c_str(), message.size());
    close(pipefds[1]);

    TBlob whole = readAll(*reader);

    EXPECT_EQ(std::string(whole.Begin(), whole.End()), message);

    close(pipefds[0]);
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

    TBlob textFromPipe = readAll(*reader);

    EXPECT_TRUE(errorsOnClose.Get().IsOK());
    EXPECT_EQ(std::string(textFromPipe.Begin(), textFromPipe.End()), text);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileIO
} // namespace NYT
