#include "stdafx.h"

#include <core/fileio/file_io_dispatcher.h>
#include <core/fileio/file_io_dispatcher_impl.h>

#include <contrib/testing/framework.h>

namespace NYT {
namespace NFileIO {

////////////////////////////////////////////////////////////////////////////////

struct NopFDWatcher : public IFDWatcher
{
    virtual void Start(ev::dynamic_loop& eventLoop)
    {
    }
};

struct FailFDWatcher : public IFDWatcher
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

    IFDWatcherPtr watcher(new NopFDWatcher());

    auto error = dispatcher.AsyncRegister(watcher);
    EXPECT_TRUE(error.Get().IsOK());
}

TEST(TFileIODispatcher, RegisterFail)
{
    TFileIODispatcher dispatcher;

    IFDWatcherPtr watcher(new FailFDWatcher());

    auto error = dispatcher.AsyncRegister(watcher);
    EXPECT_FALSE(error.Get().IsOK());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NYT
