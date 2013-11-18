#pragma once

#include <yt/core/misc/error.h>

#include <memory>

namespace NYT {
namespace NFileIO {

class IFDWatcher;
typedef TIntrusivePtr<IFDWatcher> IFDWatcherPtr;

class TFileIODispatcher
{
public:
    TFileIODispatcher();

    static TFileIODispatcher* Get();

    TAsyncError AsyncRegister(IFDWatcherPtr watcher);
    void Shutdown();
private:
    class TImpl;
    std::unique_ptr<TImpl> Impl;
};

}
}
