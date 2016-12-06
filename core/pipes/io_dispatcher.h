#pragma once

#include "public.h"

#include <yt/core/misc/shutdownable.h>

#include <yt/core/actions/public.h>

namespace ev {
    struct loop_ref;
}

namespace NYT {
namespace NPipes {

////////////////////////////////////////////////////////////////////////////////

class TIODispatcher
    : public IShutdownable
{
public:
    ~TIODispatcher();

    static TIODispatcher* Get();

    static void StaticShutdown();

    virtual void Shutdown() override;

    IInvokerPtr GetInvoker();

    const ev::loop_ref& GetEventLoop();

private:
    TIODispatcher();

    Y_DECLARE_SINGLETON_FRIEND();

    class TImpl;
    TIntrusivePtr<TImpl> Impl_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NPipes
} // namespace NYT
