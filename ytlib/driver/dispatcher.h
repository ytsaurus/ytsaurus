#pragma once

#include "public.h"

#include <yt/core/actions/public.h>

#include <yt/core/misc/shutdownable.h>

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

class TDispatcher
    : public IShutdownable
{
public:
    TDispatcher();

    ~TDispatcher();

    static TDispatcher* Get();

    static void StaticShutdown();

    void Configure(int lightPoolSize, int heavyPoolSize);

    virtual void Shutdown() override;

    /*!
     * This invoker is used by TDriver for light commands.
     */
    IInvokerPtr GetLightInvoker();

    /*!
     * This invoker is used by TDriver for light commands.
     */
    IInvokerPtr GetHeavyInvoker();

private:
    class TImpl;
    std::unique_ptr<TImpl> Impl_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT

