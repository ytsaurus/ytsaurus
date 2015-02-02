#pragma once

#include "public.h"

#include <core/misc/shutdownable.h>

#include <core/actions/public.h>

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

