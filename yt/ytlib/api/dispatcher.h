#pragma once

#include "public.h"

#include <yt/core/actions/public.h>

namespace NYT {
namespace NApi {

////////////////////////////////////////////////////////////////////////////////

class TDispatcher
    : public TIntrinsicRefCounted
{
public:
    TDispatcher(int lightPoolSize, int heavyPoolSize);

    /*!
     * This invoker is used by client for light commands.
     */
    IInvokerPtr GetLightInvoker();

    /*!
     * This invoker is used by client for heavy commands.
     */
    IInvokerPtr GetHeavyInvoker();

private:
    class TImpl;
    std::unique_ptr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TDispatcher)

////////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT

