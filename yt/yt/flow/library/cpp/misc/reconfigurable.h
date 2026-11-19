#pragma once

#include "public.h"

#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/core/actions/signal.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>

#include <library/cpp/yt/system/atomic_object.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

template <class... TConfigs>
class TReconfigurable
{
public:
    TReconfigurable() = default;
    virtual ~TReconfigurable() = default;

    void Reconfigure(const TIntrusivePtr<TConfigs>&...);

    using TReconfigureHandleSignature = void(const TIntrusivePtr<TConfigs>&...);
    DEFINE_SIGNAL(TReconfigureHandleSignature, Reconfigured);

protected:
    //! Handles a reconfiguration failure; rethrows the original exception by default.
    virtual void OnReconfigureFail(const TError& error);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow

#define RECONFIGURABLE_BASE_H_
#include "reconfigurable-inl.h"
#undef RECONFIGURABLE_BASE_H_
