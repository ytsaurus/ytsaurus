#pragma once

#include <yt/core/misc/intrusive_ptr.h>

namespace NYT {
namespace NNet {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IConnection)
DECLARE_REFCOUNTED_STRUCT(IListener)
DECLARE_REFCOUNTED_STRUCT(IDialer)
DECLARE_REFCOUNTED_STRUCT(IAsyncDialer)
DECLARE_REFCOUNTED_STRUCT(IAsyncDialerSession)

DECLARE_REFCOUNTED_CLASS(TDialerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NNet
} // namespace NYT
