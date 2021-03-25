#pragma once

#include <yt/yt/client/api/public.h>

namespace NYT::NApi::NNative {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IConnection)
DECLARE_REFCOUNTED_STRUCT(IClient)
DECLARE_REFCOUNTED_STRUCT(ITransaction)
DECLARE_REFCOUNTED_CLASS(TClientCache)
DECLARE_REFCOUNTED_CLASS(TStickyGroupSizeCache)

DECLARE_REFCOUNTED_CLASS(TMasterConnectionConfig)
DECLARE_REFCOUNTED_CLASS(TClockServersConfig)
DECLARE_REFCOUNTED_CLASS(TConnectionConfig)

struct TConnectionOptions;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative

