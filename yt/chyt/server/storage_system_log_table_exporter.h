#pragma once

#include "private.h"

#include <yt/yt/ytlib/api/native/public.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

void RegisterStorageSystemLogTableExporter(
    const THost* host,
    const NApi::NNative::IClientPtr& client,
    const IInvokerPtr& invoker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
