#pragma once

#include "api_service_proxy.h"

#include <yt/client/api/table_writer.h>

namespace NYT::NApi::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

TFuture<ITableWriterPtr> CreateRpcTableWriter(
    TApiServiceProxy::TReqCreateTableWriterPtr request);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy

