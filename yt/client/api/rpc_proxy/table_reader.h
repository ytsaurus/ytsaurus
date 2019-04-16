#pragma once

#include "api_service_proxy.h"

#include <yt/client/api/table_reader.h>

namespace NYT::NApi::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

TFuture<ITableReaderPtr> CreateRpcTableReader(
    TApiServiceProxy::TReqCreateTableReaderPtr request);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy
