#pragma once

#include "api_service_proxy.h"

#include <yt/core/concurrency/async_stream.h>

#include <yt/client/api/file_reader.h>

namespace NYT::NApi::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

TFuture<IFileReaderPtr> CreateRpcFileReader(
    TApiServiceProxy::TReqCreateFileReaderPtr request);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy

