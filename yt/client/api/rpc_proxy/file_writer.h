#pragma once

#include "api_service_proxy.h"

#include <yt/client/api/file_writer.h>

namespace NYT::NApi::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

IFileWriterPtr CreateRpcFileWriter(
    TApiServiceProxy::TReqCreateFileWriterPtr request);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy
