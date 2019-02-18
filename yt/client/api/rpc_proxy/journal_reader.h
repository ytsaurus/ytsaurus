#pragma once

#include "api_service_proxy.h"

#include <yt/core/concurrency/async_stream.h>

#include <yt/client/api/journal_reader.h>

namespace NYT::NApi::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

IJournalReaderPtr CreateRpcJournalReader(
    TApiServiceProxy::TReqCreateJournalReaderPtr request);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy
