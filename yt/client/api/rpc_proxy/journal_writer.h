#pragma once

#include "api_service_proxy.h"

#include <yt/client/api/journal_writer.h>

namespace NYT::NApi::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

IJournalWriterPtr CreateRpcJournalWriter(
    TApiServiceProxy::TReqCreateJournalWriterPtr request);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy

