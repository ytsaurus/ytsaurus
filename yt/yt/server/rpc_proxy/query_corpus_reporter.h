#pragma once

#include "public.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/server/lib/rpc_proxy/query_corpus_reporter.h>

namespace NYT::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

IQueryCorpusReporterPtr MakeQueryCorpusReporter(NApi::NNative::IClientPtr client);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
