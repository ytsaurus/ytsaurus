#pragma once

#include "public.h"

#include "bootstrap.h"

#include <yt/yt/library/profiling/sensor.h>

#include <yt/yt/server/rpc_proxy/public.h>
#include <yt/yt/server/lib/rpc_proxy/query_corpus_reporter.h>

#include <yt/yt/core/misc/mpsc_stack.h>

#include <library/cpp/yt/threading/public.h>
#include <library/cpp/yt/threading/spin_lock.h>

namespace NYT::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

IQueryCorpusReporterPtr MakeQueryCorpusReporter(NApi::NNative::IClientPtr client);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
