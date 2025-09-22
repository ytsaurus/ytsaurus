#pragma once

#include "yt/yql/providers/yt/fmr/worker/interface/yql_yt_fmr_worker.h"
#include <yql/essentials/utils/runnable.h>
#include <util/generic/string.h>

namespace NYql::NFmr {

using IFmrWorkerServer = IRunnable;

struct TFmrWorkerServerSettings {
    ui16 Port;
    TString Host = "localhost";
};

IFmrWorkerServer::TPtr MakeFmrWorkerServer(const TFmrWorkerServerSettings& settings, IFmrWorker::TPtr worker);

} // namespace NYql::NFmr
