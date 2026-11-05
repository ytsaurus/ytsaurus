#pragma once

#include <yql/tools/yqlworker/interface/worker_api.h>

#include <util/datetime/base.h>

namespace NYql::NWorkerApi {

struct TMsgBusWorkerApiConfig {
    int Port = 32300;
    bool BindLoopbackOnly = true;
    ui32 MsgBusThreads = 3;
    ui32 MsgBusMaxInFlight = 7000;
    TDuration MsgBusSendTimeout = TDuration::Seconds(60);
    TDuration MsgBusTotalTimeout = TDuration::Seconds(120);

    TDuration MaxHeartbeatGap = TDuration::Seconds(30);
    TDuration MaxHeartbeatCheckGap = TDuration::Minutes(2);
    TDuration LoseUnhealthyWorkerAfter = TDuration::Minutes(30);

    TDuration WorkerConnectTimeout = TDuration::Seconds(10);
    TDuration WorkerTotalTimeout = TDuration::Seconds(60);

    TDuration WorkerCheckPeriod = TDuration::Seconds(5);
    TDuration OperationCheckPeriod = TDuration::Seconds(10);
};

std::shared_ptr<IWorkerApi> MakeMsgBusWorkerApi(TMsgBusWorkerApiConfig config);

} // namespace NYql::NWorkerApi
