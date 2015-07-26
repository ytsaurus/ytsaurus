#include "stdafx.h"
#include "scheduler_connector.h"
#include "private.h"
#include "job.h"
#include "config.h"

#include <core/concurrency/scheduler.h>

#include <ytlib/api/client.h>

#include <server/job_agent/job_controller.h>

#include <server/data_node/master_connector.h>

#include <server/cell_node/bootstrap.h>

namespace NYT {
namespace NExecAgent {

using namespace NNodeTrackerClient;
using namespace NJobTrackerClient;
using namespace NObjectClient;
using namespace NCellNode;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ExecAgentLogger;

////////////////////////////////////////////////////////////////////////////////

TSchedulerConnector::TSchedulerConnector(
    TSchedulerConnectorConfigPtr config,
    TBootstrap* bootstrap)
    : Config_(config)
    , Bootstrap_(bootstrap)
    , ControlInvoker_(bootstrap->GetControlInvoker())
{
    YCHECK(config);
    YCHECK(bootstrap);
}

void TSchedulerConnector::Start()
{
    HeartbeatExecutor_ = New<TPeriodicExecutor>(
        ControlInvoker_,
        BIND(&TSchedulerConnector::SendHeartbeat, MakeWeak(this)),
        Config_->HeartbeatPeriod,
        EPeriodicExecutorMode::Automatic,
        Config_->HeartbeatSplay);

    // Schedule an out-of-order heartbeat whenever a job finishes
    // or its resource usage is updated.
    auto jobController = Bootstrap_->GetJobController();
    jobController->SubscribeResourcesUpdated(BIND(
        &TPeriodicExecutor::ScheduleOutOfBand,
        HeartbeatExecutor_));

    HeartbeatExecutor_->Start();
}

void TSchedulerConnector::SendHeartbeat()
{
    auto masterConnector = Bootstrap_->GetMasterConnector();
    if (!masterConnector->IsConnected()) {
        return;
    }

    auto masterClient = Bootstrap_->GetMasterClient();

    TJobTrackerServiceProxy proxy(masterClient->GetSchedulerChannel());
    auto req = proxy.Heartbeat();

    auto jobController = Bootstrap_->GetJobController();
    auto masterConnection = masterClient->GetConnection();
    jobController->PrepareHeartbeatRequest(
        masterConnection->GetPrimaryMasterCellTag(),
        EObjectType::SchedulerJob,
        req.Get());

    LOG_INFO("Scheduler heartbeat sent (ResourceUsage: {%v})",
        FormatResourceUsage(req->resource_usage(), req->resource_limits()));

    auto rspOrError = WaitFor(req->Invoke());

    if (!rspOrError.IsOK()) {
        LOG_ERROR(rspOrError, "Error reporting heartbeat to scheduler");
        return;
    }

    LOG_INFO("Successfully reported heartbeat to scheduler");

    const auto& rsp = rspOrError.Value();
    jobController->ProcessHeartbeatResponse(rsp.Get());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT
