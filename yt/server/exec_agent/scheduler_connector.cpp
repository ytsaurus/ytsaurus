#include "scheduler_connector.h"
#include "private.h"
#include "config.h"
#include "job.h"

#include <yt/server/cell_node/bootstrap.h>

#include <yt/server/data_node/master_connector.h>

#include <yt/server/data_node/master_connector.h>

#include <yt/server/job_agent/job_controller.h>

#include <yt/ytlib/api/client.h>

#include <yt/core/concurrency/scheduler.h>

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

    auto client = Bootstrap_->GetMasterClient();

    TJobTrackerServiceProxy proxy(client->GetSchedulerChannel());
    auto req = proxy.Heartbeat();

    auto jobController = Bootstrap_->GetJobController();
    auto masterConnection = client->GetConnection();
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
