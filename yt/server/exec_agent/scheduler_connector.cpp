#include "stdafx.h"
#include "scheduler_connector.h"
#include "private.h"
#include "job.h"
#include "config.h"

#include <ytlib/node_tracker_client/helpers.h>

#include <server/job_agent/job_controller.h>

#include <server/data_node/master_connector.h>

#include <server/cell_node/bootstrap.h>

namespace NYT {
namespace NExecAgent {

using namespace NNodeTrackerClient;
using namespace NJobTrackerClient;
using namespace NCellNode;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = ExecAgentLogger;

////////////////////////////////////////////////////////////////////////////////

TSchedulerConnector::TSchedulerConnector(
    TSchedulerConnectorConfigPtr config,
    TBootstrap* bootstrap)
    : Config(config)
    , Bootstrap(bootstrap)
    , ControlInvoker(bootstrap->GetControlInvoker())
{
    YCHECK(config);
    YCHECK(bootstrap);
}

void TSchedulerConnector::Start()
{
    HeartbeatInvoker = New<TPeriodicInvoker>(
        ControlInvoker,
        BIND(&TThis::SendHeartbeat, MakeWeak(this)),
        Config->HeartbeatPeriod,
        EPeriodicInvokerMode::Manual,
        Config->HeartbeatSplay);

    // Schedule an out-of-order heartbeat whenever a job finishes
    // or its resource usage is updated.
    auto jobController = Bootstrap->GetJobController();
    jobController->SubscribeResourcesUpdated(BIND(
        &TPeriodicInvoker::ScheduleOutOfBand,
        HeartbeatInvoker));

    HeartbeatInvoker->Start();
}

void TSchedulerConnector::SendHeartbeat()
{
    auto masterConnector = Bootstrap->GetMasterConnector();
    if (!masterConnector->IsConnected()) {
        HeartbeatInvoker->ScheduleNext();
        return;
    }

    TJobTrackerServiceProxy proxy(Bootstrap->GetSchedulerChannel());
    auto req = proxy.Heartbeat();

    auto jobController = Bootstrap->GetJobController();
    jobController->PrepareHeartbeat(~req);

    req->Invoke().Subscribe(
        BIND(&TSchedulerConnector::OnHeartbeatResponse, MakeStrong(this))
            .Via(ControlInvoker));

    LOG_INFO("Scheduler heartbeat sent (ResourceUsage: {%s})",
        ~FormatResourceUsage(req->resource_usage(), req->resource_limits()));
}

void TSchedulerConnector::OnHeartbeatResponse(TJobTrackerServiceProxy::TRspHeartbeatPtr rsp)
{
    HeartbeatInvoker->ScheduleNext();

    if (!rsp->IsOK()) {
        LOG_ERROR(*rsp, "Error reporting heartbeat to scheduler");
        return;
    }

    LOG_INFO("Successfully reported heartbeat to scheduler");

    auto jobController = Bootstrap->GetJobController();
    jobController->ProcessHeartbeat(~rsp);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT
