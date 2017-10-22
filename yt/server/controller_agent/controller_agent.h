#pragma once

#include "public.h"

#include "master_connector.h"

#include <yt/server/cell_scheduler/public.h>

#include <yt/ytlib/job_tracker_client/job_tracker_service.pb.h>
#include <yt/ytlib/job_tracker_client/job_spec_service.pb.h>

#include <yt/ytlib/node_tracker_client/node_directory.h>

#include <yt/ytlib/transaction_client/public.h>

#include <yt/core/rpc/service_detail.h>

#include <yt/core/ytree/public.h>

namespace NYT {
namespace NControllerAgent {

////////////////////////////////////////////////////////////////////

class TControllerAgent
    : public TRefCounted
{
public:
    TControllerAgent(
        NScheduler::TSchedulerConfigPtr config,
        NCellScheduler::TBootstrap* bootstrap);

    void Connect(IInvokerPtr invoker);
    void Disconnect();
    void ValidateConnected() const;

    const IInvokerPtr& GetInvoker();
    const IInvokerPtr& GetControllerThreadPoolInvoker();

    TMasterConnector* GetMasterConnector();

    void UpdateConfig(const NScheduler::TSchedulerConfigPtr& config);
    
    void RegisterOperation(const TOperationId& operationId, IOperationControllerPtr controller);
    void UnregisterOperation(const TOperationId& operationId);

    std::vector<TErrorOr<TSharedRef>> GetJobSpecs(const std::vector<std::pair<TOperationId, TJobId>>& jobSpecRequests);

    void AttachJobContext(
        const NYTree::TYPath& path,
        const NChunkClient::TChunkId& chunkId,
        const TOperationId& operationId,
        const TJobId& jobId);
    
private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TControllerAgent)

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
