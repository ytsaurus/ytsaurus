#pragma once

#include "public.h"

#include <core/rpc/service_detail.h>

#include <core/ytree/public.h>

#include <ytlib/transaction_client/public.h>

#include <ytlib/hydra/public.h>

#include <ytlib/node_tracker_client/node_directory.h>

#include <ytlib/job_tracker_client/job_tracker_service.pb.h>

#include <server/cell_scheduler/public.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////

class TScheduler
    : public TRefCounted
{
public:
    TScheduler(
        TSchedulerConfigPtr config,
        NCellScheduler::TBootstrap* bootstrap);

    ~TScheduler();

    void Initialize();

    ISchedulerStrategy* GetStrategy();

    NYTree::IYPathServicePtr GetOrchidService();

    std::vector<TOperationPtr> GetOperations();
    std::vector<TExecNodePtr> GetExecNodes();

    IInvokerPtr GetSnapshotIOInvoker();

    bool IsConnected();
    void ValidateConnected();

    TOperationPtr FindOperation(const TOperationId& id);
    TOperationPtr GetOperationOrThrow(const TOperationId& id);

    TExecNodePtr FindNode(const Stroka& address);
    TExecNodePtr GetNode(const Stroka& address);
    TExecNodePtr GetOrRegisterNode(const NNodeTrackerClient::TNodeDescriptor& descriptor);

    TFuture<TOperationPtr> StartOperation(
        EOperationType type,
        const NTransactionClient::TTransactionId& transactionId,
        const NHydra::TMutationId& mutationId,
        NYTree::IMapNodePtr spec,
        const Stroka& user);

    TFuture<void> AbortOperation(
        TOperationPtr operation,
        const TError& error);

    TFuture<void> SuspendOperation(TOperationPtr operation);
    TFuture<void> ResumeOperation(TOperationPtr operation);

    typedef
        NRpc::TTypedServiceContext<
            NJobTrackerClient::NProto::TReqHeartbeat,
            NJobTrackerClient::NProto::TRspHeartbeat>
        TCtxHeartbeat;
    typedef TIntrusivePtr<TCtxHeartbeat> TCtxHeartbeatPtr;
    void ProcessHeartbeat(
        TExecNodePtr node,
        TCtxHeartbeatPtr context);

private:
    class TImpl;
    TIntrusivePtr<TImpl> Impl_;

    class TSchedulingContext;

};

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

