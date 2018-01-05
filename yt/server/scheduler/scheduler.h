#pragma once

#include "public.h"

#include <yt/server/cell_scheduler/public.h>

#include <yt/ytlib/hydra/public.h>

#include <yt/ytlib/node_tracker_client/node_directory.h>

#include <yt/ytlib/transaction_client/public.h>

#include <yt/core/rpc/service_detail.h>

#include <yt/core/ytree/public.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

/*!
 *  \note Thread affinity: Control unless noted otherwise
 */
class TScheduler
    : public TRefCounted
{
public:
    TScheduler(
        TSchedulerConfigPtr config,
        NCellScheduler::TBootstrap* bootstrap);
    ~TScheduler();

    void Initialize();

    /*!
     *  \note Thread affinity: any
     */
    ISchedulerStrategyPtr GetStrategy();

    /*!
     *  \note Thread affinity: any
     */
    NYTree::IYPathServicePtr GetOrchidService();

    /*!
     *  \note Thread affinity: any
     */
    bool IsConnected();
    /*!
     *  \note Thread affinity: any
     */
    void ValidateConnected();
    /*!
     *  \note Thread affinity: any
     */
    void ValidateAcceptsHeartbeats();

    void Disconnect();

    TOperationPtr FindOperation(const TOperationId& id) const;
    TOperationPtr GetOperationOrThrow(const TOperationId& id) const;

    TFuture<TOperationPtr> StartOperation(
        EOperationType type,
        const NTransactionClient::TTransactionId& transactionId,
        const NRpc::TMutationId& mutationId,
        NYTree::IMapNodePtr spec,
        const TString& user);

    TFuture<void> AbortOperation(TOperationPtr operation, const TError& error, const TString& user);
    TFuture<void> SuspendOperation(TOperationPtr operation, const TString& user, bool abortRunningJobs);
    TFuture<void> ResumeOperation(TOperationPtr operation, const TString& user);
    TFuture<void> CompleteOperation(
        TOperationPtr operation,
        const TError& error,
        const TString& user);

    TFuture<NYson::TYsonString> Strace(const TJobId& jobId, const TString& user);
    TFuture<void> DumpInputContext(const TJobId& jobId, const NYPath::TYPath& path, const TString& user);
    TFuture<NYT::NNodeTrackerClient::TNodeDescriptor> GetJobNode(const TJobId& jobId, const TString& user);
    TFuture<void> SignalJob(const TJobId& jobId, const TString& signalName, const TString& user);
    TFuture<void> AbandonJob(const TJobId& jobId, const TString& user);
    TFuture<NYson::TYsonString> PollJobShell(const TJobId& jobId, const NYson::TYsonString& parameters, const TString& user);
    TFuture<void> AbortJob(const TJobId& jobId, TNullable<TDuration> interruptTimeout, const TString& user);

    using TCtxNodeHeartbeat = NRpc::TTypedServiceContext<
        NJobTrackerClient::NProto::TReqHeartbeat,
        NJobTrackerClient::NProto::TRspHeartbeat>;
    using TCtxNodeHeartbeatPtr = TIntrusivePtr<TCtxNodeHeartbeat>;
    /*!
     *  \note Thread affinity: any
     */
    void ProcessNodeHeartbeat(const TCtxNodeHeartbeatPtr& context);

    using TCtxAgentHeartbeat = NRpc::TTypedServiceContext<
        NScheduler::NProto::TReqHeartbeat,
        NScheduler::NProto::TRspHeartbeat>;
    using TCtxAgentHeartbeatPtr = TIntrusivePtr<TCtxAgentHeartbeat>;
    void ProcessAgentHeartbeat(const TCtxAgentHeartbeatPtr& context);

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TScheduler)

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

