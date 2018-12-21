#pragma once

#include "public.h"

#include <yt/client/node_tracker_client/node_directory.h>

#include <yt/client/scheduler/operation_id_or_alias.h>

#include <yt/ytlib/hydra/public.h>

#include <yt/ytlib/transaction_client/public.h>

#include <yt/core/rpc/service_detail.h>

#include <yt/core/ytree/public.h>

namespace NYT::NScheduler {

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
        TBootstrap* bootstrap);
    ~TScheduler();

    void Initialize();

    /*!
     *  \note Thread affinity: any
     */
    ISchedulerStrategyPtr GetStrategy() const;

    /*!
     *  \note Thread affinity: any
     */
    const TOperationsCleanerPtr& GetOperationsCleaner() const;

    /*!
     *  \note Thread affinity: any
     */
    NYTree::IYPathServicePtr CreateOrchidService() const;

    /*!
     *  \note Thread affinity: any
     */
    TRefCountedExecNodeDescriptorMapPtr GetCachedExecNodeDescriptors() const;

    const TSchedulerConfigPtr& GetConfig() const;

    /*!
     *  \note Thread affinity: any
     */
    int GetNodeShardId(NNodeTrackerClient::TNodeId nodeId) const;

    const IInvokerPtr& GetCancelableNodeShardInvoker(int shardId) const;

    /*!
     *  \note Thread affinity: any
     */
    const std::vector<TNodeShardPtr>& GetNodeShards() const;

    /*!
     *  \note Thread affinity: any
     */
    bool IsConnected() const;

    /*!
     *  \note Thread affinity: any
     */
    void ValidateConnected();

    /*!
     *  \note Thread affinity: any
     */
    TMasterConnector* GetMasterConnector() const;

    void Disconnect(const TError& error);

    TOperationPtr FindOperation(TOperationId id) const;
    TOperationPtr GetOperationOrThrow(const TOperationIdOrAlias& idOrAlias) const;

    TFuture<TOperationPtr> StartOperation(
        EOperationType type,
        NTransactionClient::TTransactionId transactionId,
        NRpc::TMutationId mutationId,
        NYTree::IMapNodePtr spec,
        const TString& user);

    TFuture<void> AbortOperation(TOperationPtr operation, const TError& error, const TString& user);
    TFuture<void> SuspendOperation(TOperationPtr operation, const TString& user, bool abortRunningJobs);
    TFuture<void> ResumeOperation(TOperationPtr operation, const TString& user);
    TFuture<void> CompleteOperation(
        TOperationPtr operation,
        const TError& error,
        const TString& user);

    TFuture<void> UpdateOperationParameters(
        TOperationPtr operation,
        const TString& user,
        NYTree::INodePtr parameters);

    void OnOperationCompleted(const TOperationPtr& operation);
    void OnOperationAborted(const TOperationPtr& operation, const TError& error);
    void OnOperationFailed(const TOperationPtr& operation, const TError& error);
    void OnOperationSuspended(const TOperationPtr& operation, const TError& error);
    void OnOperationAgentUnregistered(const TOperationPtr& operation);
    void OnOperationBannedInTentativeTree(const TOperationPtr& operation, const TString& treeId, const std::vector<TJobId>& jobIds);

    TFuture<NYson::TYsonString> Strace(TJobId jobId, const TString& user);
    TFuture<void> DumpInputContext(TJobId jobId, const NYPath::TYPath& path, const TString& user);
    TFuture<NYT::NNodeTrackerClient::TNodeDescriptor> GetJobNode(TJobId jobId, const TString& user);
    TFuture<void> SignalJob(TJobId jobId, const TString& signalName, const TString& user);
    TFuture<void> AbandonJob(TJobId jobId, const TString& user);
    TFuture<void> AbortJob(TJobId jobId, std::optional<TDuration> interruptTimeout, const TString& user);

    using TCtxNodeHeartbeat = NRpc::TTypedServiceContext<
        NJobTrackerClient::NProto::TReqHeartbeat,
        NJobTrackerClient::NProto::TRspHeartbeat>;
    using TCtxNodeHeartbeatPtr = TIntrusivePtr<TCtxNodeHeartbeat>;
    /*!
     *  \note Thread affinity: any
     */
    void ProcessNodeHeartbeat(const TCtxNodeHeartbeatPtr& context);

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TScheduler)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler

