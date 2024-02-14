#pragma once

#include "public.h"

#include "operation.h"

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/client/scheduler/operation_id_or_alias.h>

#include <yt/yt/ytlib/hydra/public.h>

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/ytlib/security_client/public.h>

#include <yt/yt/ytlib/scheduler/helpers.h>

#include <yt/yt/ytlib/transaction_client/public.h>

#include <yt/yt/core/rpc/service_detail.h>

#include <yt/yt/core/ytree/public.h>

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

    /*!
     *  \note Thread affinity: any
     */
    TSharedRef GetCachedProtoExecNodeDescriptors() const;

    const TSchedulerConfigPtr& GetConfig() const;

    /*!
     *  \note Thread affinity: any
     */
    IInvokerPtr GetBackgroundInvoker() const;

    /*!
     *  \note Thread affinity: any
     */
    IInvokerPtr GetOperationsCleanerInvoker() const;

    /*!
     *  \note Thread affinity: any
     */
    const TNodeManagerPtr& GetNodeManager() const;

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

    //! Build preprocessed spec for a newly registered operation.
    TFuture<TPreprocessedSpec> AssignExperimentsAndParseSpec(
        EOperationType type,
        const TString& user,
        NYson::TYsonString specString) const;

    TFuture<TOperationPtr> StartOperation(
        EOperationType type,
        NTransactionClient::TTransactionId transactionId,
        NRpc::TMutationId mutationId,
        const TString& user,
        TPreprocessedSpec preprocessedSpec);

    TFuture<void> AbortOperation(TOperationPtr operation, const TError& error, const TString& user);
    TFuture<void> SuspendOperation(TOperationPtr operation, const TString& user, bool abortRunningAllocations);
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
    void OnOperationBannedInTentativeTree(
        const TOperationPtr& operation,
        const TString& treeId,
        const std::vector<TAllocationId>& allocationIds);

    using TCtxNodeHeartbeat = NRpc::TTypedServiceContext<
        NProto::NNode::TReqHeartbeat,
        NProto::NNode::TRspHeartbeat>;
    using TCtxNodeHeartbeatPtr = TIntrusivePtr<TCtxNodeHeartbeat>;

    /*!
     *  \note Thread affinity: any
     */
    void ProcessNodeHeartbeat(const TCtxNodeHeartbeatPtr& context);

    NSecurityClient::TSerializableAccessControlList GetOperationBaseAcl() const;

    int GetOperationsArchiveVersion() const;

    // TODO(eshcherbin): Do we need these methods in the header?
    TString FormatResources(const TJobResourcesWithQuota& resources) const;

    void SetMediumDirectory(const NChunkClient::TMediumDirectoryPtr& mediumDirectory);

    TFuture<void> SetOperationAlert(
        TOperationId operationId,
        EOperationAlertType alertType,
        const TError& alert,
        std::optional<TDuration> timeout = {});

    TFuture<void> ValidateJobShellAccess(
        const TString& user,
        const TString& jobShellName,
        const std::vector<TString>& jobShellOwners);

    TFuture<TOperationId> FindOperationIdByAllocationId(TAllocationId allocationId) const;

    const NRpc::IResponseKeeperPtr& GetOperationServiceResponseKeeper() const;

    TAllocationBriefInfo GetAllocationBriefInfo(
        TAllocationId allocationId,
        TAllocationInfoToRequest requestedAllocationInfo) const;

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TScheduler)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler

