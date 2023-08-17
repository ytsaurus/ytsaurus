#pragma once

#include "operation_controller.h"

#include <yt/yt/ytlib/cypress_client/public.h>

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/core/actions/signal.h>

#include <library/cpp/yt/memory/ref.h>

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

//! Mediates communication between controller agent and master.
/*!
 *  \note Thread affinity: control unless noted otherwise
 */
class TMasterConnector
{
public:
    TMasterConnector(
        TControllerAgentConfigPtr config,
        NYTree::INodePtr configNode,
        TBootstrap* bootstrap);
    ~TMasterConnector();

    void Initialize();

    void RegisterOperation(TOperationId operationId);
    void UnregisterOperation(TOperationId operationId);

    TFuture<void> FlushOperationNode(TOperationId operationId);

    TFuture<void> UpdateInitializedOperationNode(TOperationId operationId, bool isCleanOperationStart);

    TFuture<void> UpdateControllerFeatures(TOperationId operationId, const NYson::TYsonString& featureYson);

    TFuture<void> AttachToLivePreview(
        TOperationId operationId,
        NObjectClient::TTransactionId transactionId,
        NCypressClient::TNodeId tableId,
        const std::vector<NChunkClient::TChunkTreeId>& childIds);

    TFuture<TOperationSnapshot> DownloadSnapshot(TOperationId operationId);
    TFuture<void> RemoveSnapshot(TOperationId operationId);

    void AddChunkTreesToUnstageList(
        std::vector<NChunkClient::TChunkTreeId> chunkTreeIds,
        bool recursive);

    TFuture<void> UpdateAccountResourceUsageLease(
        NSecurityClient::TAccountResourceUsageLeaseId leaseId,
        const NScheduler::TDiskQuota& diskQuota);

    TFuture<void> UpdateConfig();
    ui64 GetConfigRevision() const;

    //! Returns |true| if tags were already downloaded from the master
    //! and |false| otherwise.
    bool TagsLoaded() const;

    const std::vector<TString>& GetTags() const;

    void SetControllerAgentAlert(EControllerAgentAlertType alertType, const TError& alert);

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent

