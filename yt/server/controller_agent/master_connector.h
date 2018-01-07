#pragma once

#include "operation_controller.h"

#include <yt/server/cell_scheduler/public.h>

#include <yt/ytlib/cypress_client/public.h>

#include <yt/ytlib/chunk_client/public.h>

#include <yt/core/misc/ref.h>

#include <yt/core/actions/signal.h>

namespace NYT {
namespace NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

//! Mediates communication between controller agent and master.
/*!
 *  \note Thread affinity: control unless noted otherwise
 *  XXX(babenko): check affinity
 */
class TMasterConnector
{
public:
    TMasterConnector(
        TControllerAgentConfigPtr config,
        NCellScheduler::TBootstrap* bootstrap);
    ~TMasterConnector();

    // TODO(babenko): get rid of these methods
    void OnMasterConnecting(const TIncarnationId& incarnationId);
    void OnMasterConnected();
    void OnMasterDisconnected();

    /*!
     *  \note Thread affinity: any
     */
    bool IsConnected() const;

    /*!
     *  \note Thread affinity: any
     */
    TInstant GetConnectionTime() const;

    const TIncarnationId& GetIncarnationId() const;

    void StartOperationNodeUpdates(
        const TOperationId& operationId,
        NScheduler::EOperationCypressStorageMode storageMode);

    void CreateJobNode(
        const TOperationId& operationId,
        const TCreateJobNodeRequest& request);

    TFuture<void> FlushOperationNode(const TOperationId& operationId);

    TFuture<void> AttachToLivePreview(
        const TOperationId& operationId,
        const NObjectClient::TTransactionId& transactionId,
        const std::vector<NCypressClient::TNodeId>& tableIds,
        const std::vector<NChunkClient::TChunkTreeId>& childIds);

    TFuture<TOperationSnapshot> DownloadSnapshot(const TOperationId& operationId);
    TFuture<void> RemoveSnapshot(const TOperationId& operationId);

    void AddChunkTreesToUnstageList(
        std::vector<NChunkClient::TChunkTreeId> chunkTreeIds,
        bool recursive);

    // XXX(babenko): this method does not belong here
    void AttachJobContext(
        const NYPath::TYPath& path,
        const NChunkClient::TChunkId& chunkId,
        const TOperationId& operationId,
        const TJobId& jobId);

    void UpdateConfig(const TControllerAgentConfigPtr& config);

    //! Raised during connection process.
    //! Subscribers may throw and yield.
    DECLARE_SIGNAL(void(), MasterConnecting);

    //! Raised when connection is complete.
    //! Subscribers may throw but cannot yield.
    DECLARE_SIGNAL(void(), MasterConnected);

    //! Raised when disconnect happens.
    //! Subscribers may yield but cannot throw.
    DECLARE_SIGNAL(void(), MasterDisconnected);

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT

