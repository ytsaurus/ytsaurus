#include "partition_scale_request.h"
#include "read_balancer_log.h"

#include <contrib/ydb/core/protos/schemeshard/operations.pb.h>

namespace NKikimr {
namespace NPQ {

TPartitionScaleRequest::TPartitionScaleRequest(
    const TString& topicName,
    const TString& topicPath,
    const TString& databasePath,
    ui64 pathId,
    ui64 pathVersion,
    const std::vector<NKikimrSchemeOp::TPersQueueGroupDescription_TPartitionSplit>& splits,
    const std::vector<NKikimrSchemeOp::TPersQueueGroupDescription_TPartitionMerge>& merges,
    const NActors::TActorId& parentActorId
)
    : Topic(topicName)
    , TopicPath(topicPath)
    , DatabasePath(databasePath)
    , PathId(pathId)
    , PathVersion(pathVersion)
    , Splits(splits)
    , Merges(merges)
    , ParentActorId(parentActorId) {

    }

void TPartitionScaleRequest::Bootstrap(const NActors::TActorContext &ctx) {
    SendProposeRequest(ctx);
    Become(&TPartitionScaleRequest::StateWork);
}

void TPartitionScaleRequest::SendProposeRequest(const NActors::TActorContext &ctx) {
    auto proposal = std::make_unique<TEvTxUserProxy::TEvProposeTransaction>();
    proposal->Record.SetDatabaseName(CanonizePath(DatabasePath));
    FillProposeRequest(*proposal, ctx);
    ctx.Send(MakeTxProxyID(), proposal.release());
}

void TPartitionScaleRequest::FillProposeRequest(TEvTxUserProxy::TEvProposeTransaction& proposal, const NActors::TActorContext&) {
    auto workingDir = TopicPath.substr(0, TopicPath.size() - Topic.size());

    auto& modifyScheme = *proposal.Record.MutableTransaction()->MutableModifyScheme();
    modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpAlterPersQueueGroup);
    modifyScheme.SetWorkingDir(workingDir);
    modifyScheme.SetInternal(true);

    auto applyIf = modifyScheme.AddApplyIf();
    applyIf->SetPathId(PathId);
    applyIf->SetPathVersion(PathVersion == 0 ? 1 : PathVersion);
    applyIf->SetCheckEntityVersion(true);

    NKikimrSchemeOp::TPersQueueGroupDescription groupDescription;
    groupDescription.SetName(Topic);
    TStringBuilder logMessage;
    logMessage << "TPartitionScaleRequest::FillProposeRequest trying to scale partitions of '" << workingDir << "/" << Topic << "'. Spilts: ";
    for(const auto& split: Splits) {
        auto* newSplit = groupDescription.AddSplit();
        logMessage << "partition: " << split.GetPartition() << " boundary: '" << split.GetSplitBoundary() << "' ";
        *newSplit = split;
    }
    PQ_LOG_D( logMessage);

    for(const auto& merge: Merges) {
        auto* newMerge = groupDescription.AddMerge();
        *newMerge = merge;
    }

    modifyScheme.MutableAlterPersQueueGroup()->CopyFrom(groupDescription);
}

void TPartitionScaleRequest::PassAway() {
    if (SchemePipeActorId) {
        NTabletPipe::CloseClient(this->SelfId(), SchemePipeActorId);
    }

    TBase::PassAway();
}

void TPartitionScaleRequest::Handle(TEvTabletPipe::TEvClientConnected::TPtr &ev, const TActorContext &ctx) {
    if (ev->Get()->Status != NKikimrProto::OK) {
        auto scaleRequestResult = std::make_unique<TEvPartitionScaleRequestDone>(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyShardNotAvailable);
        Send(ParentActorId, scaleRequestResult.release());
        Die(ctx);
    }
}

void TPartitionScaleRequest::Handle(TEvTabletPipe::TEvClientDestroyed::TPtr&, const TActorContext &ctx) {
    auto scaleRequestResult = std::make_unique<TEvPartitionScaleRequestDone>(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyShardNotAvailable);
    Send(ParentActorId, scaleRequestResult.release());
    Die(ctx);
}

void TPartitionScaleRequest::Handle(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult::TPtr& /*ev*/, const TActorContext& ctx) {
    auto scaleRequestResult = std::make_unique<TEvPartitionScaleRequestDone>(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete);
    Send(ParentActorId, scaleRequestResult.release());
    Die(ctx);
}

void TPartitionScaleRequest::Handle(TEvTxUserProxy::TEvProposeTransactionStatus::TPtr& ev, const NActors::TActorContext& ctx) {
    auto msg = ev->Get();

    auto status = static_cast<TEvTxUserProxy::TEvProposeTransactionStatus::EStatus>(msg->Record.GetStatus());
    if (status != TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecInProgress) {
        auto scaleRequestResult = std::make_unique<TEvPartitionScaleRequestDone>(status);
        TStringBuilder issues;
        for (auto& issue : ev->Get()->Record.GetIssues()) {
            issues << issue.ShortDebugString() + ", ";
        }
        PQ_LOG_ERROR("TPartitionScaleRequest SchemaShard error when trying to execute a split request: " << issues);
        Send(ParentActorId, scaleRequestResult.release());
        Die(ctx);
    } else {
        NTabletPipe::TClientConfig clientConfig;
        clientConfig.RetryPolicy = {.RetryLimitCount = 3};
        if (!SchemePipeActorId) {
            SchemePipeActorId = ctx.Register(NTabletPipe::CreateClient(ctx.SelfID, msg->Record.GetSchemeShardTabletId(), clientConfig));
        }

        auto request = std::make_unique<NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletion>(msg->Record.GetTxId());
        NTabletPipe::SendData(this->SelfId(), SchemePipeActorId, request.release());
    }
}

} // namespace NPQ
} // namespace NKikimr
