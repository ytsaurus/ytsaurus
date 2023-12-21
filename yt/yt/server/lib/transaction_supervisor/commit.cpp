#include "commit.h"

#include <yt/yt/server/lib/hydra/serialize.h>

#include <yt/yt/core/misc/serialize.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NTransactionSupervisor {

using namespace NRpc;
using namespace NHydra;
using namespace NTracing;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TCommit::TCommit(TTransactionId transactionId)
    : TransactionId_(transactionId)
    , Persistent_(false)
{ }

TCommit::TCommit(
    TTransactionId transactionId,
    TMutationId mutationId,
    std::vector<TCellId> participantCellIds,
    std::vector<TCellId> prepareOnlyParticipantCellIds,
    std::vector<TCellId> cellIdsToSyncWithBeforePrepare,
    bool distributed,
    bool generatePrepareTimestamp,
    bool inheritCommitTimestamp,
    NApi::ETransactionCoordinatorPrepareMode coordinatorPrepareMode,
    NApi::ETransactionCoordinatorCommitMode coordinatorCommitMode,
    TTimestamp maxAllowedCommitTimestamp,
    NRpc::TAuthenticationIdentity identity,
    std::vector<TTransactionId> prerequisiteTransactionIds)
    : TransactionId_(transactionId)
    , MutationId_(mutationId)
    , ParticipantCellIds_(std::move(participantCellIds))
    , PrepareOnlyParticipantCellIds_(std::move(prepareOnlyParticipantCellIds))
    , CellIdsToSyncWithBeforePrepare_(std::move(cellIdsToSyncWithBeforePrepare))
    , Distributed_(distributed)
    , GeneratePrepareTimestamp_(generatePrepareTimestamp)
    , InheritCommitTimestamp_(inheritCommitTimestamp)
    , CoordinatorPrepareMode_(coordinatorPrepareMode)
    , CoordinatorCommitMode_(coordinatorCommitMode)
    , MaxAllowedCommitTimestamp_(maxAllowedCommitTimestamp)
    , AuthenticationIdentity_(std::move(identity))
    , PrerequisiteTransactionIds_(std::move(prerequisiteTransactionIds))
{ }

TFuture<TSharedRefArray> TCommit::GetAsyncResponseMessage()
{
    return ResponseMessagePromise_;
}

void TCommit::SetResponseMessage(TSharedRefArray message)
{
    ResponseMessagePromise_.TrySet(std::move(message));
}

bool TCommit::IsPrepareOnlyParticipant(TCellId cellId) const
{
    return
        std::find(PrepareOnlyParticipantCellIds_.begin(), PrepareOnlyParticipantCellIds_.end(), cellId) !=
        PrepareOnlyParticipantCellIds_.end();
}

void TCommit::Save(TSaveContext& context) const
{
    using NYT::Save;

    YT_VERIFY(Persistent_);
    Save(context, TransactionId_);
    Save(context, MutationId_);
    Save(context, ParticipantCellIds_);
    Save(context, PrepareOnlyParticipantCellIds_);
    Save(context, CellIdsToSyncWithBeforePrepare_);
    Save(context, Distributed_);
    Save(context, GeneratePrepareTimestamp_);
    Save(context, InheritCommitTimestamp_);
    Save(context, PrepareTimestamp_);
    Save(context, PrepareTimestampClusterTag_);
    Save(context, CommitTimestamps_);
    Save(context, PersistentState_);
    Save(context, CoordinatorPrepareMode_);
    Save(context, CoordinatorCommitMode_);
    Save(context, MaxAllowedCommitTimestamp_);
    Save(context, AuthenticationIdentity_.User);
    Save(context, AuthenticationIdentity_.UserTag);
}

void TCommit::Load(TLoadContext& context)
{
    using NYT::Load;

    Persistent_ = true;
    Load(context, TransactionId_);
    Load(context, MutationId_);
    Load(context, ParticipantCellIds_);
    Load(context, PrepareOnlyParticipantCellIds_);
    // COMPAT(babenko)
    if (context.GetVersion() >= 10) {
        Load(context, CellIdsToSyncWithBeforePrepare_);
    }
    Load(context, Distributed_);
    Load(context, GeneratePrepareTimestamp_);
    Load(context, InheritCommitTimestamp_);
    // COMPAT(gritukan)
    if (context.GetVersion() >= 12) {
        Load(context, PrepareTimestamp_);
        Load(context, PrepareTimestampClusterTag_);
    } else {
        PrepareTimestamp_ = NTransactionClient::NullTimestamp;
        PrepareTimestampClusterTag_ = NObjectClient::InvalidCellTag;
    }
    Load(context, CommitTimestamps_);
    Load(context, PersistentState_);
    // COMPAT(gritukan)
    if (context.GetVersion() >= 12) {
        Load(context, CoordinatorPrepareMode_);
    } else {
        CoordinatorPrepareMode_ = NApi::ETransactionCoordinatorPrepareMode::Early;
    }
    Load(context, CoordinatorCommitMode_);
    // COMPAT(ifsmirnov)
    if (context.GetVersion() >= 11) {
        Load(context, MaxAllowedCommitTimestamp_);
    }
    Load(context, AuthenticationIdentity_.User);
    Load(context, AuthenticationIdentity_.UserTag);
}

void TCommit::BuildOrchidYson(IYsonConsumer* consumer) const
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("mutation_id").Value(MutationId_)
            .Item("trace_id").Value(TraceId_)
            .Item("participant_cell_ids").Value(ParticipantCellIds_)
            .DoIf(!PrepareOnlyParticipantCellIds_.empty(), [&] (auto fluent) {
                fluent
                    .Item("prepare_only_participant_cell_ids")
                    .Value(PrepareOnlyParticipantCellIds_);
            })
            .DoIf(!CellIdsToSyncWithBeforePrepare_.empty(), [&] (auto fluent) {
                fluent
                    .Item("cell_ids_to_sync_with_before_prepare")
                    .Value(CellIdsToSyncWithBeforePrepare_);
            })
            .Item("distributed").Value(Distributed_)
            .Item("generate_prepare_timestamp").Value(GeneratePrepareTimestamp_)
            .Item("inherit_commit_timestamp").Value(InheritCommitTimestamp_)
            .Item("coordinator_prepare_mode").Value(CoordinatorPrepareMode_)
            .Item("coordinator_commit_mode").Value(CoordinatorCommitMode_)
            .Item("max_allowed_commit_timestamp").Value(MaxAllowedCommitTimestamp_)
            .Item("persistent").Value(Persistent_)
            .Item("prepare_timestamp").Value(PrepareTimestamp_)
            .Item("prepare_timestamp_cluster_tag").Value(PrepareTimestampClusterTag_)
            .Item("commit_timestamps").DoMapFor(
                CommitTimestamps_.Timestamps,
                [] (auto fluent, auto item) {
                    fluent.Item(ToString(item.first)).Value(item.second);
                })
            .Item("transient_state").Value(TransientState_)
            .Item("persistent_state").Value(PersistentState_)
            .Item("responded_cell_ids").Value(RespondedCellIds_)
            .DoIf(!PrerequisiteTransactionIds_.empty(), [&] (auto fluent) {
                fluent
                    .Item("prerequisite_transaction_ids")
                    .Value(PrerequisiteTransactionIds_);
            })
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionSupervisor
