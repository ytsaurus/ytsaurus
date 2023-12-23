#pragma once

#include "public.h"

#include <yt/yt/server/lib/hydra/entity_map.h>

#include <yt/yt/ytlib/transaction_client/public.h>

#include <yt/yt/client/hive/timestamp_map.h>

#include <yt/yt/client/api/public.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/misc/property.h>

#include <yt/yt/core/rpc/authentication_identity.h>

#include <yt/yt/core/tracing/public.h>

#include <library/cpp/yt/memory/ref_tracked.h>

namespace NYT::NTransactionSupervisor {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ECommitState,
    ((Start)                     (0))
    ((Prepare)                   (1))
    ((GeneratingCommitTimestamps)(2)) // transient only
    ((Commit)                    (3))
    ((Aborting)                  (4)) // transient only
    ((Abort)                     (5))
    ((Finishing)                 (6)) // transient only
);

class TCommit
    : public NHydra::TEntityBase
    , public TRefTracked<TCommit>
{
public:
    DEFINE_BYVAL_RO_PROPERTY(TTransactionId, TransactionId);
    DEFINE_BYVAL_RO_PROPERTY(NRpc::TMutationId, MutationId);
    DEFINE_BYVAL_RO_PROPERTY(NTracing::TTraceId, TraceId);
    DEFINE_BYREF_RO_PROPERTY(std::vector<TCellId>, ParticipantCellIds);
    DEFINE_BYREF_RO_PROPERTY(std::vector<TCellId>, PrepareOnlyParticipantCellIds);
    DEFINE_BYREF_RO_PROPERTY(std::vector<TCellId>, CellIdsToSyncWithBeforePrepare);
    DEFINE_BYVAL_RO_PROPERTY(bool, Distributed);
    DEFINE_BYVAL_RO_PROPERTY(bool, GeneratePrepareTimestamp);
    DEFINE_BYVAL_RO_PROPERTY(bool, InheritCommitTimestamp);
    DEFINE_BYVAL_RO_PROPERTY(NApi::ETransactionCoordinatorPrepareMode, CoordinatorPrepareMode);
    DEFINE_BYVAL_RO_PROPERTY(NApi::ETransactionCoordinatorCommitMode, CoordinatorCommitMode);
    DEFINE_BYVAL_RW_PROPERTY(TTimestamp, MaxAllowedCommitTimestamp);
    DEFINE_BYVAL_RW_PROPERTY(bool, Persistent);
    DEFINE_BYREF_RW_PROPERTY(TTimestamp, PrepareTimestamp);
    DEFINE_BYREF_RW_PROPERTY(NApi::TClusterTag, PrepareTimestampClusterTag);
    DEFINE_BYREF_RW_PROPERTY(NHiveClient::TTimestampMap, CommitTimestamps);
    DEFINE_BYVAL_RW_PROPERTY(ECommitState, TransientState, ECommitState::Start);
    DEFINE_BYVAL_RW_PROPERTY(ECommitState, PersistentState, ECommitState::Start);
    DEFINE_BYREF_RW_PROPERTY(THashSet<TCellId>, RespondedCellIds);
    DEFINE_BYREF_RW_PROPERTY(NRpc::TAuthenticationIdentity, AuthenticationIdentity);
    // For simple transaction commits only. Transient.
    DEFINE_BYREF_RO_PROPERTY(std::vector<TTransactionId>, PrerequisiteTransactionIds);

public:
    explicit TCommit(TTransactionId transactionId);
    TCommit(
        TTransactionId transactionId,
        NRpc::TMutationId mutationId,
        std::vector<TCellId> participantCellIds,
        std::vector<TCellId> prepareOnlyPrticipantCellIds,
        std::vector<TCellId> cellIdsToSyncWithBeforePrepare,
        bool distributed,
        bool generatePrepareTimestamp,
        bool inheritCommitTimestamp,
        NApi::ETransactionCoordinatorPrepareMode coordinatorPrepareMode,
        NApi::ETransactionCoordinatorCommitMode coordinatorCommitMode,
        TTimestamp maxAllowedCommitTimestamp,
        NRpc::TAuthenticationIdentity identity,
        std::vector<TTransactionId> prerequisiteTransactionIds = {});

    TFuture<TSharedRefArray> GetAsyncResponseMessage();
    void SetResponseMessage(TSharedRefArray message);

    bool IsPrepareOnlyParticipant(TCellId cellId) const;

    void Save(NHydra::TSaveContext& context) const;
    void Load(NHydra::TLoadContext& context);

    void BuildOrchidYson(NYson::IYsonConsumer* consumer) const;

private:
    TPromise<TSharedRefArray> ResponseMessagePromise_ = NewPromise<TSharedRefArray>();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionSupervisor
