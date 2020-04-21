#include "transaction_manager.h"
#include "private.h"
#include "config.h"
#include "helpers.h"
#include "action.h"

#include <yt/ytlib/api/native/connection.h>
#include <yt/ytlib/api/native/config.h>

#include <yt/ytlib/hive/cell_directory.h>
#include <yt/ytlib/hive/cell_tracker.h>
#include <yt/ytlib/hive/transaction_supervisor_service_proxy.h>
#include <yt/ytlib/hive/transaction_participant_service_proxy.h>

#include <yt/client/hive/timestamp_map.h>

#include <yt/client/object_client/helpers.h>

#include <yt/ytlib/transaction_client/transaction_service_proxy.h>
#include <yt/client/transaction_client/timestamp_provider.h>

#include <yt/ytlib/tablet_client/tablet_service_proxy.h>

#include <yt/core/concurrency/delayed_executor.h>
#include <yt/core/concurrency/thread_affinity.h>

#include <yt/core/rpc/helpers.h>
#include <yt/core/rpc/retrying_channel.h>

#include <yt/core/ytree/public.h>

#include <atomic>

namespace NYT::NTransactionClient {

using namespace NApi;
using namespace NConcurrency;
using namespace NHiveClient;
using namespace NHydra;
using namespace NObjectClient;
using namespace NRpc;
using namespace NTabletClient;
using namespace NYTree;

using NYT::ToProto;
using NYT::FromProto;

using NNative::IConnection;
using NNative::IConnectionPtr;

////////////////////////////////////////////////////////////////////////////////

// Used as a part of transaction id. Random initialization prevents collisions
// between different machines.
static std::atomic<ui32> TabletTransactionHashCounter = {RandomNumber<ui32>()};

////////////////////////////////////////////////////////////////////////////////

class TTransactionManager::TImpl
    : public TRefCounted
{
public:
    TImpl(
        IConnectionPtr connection,
        const TString& user);

    TFuture<TTransactionPtr> Start(
        ETransactionType type,
        const TTransactionStartOptions& options);

    TTransactionPtr Attach(
        TTransactionId id,
        const TTransactionAttachOptions& options);

    void AbortAll();

private:
    friend class TTransaction;

    const TWeakPtr<IConnection> Connection_;
    const TTransactionManagerConfigPtr Config_;
    const TCellId PrimaryCellId_;
    const TCellTag PrimaryCellTag_;
    const TCellTagList SecondaryCellTags_;
    const TString User_;
    const ITimestampProviderPtr TimestampProvider_;
    const TCellDirectoryPtr CellDirectory_;
    const TCellTrackerPtr DownedCellTracker_;

    TSpinLock SpinLock_;
    THashSet<TTransaction::TImpl*> AliveTransactions_;


    static TRetryChecker GetCommitRetryChecker()
    {
        static const auto Result = BIND(&IsRetriableError);
        return Result;
    }

    static TRetryChecker GetAbortRetryChecker()
    {
        static const auto Result = BIND([] (const TError& error) {
            return
                IsRetriableError(error) ||
                error.FindMatching(NTransactionClient::EErrorCode::InvalidTransactionState);
        });
        return Result;
    }

    static TRetryChecker GetPingRetryChecker()
    {
        static const auto Result = BIND(&IsRetriableError);
        return Result;
    }

    static TRetryChecker GetCheckDownedParticipantsRetryChecker()
    {
        static const auto Result = BIND(&IsRetriableError);
        return Result;
    }

    TTransactionSupervisorServiceProxy MakeSupervisorProxy(IChannelPtr channel, TRetryChecker retryChecker)
    {
        if (retryChecker) {
            channel = CreateRetryingChannel(Config_, std::move(channel), std::move(retryChecker));
        }
        TTransactionSupervisorServiceProxy proxy(std::move(channel));
        proxy.SetDefaultTimeout(Config_->RpcTimeout);
        return proxy;
    }
};

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ETransactionState,
    (Initializing)
    (Active)
    (Aborted)
    (Committing)
    (Committed)
    (Detached)
);

class TTransaction::TImpl
    : public TRefCounted
{
public:
    explicit TImpl(TIntrusivePtr<TTransactionManager::TImpl> owner)
        : Owner_(owner)
        , Logger(NLogging::TLogger(TransactionClientLogger)
            .AddTag("ConnectionCellTag: %v",
                Owner_->PrimaryCellTag_))
    { }

    ~TImpl()
    {
        YT_LOG_DEBUG("Destroying transaction (TransactionId: %v)",
            Id_);

        Unregister();
    }


    TFuture<void> Start(
        ETransactionType type,
        const TTransactionStartOptions& options)
    {
        try {
            ValidateStartOptions(type, options);
        } catch (const std::exception& ex) {
            return MakeFuture<void>(ex);
        }

        Type_ = type;
        if (Type_ == ETransactionType::Master) {
            CoordinatorMasterCellTag_ = options.CoordinatorMasterCellTag == InvalidCellTag ? Owner_->PrimaryCellTag_ : options.CoordinatorMasterCellTag;
            CoordinatorMasterCellId_ = ReplaceCellTagInId(Owner_->PrimaryCellId_, CoordinatorMasterCellTag_);
            ReplicatedToMasterCellTags_ = options.ReplicateToMasterCellTags.value_or(Owner_->SecondaryCellTags_);
        }
        AutoAbort_ = options.AutoAbort;
        PingPeriod_ = options.PingPeriod;
        Ping_ = options.Ping;
        PingAncestors_ = options.PingAncestors;
        Timeout_ = options.Timeout;
        Atomicity_ = options.Atomicity;
        Durability_ = options.Durability;

        switch (Atomicity_) {
            case EAtomicity::Full:
                if (options.StartTimestamp != NullTimestamp) {
                    return OnGotStartTimestamp(options, options.StartTimestamp);
                } else {
                    YT_LOG_DEBUG("Generating transaction start timestamp");
                    return Owner_->TimestampProvider_->GenerateTimestamps()
                        .Apply(BIND(&TImpl::OnGotStartTimestamp, MakeStrong(this), options));
                }

            case EAtomicity::None:
                return StartNonAtomicTabletTransaction();

            default:
                YT_ABORT();
        }
    }

    void Attach(
        TTransactionId id,
        const TTransactionAttachOptions& options)
    {
        ValidateAttachOptions(id, options);

        Type_ = ETransactionType::Master;
        CoordinatorMasterCellTag_ = CellTagFromId(id);
        CoordinatorMasterCellId_ = ReplaceCellTagInId(Owner_->PrimaryCellId_, CoordinatorMasterCellTag_);
        Id_ = id;
        AutoAbort_ = options.AutoAbort;
        YT_VERIFY(!options.Sticky);
        PingPeriod_ = options.PingPeriod;
        Ping_ = options.Ping;
        PingAncestors_ = options.PingAncestors;
        State_ = ETransactionState::Active;
        YT_VERIFY(RegisteredParticipantIds_.insert(CoordinatorMasterCellId_).second);

        Register();

        YT_LOG_DEBUG("Master transaction attached (TransactionId: %v, AutoAbort: %v, Ping: %v, PingAncestors: %v)",
            Id_,
            AutoAbort_,
            Ping_,
            PingAncestors_);

        if (Ping_) {
            RunPeriodicPings();
        }
    }

    TFuture<TTransactionCommitResult> Commit(const TTransactionCommitOptions& options)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        {
            auto guard = Guard(SpinLock_);

            if (!Error_.IsOK()) {
                return MakeFuture<TTransactionCommitResult>(Error_);
            }

            switch (State_) {
                case ETransactionState::Committing:
                    return MakeFuture<TTransactionCommitResult>(TError("Transaction %v is already being committed",
                        Id_));

                case ETransactionState::Committed:
                    MakeFuture<TTransactionCommitResult>(TError("Transaction %v is already committed",
                        Id_));

                case ETransactionState::Aborted:
                    MakeFuture<TTransactionCommitResult>(TError("Transaction %v is already aborted",
                        Id_));

                case ETransactionState::Active:
                    State_ = ETransactionState::Committing;
                    break;

                default:
                    YT_ABORT();
            }
        }

        switch (Atomicity_) {
            case EAtomicity::Full:
                return DoCommitAtomic(options);

            case EAtomicity::None:
                return DoCommitNonAtomic();

            default:
                YT_ABORT();
        }
    }

    TFuture<void> Abort(const TTransactionAbortOptions& options = TTransactionAbortOptions())
    {
        VERIFY_THREAD_AFFINITY_ANY();

        YT_LOG_DEBUG("Transaction abort requested (TransactionId: %v)",
            Id_);
        SetAborted(TError("Transaction aborted by user request"));

        if (Atomicity_ != EAtomicity::Full) {
            return VoidFuture;
        }

        return SendAbort(options);
    }

    TFuture<void> Ping(const TTransactionPingOptions& options = {})
    {
        VERIFY_THREAD_AFFINITY_ANY();

        if (Atomicity_ != EAtomicity::Full) {
            return MakeFuture(TError("Cannot ping a transaction with %Qlv atomicity",
                Atomicity_));
        }

        return SendPing(options);
    }

    void Detach()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        YT_VERIFY(Atomicity_ == EAtomicity::Full);

        {
            auto guard = Guard(SpinLock_);
            State_ = ETransactionState::Detached;
        }

        YT_LOG_DEBUG("Transaction detached (TransactionId: %v)",
            Id_);
    }


    ETransactionType GetType() const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return Type_;
    }

    TTransactionId GetId() const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return Id_;
    }

    TTimestamp GetStartTimestamp() const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return StartTimestamp_;
    }

    ETransactionState GetState()
    {
        auto guard = Guard(SpinLock_);
        return State_;
    }

    EAtomicity GetAtomicity() const
    {
        return Atomicity_;
    }

    EDurability GetDurability() const
    {
        return Durability_;
    }

    TDuration GetTimeout() const
    {
        return Timeout_.value_or(Owner_->Config_->DefaultTransactionTimeout);
    }

    TCellTagList GetReplicatedToCellTags() const
    {
        return ReplicatedToMasterCellTags_;
    }


    void RegisterParticipant(TCellId cellId)
    {
        YT_VERIFY(
            TypeFromId(cellId) == EObjectType::TabletCell ||
            TypeFromId(cellId) == EObjectType::MasterCell);

        if (Atomicity_ != EAtomicity::Full) {
            return;
        }

        {
            auto guard = Guard(SpinLock_);

            if (State_ != ETransactionState::Active) {
                return;
            }

            if (RegisteredParticipantIds_.insert(cellId).second) {
                bool prepareOnly = IsReplicatedToMasterCell(cellId);
                YT_LOG_DEBUG("Transaction participant registered (TransactionId: %v, CellId: %v, PrepareOnly: %v)",
                    Id_,
                    cellId,
                    prepareOnly);
                if (prepareOnly) {
                    YT_VERIFY(PrepareOnlyRegisteredParticipantIds_.insert(cellId).second);
                }
            }
        }
    }

    void ChooseCoordinator(const TTransactionCommitOptions& options)
    {
        YT_VERIFY(!CoordinatorCellId_);

        if (Atomicity_ != EAtomicity::Full || RegisteredParticipantIds_.empty()) {
            return;
        }

        CoordinatorCellId_ = DoChooseCoordinator(options);
    }

    TFuture<void> ValidateNoDownedParticipants()
    {
        if (Atomicity_ != EAtomicity::Full) {
            return VoidFuture;
        }

        auto participantIds = Owner_->DownedCellTracker_->Select(GetRegisteredParticipantIds());
        return CheckDownedParticipants(participantIds);
    }

    void SubscribeCommitted(const TCallback<void()>& handler)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        Committed_.Subscribe(handler);
    }

    void UnsubscribeCommitted(const TCallback<void()>& handler)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        Committed_.Unsubscribe(handler);
    }


    void SubscribeAborted(const TCallback<void()>& handler)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        Aborted_.Subscribe(handler);
    }

    void UnsubscribeAborted(const TCallback<void()>& handler)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        Aborted_.Unsubscribe(handler);
    }

private:
    friend class TTransactionManager::TImpl;

    const TIntrusivePtr<TTransactionManager::TImpl> Owner_;

    ETransactionType Type_;

    // Only initialized for master transactions.
    TCellTag CoordinatorMasterCellTag_ = InvalidCellTag;
    TCellId CoordinatorMasterCellId_;
    TCellTagList ReplicatedToMasterCellTags_;

    bool AutoAbort_ = false;
    std::optional<TDuration> PingPeriod_;
    bool Ping_ = false;
    bool PingAncestors_ = false;
    std::optional<TDuration> Timeout_;
    EAtomicity Atomicity_ = EAtomicity::Full;
    EDurability Durability_ = EDurability::Sync;

    TSpinLock SpinLock_;

    ETransactionState State_ = ETransactionState::Initializing;

    TSingleShotCallbackList<void()> Committed_;
    TSingleShotCallbackList<void()> Aborted_;

    THashSet<TCellId> RegisteredParticipantIds_;
    THashSet<TCellId> PrepareOnlyRegisteredParticipantIds_;

    TCellId CoordinatorCellId_;

    TError Error_;

    TTimestamp StartTimestamp_ = NullTimestamp;
    TTransactionId Id_;

    const NLogging::TLogger Logger;


    static void ValidateStartOptions(
        ETransactionType type,
        const TTransactionStartOptions& options)
    {
        switch (type) {
            case ETransactionType::Master:
                ValidateMasterStartOptions(options);
                break;
            case ETransactionType::Tablet:
                ValidateTabletStartOptions(options);
                break;
            default:
                YT_ABORT();
        }
    }

    static void ValidateMasterStartOptions(const TTransactionStartOptions& options)
    {
        if (options.Id) {
            THROW_ERROR_EXCEPTION("Cannot use externally provided id for master transactions");
        }
        if (options.Atomicity != EAtomicity::Full) {
            THROW_ERROR_EXCEPTION("Atomicity must be %Qlv for master transactions",
                EAtomicity::Full);
        }
        if (options.Durability != EDurability::Sync) {
            THROW_ERROR_EXCEPTION("Durability must be %Qlv for master transactions",
                EDurability::Sync);
        }
    }

    static void ValidateTabletStartOptions(const TTransactionStartOptions& options)
    {
        if (options.CoordinatorMasterCellTag != InvalidCellTag) {
            THROW_ERROR_EXCEPTION("Cannot specify a master coordinator for tablet transaction");
        }
        if (options.ReplicateToMasterCellTags) {
            THROW_ERROR_EXCEPTION("Cannot request replication to master cells for tablet transaction");
        }
        if (options.ParentId) {
            THROW_ERROR_EXCEPTION("Tablet transaction cannot have a parent");
        }
        if (!options.PrerequisiteTransactionIds.empty()) {
            THROW_ERROR_EXCEPTION("Tablet transaction cannot have prerequisites");
        }
        if (options.Id) {
            auto type = TypeFromId(options.Id);
            if (type != EObjectType::AtomicTabletTransaction) {
                THROW_ERROR_EXCEPTION("Externally provided transaction id %v has invalid type",
                    options.Id);
            }
        }
        if (!options.Ping) {
            THROW_ERROR_EXCEPTION("Cannot switch off pings for a tablet transaction");
        }
        if (options.Atomicity == EAtomicity::Full && options.Durability != EDurability::Sync) {
            THROW_ERROR_EXCEPTION("Durability must be %Qlv for tablet transactions with %Qlv atomicity",
                EDurability::Sync,
                EAtomicity::Full);
        }
    }

    static void ValidateAttachOptions(
        TTransactionId id,
        const TTransactionAttachOptions& options)
    {
        ValidateMasterTransactionId(id);
        // NB: Sticky transactions are handled in TNativeClient.
        YT_VERIFY(!options.Sticky);
    }


    void Register()
    {
        if (AutoAbort_) {
            auto guard = Guard(Owner_->SpinLock_);
            YT_VERIFY(Owner_->AliveTransactions_.insert(this).second);
        }
    }

    void Unregister()
    {
        if (AutoAbort_) {
            {
                auto guard = Guard(Owner_->SpinLock_);
                // NB: Instance is not necessarily registered.
                Owner_->AliveTransactions_.erase(this);
            }

            if (State_ == ETransactionState::Active) {
                SendAbort();
            }
        }
    }


    TFuture<void> OnGotStartTimestamp(const TTransactionStartOptions& options, TTimestamp timestamp)
    {
        StartTimestamp_ = timestamp;

        Register();

        YT_LOG_DEBUG("Starting transaction (StartTimestamp: %llx, Type: %v)",
            StartTimestamp_,
            Type_);

        switch (Type_) {
            case ETransactionType::Master:
                return StartMasterTransaction(options);
            case ETransactionType::Tablet:
                return StartAtomicTabletTransaction(options);
            default:
                YT_ABORT();
        }
    }

    TFuture<void> StartMasterTransaction(const TTransactionStartOptions& options)
    {
        auto connection = Owner_->Connection_.Lock();
        if (!connection) {
            THROW_ERROR_EXCEPTION("Unable to start master transaction: connection terminated");
        }

        auto channel = connection->GetMasterChannelOrThrow(EMasterChannelKind::Leader, CoordinatorMasterCellTag_);

        TTransactionServiceProxy proxy(channel);
        auto req = proxy.StartTransaction();
        req->SetUser(Owner_->User_);
        auto attributes = options.Attributes
            ? options.Attributes->Clone()
            : CreateEphemeralAttributes();
        auto optionalTitle = attributes->FindAndRemove<TString>("title");
        if (optionalTitle) {
            req->set_title(*optionalTitle);
        }
        ToProto(req->mutable_attributes(), *attributes);
        req->set_timeout(ToProto<i64>(GetTimeout()));
        if (options.ParentId) {
            ToProto(req->mutable_parent_id(), options.ParentId);
        }
        ToProto(req->mutable_prerequisite_transaction_ids(), options.PrerequisiteTransactionIds);
        if (options.Deadline) {
            req->set_deadline(ToProto<ui64>(*options.Deadline));
        }
        if (options.ReplicateToMasterCellTags) {
            if (options.ReplicateToMasterCellTags->empty()) {
                req->set_dont_replicate(true);
            } else {
                for (auto tag : *options.ReplicateToMasterCellTags) {
                    if (tag != CoordinatorMasterCellTag_) {
                        req->add_replicate_to_cell_tags(tag);
                    }
                }
            }
        }
        SetOrGenerateMutationId(req, options.MutationId, options.Retry);

        return req->Invoke().Apply(
            BIND(&TImpl::OnMasterTransactionStarted, MakeStrong(this)));
    }

    TError OnMasterTransactionStarted(const TTransactionServiceProxy::TErrorOrRspStartTransactionPtr& rspOrError)
    {
        if (!rspOrError.IsOK()) {
            State_ = ETransactionState::Aborted;
            return rspOrError;
        }

        State_ = ETransactionState::Active;
        YT_VERIFY(RegisteredParticipantIds_.insert(CoordinatorMasterCellId_).second);

        const auto& rsp = rspOrError.Value();
        Id_ = FromProto<TTransactionId>(rsp->id());

        YT_LOG_DEBUG("Master transaction started (TransactionId: %v, CellTag: %v, ReplicatedToCellTags: %v, "
            "StartTimestamp: %llx, AutoAbort: %v, Ping: %v, PingAncestors: %v)",
            Id_,
            CoordinatorMasterCellTag_,
            ReplicatedToMasterCellTags_,
            StartTimestamp_,
            AutoAbort_,
            Ping_,
            PingAncestors_);

        if (Ping_) {
            RunPeriodicPings();
        }

        return TError();
    }

    TFuture<void> StartAtomicTabletTransaction(const TTransactionStartOptions& options)
    {
        YT_VERIFY(Atomicity_ == EAtomicity::Full);
        YT_VERIFY(Durability_ == EDurability::Sync);

        Id_ = options.Id
            ? options.Id
            : MakeTabletTransactionId(
                Atomicity_,
                Owner_->PrimaryCellTag_,
                StartTimestamp_,
                TabletTransactionHashCounter++);

        State_ = ETransactionState::Active;

        YT_LOG_DEBUG("Atomic tablet transaction started (TransactionId: %v, StartTimestamp: %llx, AutoAbort: %v)",
            Id_,
            StartTimestamp_,
            AutoAbort_);

        // Start ping scheduling.
        // Participants will be added into it upon arrival.
        YT_VERIFY(Ping_);
        RunPeriodicPings();

        return VoidFuture;
    }

    TFuture<void> StartNonAtomicTabletTransaction()
    {
        YT_VERIFY(Atomicity_ == EAtomicity::None);

        StartTimestamp_ = InstantToTimestamp(TInstant::Now()).first;

        Id_ = MakeTabletTransactionId(
            Atomicity_,
            Owner_->PrimaryCellTag_,
            StartTimestamp_,
            TabletTransactionHashCounter++);

        State_ = ETransactionState::Active;

        YT_LOG_DEBUG("Non-atomic tablet transaction started (TransactionId: %v, Durability: %v)",
            Id_,
            Durability_);

        return VoidFuture;
    }

    void FireCommitted()
    {
        Committed_.Fire();
    }

    TError SetCommitted(const TTransactionCommitResult& result)
    {
        {
            auto guard = Guard(SpinLock_);
            if (State_ != ETransactionState::Committing) {
                return Error_;
            }
            State_ = ETransactionState::Committed;
        }

        FireCommitted();

        YT_LOG_DEBUG("Transaction committed (TransactionId: %v, CommitTimestamps: %v)",
            Id_,
            result.CommitTimestamps);

        return TError();
    }

    TFuture<TTransactionCommitResult> DoCommitAtomic(const TTransactionCommitOptions& options)
    {
        if (RegisteredParticipantIds_.empty()) {
            TTransactionCommitResult result;
            auto error = SetCommitted(result);
            if (!error.IsOK()) {
                return MakeFuture<TTransactionCommitResult>(error);
            }
            return MakeFuture(result);
        }

        try {
            YT_VERIFY(CoordinatorCellId_);

            auto supervisorParticipantCellIds = GetSupervisorParticipantIds();
            auto supervisorPrepareOnlyParticipantCellIds = GetSupervisorPrepareOnlyParticipantIds();
            YT_LOG_DEBUG("Committing transaction (TransactionId: %v, CoordinatorCellId: %v, "
                "ParticipantCellIds: %v, PrepareOnlyParticipantCellIds: %v)",
                Id_,
                CoordinatorCellId_,
                supervisorParticipantCellIds,
                supervisorPrepareOnlyParticipantCellIds);

            auto coordinatorChannel = Owner_->CellDirectory_->GetChannelOrThrow(CoordinatorCellId_);
            auto proxy = Owner_->MakeSupervisorProxy(std::move(coordinatorChannel), Owner_->GetCommitRetryChecker());
            auto req = proxy.CommitTransaction();
            req->SetUser(Owner_->User_);
            ToProto(req->mutable_transaction_id(), Id_);
            ToProto(req->mutable_participant_cell_ids(), supervisorParticipantCellIds);
            ToProto(req->mutable_prepare_only_participant_cell_ids(), supervisorPrepareOnlyParticipantCellIds);
            req->set_force_2pc(options.Force2PC);
            req->set_generate_prepare_timestamp(options.GeneratePrepareTimestamp);
            req->set_inherit_commit_timestamp(options.InheritCommitTimestamp);
            req->set_coordinator_commit_mode(static_cast<int>(options.CoordinatorCommitMode));
            SetOrGenerateMutationId(req, options.MutationId, options.Retry);

            return req->Invoke().Apply(
                BIND(&TImpl::OnAtomicTransactionCommitted, MakeStrong(this), CoordinatorCellId_));
        } catch (const std::exception& ex) {
            return MakeFuture<TTransactionCommitResult>(TError(ex));
        }
    }

    TFuture<TTransactionCommitResult> DoCommitNonAtomic()
    {
        TTransactionCommitResult result;
        auto error = SetCommitted(result);
        if (!error.IsOK()) {
            return MakeFuture<TTransactionCommitResult>(error);
        }
        return MakeFuture(result);
    }


    TCellId DoChooseCoordinator(const TTransactionCommitOptions& options)
    {
        if (Type_ == ETransactionType::Master) {
            return CoordinatorMasterCellId_;
        }

        if (options.CoordinatorCellId) {
            if (!IsParticipantRegistered(options.CoordinatorCellId)) {
                THROW_ERROR_EXCEPTION("Cell %v is not a participant and thus cannot be explicitly",
                    options.CoordinatorCellId);
            }
            return options.CoordinatorCellId;
        }

        auto connection = Owner_->Connection_.Lock();
        if (!connection) {
            THROW_ERROR_EXCEPTION(NYT::EErrorCode::Canceled, "Connection destoyred");

        }

        std::vector<TCellId> candidateIds;
        auto coordinatorCellTag = connection->GetPrimaryMasterCellTag();
        for (auto cellId : GetRegisteredParticipantIds()) {
            if (CellTagFromId(cellId) == coordinatorCellTag) {
                candidateIds.push_back(cellId);
            }
        }

        if (candidateIds.empty()) {
            THROW_ERROR_EXCEPTION("No coordinator can be chosen");
        }

        return candidateIds[RandomNumber(candidateIds.size())];
    }

    void UpdateDownedParticipants()
    {
        auto participantIds = GetRegisteredParticipantIds();
        CheckDownedParticipants(participantIds);
    }


    TFuture<void> CheckDownedParticipants(std::vector<TCellId> participantIds)
    {
        if (participantIds.empty()) {
            return VoidFuture;
        }

        YT_VERIFY(CoordinatorCellId_);
        auto coordinatorChannel = Owner_->CellDirectory_->GetChannelOrThrow(CoordinatorCellId_);
        auto proxy = Owner_->MakeSupervisorProxy(std::move(coordinatorChannel), Owner_->GetCheckDownedParticipantsRetryChecker());
        auto req = proxy.GetDownedParticipants();
        req->SetUser(Owner_->User_);
        ToProto(req->mutable_cell_ids(), participantIds);

        return req->Invoke().Apply(
            BIND([this, this_ = MakeStrong(this), participantIds = std::move(participantIds)] (
                const TTransactionSupervisorServiceProxy::TErrorOrRspGetDownedParticipantsPtr& rspOrError)
            {
                if (rspOrError.IsOK()) {
                    const auto& rsp = rspOrError.Value();
                    auto downedParticipantIds = FromProto<std::vector<TCellId>>(rsp->cell_ids());
                    Owner_->DownedCellTracker_->Update(participantIds, downedParticipantIds);

                    if (!downedParticipantIds.empty()) {
                        YT_LOG_DEBUG("Some transaction participants are known to be down (CellIds: %v)",
                            downedParticipantIds);
                        return TError(
                            NTransactionClient::EErrorCode::SomeParticipantsAreDown,
                            "Some transaction participants are known to be down")
                            << TErrorAttribute("downed_participants", downedParticipantIds);
                    }
                } else {
                    YT_LOG_WARNING("Error updating downed participants (CellId: %v)",
                        CoordinatorCellId_);
                    return TError("Error updating downed participants at cell %v",
                        CoordinatorCellId_)
                        << rspOrError;
                }
                return TError();
            }));
    }


    TErrorOr<TTransactionCommitResult> OnAtomicTransactionCommitted(
        TCellId cellId,
        const TTransactionSupervisorServiceProxy::TErrorOrRspCommitTransactionPtr& rspOrError)
    {
        if (!rspOrError.IsOK()) {
            auto error = TError("Error committing transaction %v at cell %v",
                Id_,
                cellId)
                << rspOrError;
            UpdateDownedParticipants();
            OnFailure(error);
            return error;
        }

        const auto& rsp = rspOrError.Value();
        TTransactionCommitResult result;
        result.CommitTimestamps = FromProto<TTimestampMap>(rsp->commit_timestamps());
        auto error = SetCommitted(result);
        if (!error.IsOK()) {
            return error;
        }
        return result;
    }


    TFuture<void> SendPing(const TTransactionPingOptions& options)
    {
        std::vector<TFuture<void>> asyncResults;
        for (auto cellId : GetRegisteredParticipantIds()) {
            if (IsReplicatedToMasterCell(cellId)) {
                continue;
            }
            
            YT_LOG_DEBUG("Pinging transaction (TransactionId: %v, CellId: %v)",
                Id_,
                cellId);

            auto channel = Owner_->CellDirectory_->FindChannel(cellId);
            if (!channel) {
                continue;
            }

            auto proxy = Owner_->MakeSupervisorProxy(
                std::move(channel),
                options.EnableRetries ? Owner_->GetPingRetryChecker() : TRetryChecker());
            auto req = proxy.PingTransaction();
            req->SetUser(Owner_->User_);
            ToProto(req->mutable_transaction_id(), Id_);
            if (cellId == CoordinatorMasterCellId_) {
                req->set_ping_ancestors(PingAncestors_);
            }

            auto asyncRspOrError = req->Invoke();
            asyncResults.push_back(asyncRspOrError.Apply(
                BIND([=, this_ = MakeStrong(this)] (const TTransactionSupervisorServiceProxy::TErrorOrRspPingTransactionPtr& rspOrError) {
                    if (rspOrError.IsOK()) {
                        YT_LOG_DEBUG("Transaction pinged (TransactionId: %v, CellId: %v)",
                            Id_,
                            cellId);
                        return TError();
                    } else if (rspOrError.GetCode() == NTransactionClient::EErrorCode::NoSuchTransaction &&
                               TypeFromId(cellId) == EObjectType::MasterCell)
                    {
                        // Hard error.
                        // NB: For tablet cells transactions are started asynchronously so NoSuchTransaction
                        // is not considered fatal.
                        YT_LOG_DEBUG("Transaction has expired or was aborted (TransactionId: %v, CellId: %v)",
                            Id_,
                            cellId);
                        auto error = TError(
                            NTransactionClient::EErrorCode::NoSuchTransaction,
                            "Transaction %v has expired or was aborted at cell %v",
                            Id_,
                            cellId);
                        if (GetState() == ETransactionState::Active) {
                            OnFailure(error);
                        }
                        return error;
                    } else {
                        // Soft error.
                        YT_LOG_DEBUG(rspOrError, "Error pinging transaction (TransactionId: %v, CellId: %v)",
                            Id_,
                            cellId);
                        return TError("Failed to ping transaction %v at cell %v",
                            Id_,
                            cellId)
                            << rspOrError;
                    }
                })));
        }

        return Combine(asyncResults);
    }

    void RunPeriodicPings()
    {
        if (!IsPingableState()) {
            YT_LOG_DEBUG("Transaction is not in pingable state (TransactionId: %v, State: %v)",
                Id_,
                GetState());
            return;
        }

        // COMPAT(shakurov): disable retries here once all clients have retries enabled.
        TTransactionPingOptions options{
            .EnableRetries = true
        };
        SendPing(options).Subscribe(BIND([=, this_ = MakeStrong(this)] (const TError& error) {
            if (!IsPingableState()) {
                YT_LOG_DEBUG("Transaction is not in pingable state (TransactionId: %v, State: %v)",
                    Id_,
                    GetState());
                return;
            }

            if (error.FindMatching(NYT::EErrorCode::Timeout)) {
                RunPeriodicPings();
                return;
            }

            YT_LOG_DEBUG("Transaction ping scheduled (TransactionId: %v)",
                Id_);

            auto pingPeriod = std::min(PingPeriod_.value_or(Owner_->Config_->DefaultPingPeriod), GetTimeout() / 2);
            TDelayedExecutor::Submit(BIND(&TImpl::RunPeriodicPings, MakeWeak(this)), pingPeriod);
        }));
    }

    bool IsPingableState()
    {
        auto state = GetState();
        // NB: We have to continue pinging the transaction while committing.
        return state == ETransactionState::Active || state == ETransactionState::Committing;
    }


    TFuture<void> SendAbort(const TTransactionAbortOptions& options = TTransactionAbortOptions())
    {
        YT_LOG_DEBUG("Aborting transaction (TransactionId: %v)",
            Id_);

        std::vector<TFuture<void>> asyncResults;
        auto participantIds = GetRegisteredParticipantIds();
        for (auto cellId : participantIds) {
            auto channel = Owner_->CellDirectory_->FindChannel(cellId);
            if (!channel) {
                continue;
            }

            YT_LOG_DEBUG("Sending abort to participant (TransactionId: %v, ParticipantCellId: %v)",
                Id_,
                cellId);

            auto proxy = Owner_->MakeSupervisorProxy(std::move(channel), Owner_->GetAbortRetryChecker());
            auto req = proxy.AbortTransaction();
            req->SetHeavy(true);
            req->SetUser(Owner_->User_);
            ToProto(req->mutable_transaction_id(), Id_);
            req->set_force(options.Force);
            SetMutationId(req, options.MutationId, options.Retry);

            auto asyncRspOrError = req->Invoke();
            // NB: "this" could be dying; can't capture it.
            auto transactionId = Id_;
            asyncResults.push_back(asyncRspOrError.Apply(
                BIND([=, Logger = Logger] (const TTransactionSupervisorServiceProxy::TErrorOrRspAbortTransactionPtr& rspOrError) {
                    if (rspOrError.IsOK()) {
                        YT_LOG_DEBUG("Transaction aborted (TransactionId: %v, CellId: %v)",
                            transactionId,
                            cellId);
                        return TError();
                    } else if (rspOrError.GetCode() == NTransactionClient::EErrorCode::NoSuchTransaction) {
                        YT_LOG_DEBUG("Transaction has expired or was already aborted, ignored (TransactionId: %v, CellId: %v)",
                            transactionId,
                            cellId);
                        return TError();
                    } else {
                        YT_LOG_WARNING(rspOrError, "Error aborting transaction (TransactionId: %v, CellId: %v)",
                            transactionId,
                            cellId);
                        return TError("Error aborting transaction %v at cell %v",
                            transactionId,
                            cellId)
                            << rspOrError;
                    }
                })));
        }

        return Combine(asyncResults);
    }


    void FireAborted()
    {
        Aborted_.Fire();
    }

    void SetAborted(const TError& error)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        {
            auto guard = Guard(SpinLock_);
            if (State_ == ETransactionState::Aborted) {
                return;
            }
            State_ = ETransactionState::Aborted;
            Error_ = error;
        }

        FireAborted();
    }

    void OnFailure(const TError& error)
    {
        SetAborted(error);
        // Best-effort, fire-and-forget.
        SendAbort();
    }

    std::vector<TCellId> GetRegisteredParticipantIds()
    {
        auto guard = Guard(SpinLock_);
        return std::vector<TCellId>(RegisteredParticipantIds_.begin(), RegisteredParticipantIds_.end());
    }

    std::vector<TCellId> GetSupervisorParticipantIds()
    {
        auto guard = Guard(SpinLock_);
        std::vector<TCellId> result;
        result.reserve(RegisteredParticipantIds_.size());
        for (auto cellId : RegisteredParticipantIds_) {
            if (cellId != CoordinatorCellId_) {
                result.push_back(cellId);
            }
        }
        return result;
    }

    std::vector<TCellId> GetSupervisorPrepareOnlyParticipantIds()
    {
        auto guard = Guard(SpinLock_);
        std::vector<TCellId> result;
        result.reserve(RegisteredParticipantIds_.size());
        for (auto cellId : PrepareOnlyRegisteredParticipantIds_) {
            if (cellId != CoordinatorCellId_) {
                result.push_back(cellId);
            }
        }
        return result;
    }

    bool IsParticipantRegistered(TCellId cellId)
    {
        auto guard = Guard(SpinLock_);
        return RegisteredParticipantIds_.find(cellId) != RegisteredParticipantIds_.end();
    }
    
    bool IsReplicatedToMasterCell(TCellId cellId)
    {
        return
            TypeFromId(cellId) == EObjectType::MasterCell &&
            std::find(ReplicatedToMasterCellTags_.begin(), ReplicatedToMasterCellTags_.end(), CellTagFromId(cellId)) != ReplicatedToMasterCellTags_.end();
    }
};

////////////////////////////////////////////////////////////////////////////////

TTransactionManager::TImpl::TImpl(
    IConnectionPtr connection,
    const TString& user)
    : Connection_(connection)
    , Config_(connection->GetConfig()->TransactionManager)
    , PrimaryCellId_(connection->GetPrimaryMasterCellId())
    , PrimaryCellTag_(connection->GetPrimaryMasterCellTag())
    , SecondaryCellTags_(connection->GetSecondaryMasterCellTags())
    , User_(user)
    , TimestampProvider_(connection->GetTimestampProvider())
    , CellDirectory_(connection->GetCellDirectory())
    , DownedCellTracker_(connection->GetDownedCellTracker())
{ }

TFuture<TTransactionPtr> TTransactionManager::TImpl::Start(
    ETransactionType type,
    const TTransactionStartOptions& options)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto transaction = New<TTransaction::TImpl>(this);
    return transaction->Start(type, options).Apply(BIND([=] () {
        return TTransaction::Create(transaction);
    }));
}

TTransactionPtr TTransactionManager::TImpl::Attach(
    TTransactionId id,
    const TTransactionAttachOptions& options)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto transaction = New<TTransaction::TImpl>(this);
    transaction->Attach(id, options);
    return TTransaction::Create(transaction);
}

void TTransactionManager::TImpl::AbortAll()
{
    VERIFY_THREAD_AFFINITY_ANY();

    std::vector<TIntrusivePtr<TTransaction::TImpl>> transactions;
    {
        auto guard = Guard(SpinLock_);
        for (auto* rawTransaction : AliveTransactions_) {
            auto transaction = DangerousGetPtr(rawTransaction);
            if (transaction) {
                transactions.push_back(transaction);
            }
        }
    }

    for (const auto& transaction : transactions) {
        transaction->Abort();
    }
}

////////////////////////////////////////////////////////////////////////////////

TTransaction::TTransaction(TIntrusivePtr<TImpl> impl)
    : Impl_(std::move(impl))
{ }

TTransactionPtr TTransaction::Create(TIntrusivePtr<TImpl> impl)
{
    return New<TTransaction>(std::move(impl));
}

TTransaction::~TTransaction() = default;

TFuture<TTransactionCommitResult> TTransaction::Commit(const TTransactionCommitOptions& options)
{
    return Impl_->Commit(options);
}

TFuture<void> TTransaction::Abort(const TTransactionAbortOptions& options)
{
    return Impl_->Abort(options);
}

void TTransaction::Detach()
{
    Impl_->Detach();
}

TFuture<void> TTransaction::Ping(const TTransactionPingOptions& options)
{
    return Impl_->Ping(options);
}

ETransactionType TTransaction::GetType() const
{
    return Impl_->GetType();
}

TTransactionId TTransaction::GetId() const
{
    return Impl_->GetId();
}

TTimestamp TTransaction::GetStartTimestamp() const
{
    return Impl_->GetStartTimestamp();
}

EAtomicity TTransaction::GetAtomicity() const
{
    return Impl_->GetAtomicity();
}

EDurability TTransaction::GetDurability() const
{
    return Impl_->GetDurability();
}

TDuration TTransaction::GetTimeout() const
{
    return Impl_->GetTimeout();
}

TCellTagList TTransaction::GetReplicatedToCellTags() const
{
    return Impl_->GetReplicatedToCellTags();
}

void TTransaction::RegisterParticipant(TCellId cellId)
{
    Impl_->RegisterParticipant(cellId);
}

void TTransaction::ChooseCoordinator(const TTransactionCommitOptions& options)
{
    Impl_->ChooseCoordinator(options);
}

TFuture<void> TTransaction::ValidateNoDownedParticipants()
{
    return Impl_->ValidateNoDownedParticipants();
}

DELEGATE_SIGNAL(TTransaction, void(), Committed, *Impl_);
DELEGATE_SIGNAL(TTransaction, void(), Aborted, *Impl_);

////////////////////////////////////////////////////////////////////////////////

TTransactionManager::TTransactionManager(
    IConnectionPtr connection,
    const TString& user)
    : Impl_(New<TImpl>(
        std::move(connection),
        user))
{ }

TTransactionManager::~TTransactionManager()
{ }

TFuture<TTransactionPtr> TTransactionManager::Start(
    ETransactionType type,
    const TTransactionStartOptions& options)
{
    return Impl_->Start(type, options);
}

TTransactionPtr TTransactionManager::Attach(
    TTransactionId id,
    const TTransactionAttachOptions& options)
{
    return Impl_->Attach(id, options);
}

void TTransactionManager::AbortAll()
{
    Impl_->AbortAll();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionClient
