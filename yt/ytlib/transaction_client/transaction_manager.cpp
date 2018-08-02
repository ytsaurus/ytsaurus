#include "transaction_manager.h"
#include "private.h"
#include "config.h"
#include "helpers.h"
#include "action.h"

#include <yt/ytlib/api/native/connection.h>

#include <yt/ytlib/hive/cell_directory.h>
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

namespace NYT {
namespace NTransactionClient {

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

static std::atomic<ui32> TabletTransactionHashCounter; // used as a part of transaction id

////////////////////////////////////////////////////////////////////////////////

class TTransactionManager::TImpl
    : public TRefCounted
{
public:
    TImpl(
        TTransactionManagerConfigPtr config,
        const TCellId& primaryCellId,
        IConnectionPtr connection,
        const TString& user,
        ITimestampProviderPtr timestampProvider,
        TCellDirectoryPtr cellDirectory);

    TFuture<TTransactionPtr> Start(
        ETransactionType type,
        const TTransactionStartOptions& options);

    TTransactionPtr Attach(
        const TTransactionId& id,
        const TTransactionAttachOptions& options);

    void AbortAll();

private:
    friend class TTransaction;
    class TCellTracker;

    const TTransactionManagerConfigPtr Config_;
    const TCellId PrimaryCellId_;
    const TWeakPtr<IConnection> Connection_;

    const TString User_;
    const ITimestampProviderPtr TimestampProvider_;
    const TCellDirectoryPtr CellDirectory_;
    const TIntrusivePtr<TCellTracker> DownedCellTracker_;

    TSpinLock SpinLock_;
    THashSet<TTransaction::TImpl*> AliveTransactions_;


    TTransactionSupervisorServiceProxy MakeSupervisorProxy(IChannelPtr channel, bool retry)
    {
        if (retry) {
            channel = CreateRetryingChannel(Config_, std::move(channel));
        }
        TTransactionSupervisorServiceProxy proxy(std::move(channel));
        proxy.SetDefaultTimeout(Config_->RpcTimeout);
        return proxy;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TTransactionManager::TImpl::TCellTracker
    : public TRefCounted
{
public:
    std::vector<TCellId> Select(const std::vector<TCellId>& candidates)
    {
        auto guard = Guard(SpinLock_);

        std::vector<TCellId> result;
        result.reserve(candidates.size());

        for (const auto& id : candidates) {
            if (CellIds_.find(id) != CellIds_.end()) {
                result.push_back(id);
            }
        }

        return result;
    }

    void Update(const std::vector<TCellId>& toRemove, const std::vector<TCellId>& toAdd)
    {
        auto guard = Guard(SpinLock_);

        for (const auto& id : toRemove) {
            CellIds_.erase(id);
        }
        for (const auto& id : toAdd) {
            CellIds_.insert(id);
        }
    }

private:
    TSpinLock SpinLock_;
    THashSet<TCellId> CellIds_;
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
                CellTagFromId(Owner_->PrimaryCellId_)))
    { }

    ~TImpl()
    {
        LOG_DEBUG("Destroy transaction (TransactionId: %v)",
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
        CellTag_ = options.CellTag == PrimaryMasterCellTag
            ? CellTagFromId(Owner_->PrimaryCellId_)
            : options.CellTag;
        CellId_ = options.CellTag == PrimaryMasterCellTag
            ? Owner_->PrimaryCellId_
            : ReplaceCellTagInId(Owner_->PrimaryCellId_, CellTag_);
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
                    LOG_DEBUG("Generating transaction start timestamp");
                    return Owner_->TimestampProvider_->GenerateTimestamps()
                        .Apply(BIND(&TImpl::OnGotStartTimestamp, MakeStrong(this), options));
                }

            case EAtomicity::None:
                return StartNonAtomicTabletTransaction();

            default:
                Y_UNREACHABLE();
        }
    }

    void Attach(
        const TTransactionId& id,
        const TTransactionAttachOptions& options)
    {
        ValidateAttachOptions(id, options);

        Type_ = ETransactionType::Master;
        CellTag_ = CellTagFromId(Owner_->PrimaryCellId_);
        CellId_ = Owner_->PrimaryCellId_;
        Id_ = id;
        AutoAbort_ = options.AutoAbort;
        YCHECK(!options.Sticky);
        PingPeriod_ = options.PingPeriod;
        Ping_ = options.Ping;
        PingAncestors_ = options.PingAncestors;
        State_ = ETransactionState::Active;
        YCHECK(RegisteredParticipantIds_.insert(CellId_).second);
        YCHECK(ConfirmedParticipantIds_.insert(CellId_).second);

        Register();

        LOG_DEBUG("Master transaction attached (TransactionId: %v, AutoAbort: %v, Ping: %v, PingAncestors: %v)",
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
                    Y_UNREACHABLE();
            }
        }

        switch (Atomicity_) {
            case EAtomicity::Full:
                return DoCommitAtomic(options);

            case EAtomicity::None:
                return DoCommitNonAtomic();

            default:
                Y_UNREACHABLE();
        }
    }

    TFuture<void> Abort(const TTransactionAbortOptions& options = TTransactionAbortOptions())
    {
        VERIFY_THREAD_AFFINITY_ANY();

        LOG_DEBUG("Transaction abort requested (TransactionId: %v)",
            Id_);
        SetAborted(TError("Transaction aborted by user request"));

        if (Atomicity_ != EAtomicity::Full) {
            return VoidFuture;
        }

        return SendAbort(options);
    }

    TFuture<void> Ping()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        if (Atomicity_ != EAtomicity::Full) {
            return MakeFuture(TError("Cannot ping a transaction with %Qlv atomicity",
                Atomicity_));
        }

        return SendPing(true);
    }

    void Detach()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        YCHECK(Atomicity_ == EAtomicity::Full);

        auto guard = Guard(SpinLock_);

        switch (State_) {
            case ETransactionState::Committed:
                THROW_ERROR_EXCEPTION("Transaction %v is already committed",
                    Id_);

            case ETransactionState::Aborted:
                THROW_ERROR_EXCEPTION("Transaction %v is already aborted",
                    Id_);

            case ETransactionState::Active:
                State_ = ETransactionState::Detached;
                break;

            case ETransactionState::Detached:
                return;

            default:
                Y_UNREACHABLE();
        }

        LOG_DEBUG("Transaction detached (TransactionId: %v)",
            Id_);
    }


    ETransactionType GetType() const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return Type_;
    }

    const TTransactionId& GetId() const
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
        return Timeout_.Get(Owner_->Config_->DefaultTransactionTimeout);
    }


    void RegisterParticipant(const TCellId& cellId)
    {
        YCHECK(TypeFromId(cellId) == EObjectType::TabletCell ||
               TypeFromId(cellId) == EObjectType::ClusterCell);


        if (Atomicity_ != EAtomicity::Full) {
            return;
        }

        {
            auto guard = Guard(SpinLock_);

            if (State_ != ETransactionState::Active) {
                return;
            }

            if (RegisteredParticipantIds_.insert(cellId).second) {
                LOG_DEBUG("Transaction participant registered (TransactionId: %v, CellId: %v)",
                    Id_,
                    cellId);
            }
        }
    }

    void ConfirmParticipant(const TCellId& cellId)
    {
        YCHECK(TypeFromId(cellId) == EObjectType::TabletCell);


        if (Atomicity_ != EAtomicity::Full) {
            return;
        }

        {
            auto guard = Guard(SpinLock_);

            if (State_ != ETransactionState::Active) {
                return;
            }

            YCHECK(RegisteredParticipantIds_.find(cellId) != RegisteredParticipantIds_.end());
            if (ConfirmedParticipantIds_.insert(cellId).second) {
                LOG_DEBUG("Transaction participant confirmed (TransactionId: %v, CellId: %v)",
                    Id_,
                    cellId);
            }
        }
    }

    void ChooseCoordinator(const TTransactionCommitOptions& options)
    {
        YCHECK(!CoordinatorCellId_);

        if (Atomicity_ != EAtomicity::Full || RegisteredParticipantIds_.empty()) {
            return;
        }

        CoordinatorCellId_ = PickCoordinator(options);
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
    TCellTag CellTag_;
    TCellId CellId_;
    bool AutoAbort_ = false;
    TNullable<TDuration> PingPeriod_;
    bool Ping_ = false;
    bool PingAncestors_ = false;
    TNullable<TDuration> Timeout_;
    EAtomicity Atomicity_ = EAtomicity::Full;
    EDurability Durability_ = EDurability::Sync;

    TSpinLock SpinLock_;

    ETransactionState State_ = ETransactionState::Initializing;

    TSingleShotCallbackList<void()> Committed_;
    TSingleShotCallbackList<void()> Aborted_;

    THashSet<TCellId> RegisteredParticipantIds_;
    THashSet<TCellId> ConfirmedParticipantIds_;

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
                Y_UNREACHABLE();
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
        const TTransactionId& id,
        const TTransactionAttachOptions& options)
    {
        ValidateMasterTransactionId(id);
        // NB: Sticky transactions are handled in TNativeClient.
        YCHECK(!options.Sticky);
    }


    void Register()
    {
        if (AutoAbort_) {
            auto guard = Guard(Owner_->SpinLock_);
            YCHECK(Owner_->AliveTransactions_.insert(this).second);
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

        LOG_DEBUG("Starting transaction (StartTimestamp: %llx, Type: %v)",
            StartTimestamp_,
            Type_);

        switch (Type_) {
            case ETransactionType::Master:
                return StartMasterTransaction(options);
            case ETransactionType::Tablet:
                return StartAtomicTabletTransaction(options);
            default:
                Y_UNREACHABLE();
        }
    }

    TFuture<void> StartMasterTransaction(const TTransactionStartOptions& options)
    {
        auto connection = Owner_->Connection_.Lock();
        if (!connection) {
            THROW_ERROR_EXCEPTION("Unable to start master transaction: connection terminated");
        }

        auto cellTag = options.Multicell
            ? CellTagFromId(Owner_->PrimaryCellId_)
            : CellTag_;

        auto channel = connection->GetMasterChannelOrThrow(EMasterChannelKind::Leader, cellTag);

        TTransactionServiceProxy proxy(channel);
        auto req = proxy.StartTransaction();
        req->SetUser(Owner_->User_);
        auto attributes = options.Attributes
            ? options.Attributes->Clone()
            : CreateEphemeralAttributes();
        auto maybeTitle = attributes->FindAndRemove<TString>("title");
        if (maybeTitle) {
            req->set_title(*maybeTitle);
        }
        ToProto(req->mutable_attributes(), *attributes);
        req->set_timeout(ToProto<i64>(GetTimeout()));
        if (options.ParentId) {
            ToProto(req->mutable_parent_id(), options.ParentId);
        }
        ToProto(req->mutable_prerequisite_transaction_ids(), options.PrerequisiteTransactionIds);
        SetOrGenerateMutationId(req, options.MutationId, options.Retry);

        if (options.Multicell) {
            auto replicateToCellTags = TCellTagList{CellTag_};
            ToProto(req->mutable_replicate_to_cell_tags(), replicateToCellTags);
        }

        return req->Invoke().Apply(
            BIND(&TImpl::OnMasterTransactionStarted, MakeStrong(this), cellTag));
    }

    TError OnMasterTransactionStarted(TCellTag cellTag, const TTransactionServiceProxy::TErrorOrRspStartTransactionPtr& rspOrError)
    {
        if (!rspOrError.IsOK()) {
            State_ = ETransactionState::Aborted;
            return rspOrError;
        }

        auto cellId = ReplaceCellTagInId(CellId_, cellTag);

        State_ = ETransactionState::Active;
        YCHECK(RegisteredParticipantIds_.insert(cellId).second);
        YCHECK(ConfirmedParticipantIds_.insert(cellId).second);

        const auto& rsp = rspOrError.Value();
        Id_ = FromProto<TTransactionId>(rsp->id());

        LOG_DEBUG("Master transaction started (TransactionId: %v, CellTag: %v, StartTimestamp: %llx, AutoAbort: %v, Ping: %v, PingAncestors: %v)",
            Id_,
            cellTag,
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
        YCHECK(Atomicity_ == EAtomicity::Full);
        YCHECK(Durability_ == EDurability::Sync);

        Id_ = options.Id
            ? options.Id
            : MakeTabletTransactionId(
                Atomicity_,
                CellTagFromId(Owner_->PrimaryCellId_),
                StartTimestamp_,
                TabletTransactionHashCounter++);

        State_ = ETransactionState::Active;

        LOG_DEBUG("Atomic tablet transaction started (TransactionId: %v, StartTimestamp: %llx, AutoAbort: %v)",
            Id_,
            StartTimestamp_,
            AutoAbort_);

        // Start ping scheduling.
        // Participants will be added into it upon arrival.
        YCHECK(Ping_);
        RunPeriodicPings();

        return VoidFuture;
    }

    TFuture<void> StartNonAtomicTabletTransaction()
    {
        YCHECK(Atomicity_ == EAtomicity::None);

        StartTimestamp_ = InstantToTimestamp(TInstant::Now()).first;

        Id_ = MakeTabletTransactionId(
            Atomicity_,
            CellTag_,
            StartTimestamp_,
            TabletTransactionHashCounter++);

        State_ = ETransactionState::Active;

        LOG_DEBUG("Non-atomic tablet transaction started (TransactionId: %v, Durability: %v)",
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

        LOG_DEBUG("Transaction committed (TransactionId: %v, CommitTimestamps: %v)",
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
            YCHECK(CoordinatorCellId_);

            LOG_DEBUG("Committing transaction (TransactionId: %v, CoordinatorCellId: %v)",
                Id_,
                CoordinatorCellId_);

            auto coordinatorChannel = Owner_->CellDirectory_->GetChannelOrThrow(CoordinatorCellId_);
            auto proxy = Owner_->MakeSupervisorProxy(std::move(coordinatorChannel), true);
            auto req = proxy.CommitTransaction();
            req->SetUser(Owner_->User_);

            ToProto(req->mutable_transaction_id(), Id_);
            auto participantIds = GetRegisteredParticipantIds();
            for (const auto& cellId : participantIds) {
                if (cellId != CoordinatorCellId_) {
                    ToProto(req->add_participant_cell_ids(), cellId);
                }
            }
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

    TCellId PickCoordinator(const TTransactionCommitOptions& options)
    {
        if (Type_ == ETransactionType::Master) {
            return CellId_;
        }

        if (options.CoordinatorCellId) {
            if (!IsParticipantRegistered(options.CoordinatorCellId)) {
                THROW_ERROR_EXCEPTION("Cell %v is not a participant",
                    options.CoordinatorCellId);
            }
            return options.CoordinatorCellId;
        }

        std::vector<TCellId> feasibleParticipantIds;
        for (const auto& cellId : GetRegisteredParticipantIds()) {
            if (options.CoordinatorCellTag == InvalidCellTag ||
                CellTagFromId(cellId) == options.CoordinatorCellTag)
            {
                feasibleParticipantIds.push_back(cellId);
            }
        }

        if (feasibleParticipantIds.empty()) {
            THROW_ERROR_EXCEPTION("No participant matches the coordinator criteria");
        }

        return feasibleParticipantIds[RandomNumber(feasibleParticipantIds.size())];
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

        YCHECK(CoordinatorCellId_);
        auto coordinatorChannel = Owner_->CellDirectory_->GetChannelOrThrow(CoordinatorCellId_);
        auto proxy = Owner_->MakeSupervisorProxy(std::move(coordinatorChannel), true);
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
                        LOG_DEBUG("Some transaction participants are known to be down (CellIds: %v)",
                            downedParticipantIds);
                        return TError("Some transaction participants are known to be down")
                            << TErrorAttribute("downed_participants", downedParticipantIds);
                    }
                } else if (rspOrError.GetCode() == NYT::NRpc::EErrorCode::NoSuchMethod) {
                    // COMPAT(savrus)
                    LOG_DEBUG("Method GetDownedParticipants is not implemented (CellId: %v)",
                        CoordinatorCellId_);
                } else {
                    LOG_WARNING("Error updating downed participants (CellId: %v)",
                        CoordinatorCellId_);
                    return TError("Error updating downed participants at cell %v",
                        CoordinatorCellId_)
                        << rspOrError;
                }
                return TError();
            }));
    }


    TErrorOr<TTransactionCommitResult> OnAtomicTransactionCommitted(
        const TCellId& cellId,
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


    TFuture<void> SendPing(bool retry)
    {
        std::vector<TFuture<void>> asyncResults;
        auto participantIds = GetConfirmedParticipantIds();
        for (const auto& cellId : participantIds) {
            LOG_DEBUG("Pinging transaction (TransactionId: %v, CellId: %v)",
                Id_,
                cellId);

            auto channel = Owner_->CellDirectory_->FindChannel(cellId);
            if (!channel) {
                continue;
            }

            auto proxy = Owner_->MakeSupervisorProxy(std::move(channel), retry);
            auto req = proxy.PingTransaction();
            req->SetUser(Owner_->User_);
            ToProto(req->mutable_transaction_id(), Id_);
            if (cellId == CellId_) {
                req->set_ping_ancestors(PingAncestors_);
            }

            auto asyncRspOrError = req->Invoke();
            asyncResults.push_back(asyncRspOrError.Apply(
                BIND([=, this_ = MakeStrong(this)] (const TTransactionSupervisorServiceProxy::TErrorOrRspPingTransactionPtr& rspOrError) {
                    if (rspOrError.IsOK()) {
                        LOG_DEBUG("Transaction pinged (TransactionId: %v, CellId: %v)",
                            Id_,
                            cellId);
                        return TError();
                    } else if (rspOrError.GetCode() == NTransactionClient::EErrorCode::NoSuchTransaction) {
                        // Hard error.
                        LOG_DEBUG("Transaction has expired or was aborted (TransactionId: %v, CellId: %v)",
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
                        LOG_DEBUG(rspOrError, "Error pinging transaction (TransactionId: %v, CellId: %v)",
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
            LOG_DEBUG("Transaction is not in pingable state (TransactionId: %v, State: %v)",
                Id_,
                GetState());
            return;
        }

        SendPing(false).Subscribe(BIND([=, this_ = MakeStrong(this)] (const TError& error) {
            if (!IsPingableState()) {
                LOG_DEBUG("Transaction is not in pingable state (TransactionId: %v, State: %v)",
                    Id_,
                    GetState());
                return;
            }

            if (error.FindMatching(NYT::EErrorCode::Timeout)) {
                RunPeriodicPings();
                return;
            }

            LOG_DEBUG("Transaction ping scheduled (TransactionId: %v)",
                Id_);

            TDelayedExecutor::Submit(
                BIND(&TImpl::RunPeriodicPings, MakeWeak(this)),
                PingPeriod_.Get(Owner_->Config_->DefaultPingPeriod));
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
        std::vector<TFuture<void>> asyncResults;
        auto participantIds = GetRegisteredParticipantIds();
        for (const auto& cellId : participantIds) {
            LOG_DEBUG("Aborting transaction (TransactionId: %v, CellId: %v)",
                Id_,
                cellId);

            auto channel = Owner_->CellDirectory_->FindChannel(cellId);
            if (!channel) {
                continue;
            }

            auto proxy = Owner_->MakeSupervisorProxy(std::move(channel), true);
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
                        LOG_DEBUG("Transaction aborted (TransactionId: %v, CellId: %v)",
                            transactionId,
                            cellId);
                        return TError();
                    } else if (rspOrError.GetCode() == NTransactionClient::EErrorCode::NoSuchTransaction) {
                        LOG_DEBUG("Transaction has expired or was already aborted, ignored (TransactionId: %v, CellId: %v)",
                            transactionId,
                            cellId);
                        return TError();
                    } else {
                        LOG_WARNING(rspOrError, "Error aborting transaction (TransactionId: %v, CellId: %v)",
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

    std::vector<TCellId> GetConfirmedParticipantIds()
    {
        auto guard = Guard(SpinLock_);
        return std::vector<TCellId>(ConfirmedParticipantIds_.begin(), ConfirmedParticipantIds_.end());
    }

    bool IsParticipantRegistered(const TCellId& cellId)
    {
        auto guard = Guard(SpinLock_);
        return RegisteredParticipantIds_.find(cellId) != RegisteredParticipantIds_.end();
    }

};

////////////////////////////////////////////////////////////////////////////////

TTransactionManager::TImpl::TImpl(
    TTransactionManagerConfigPtr config,
    const TCellId& primaryCellId,
    IConnectionPtr connection,
    const TString& user,
    ITimestampProviderPtr timestampProvider,
    TCellDirectoryPtr cellDirectory)
    : Config_(config)
    , PrimaryCellId_(primaryCellId)
    , Connection_(connection)
    , User_(user)
    , TimestampProvider_(timestampProvider)
    , CellDirectory_(cellDirectory)
    , DownedCellTracker_(New<TCellTracker>())
{
    YCHECK(Config_);
    YCHECK(TimestampProvider_);
    YCHECK(CellDirectory_);
}

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
    const TTransactionId& id,
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
            auto transaction = TRefCounted::DangerousGetPtr(rawTransaction);
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

TFuture<void> TTransaction::Ping()
{
    return Impl_->Ping();
}

ETransactionType TTransaction::GetType() const
{
    return Impl_->GetType();
}

const TTransactionId& TTransaction::GetId() const
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

void TTransaction::RegisterParticipant(const TCellId& cellId)
{
    Impl_->RegisterParticipant(cellId);
}

void TTransaction::ConfirmParticipant(const TCellId& cellId)
{
    Impl_->ConfirmParticipant(cellId);
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
    TTransactionManagerConfigPtr config,
    const TCellId& primaryCellId,
    IConnectionPtr connection,
    const TString& user,
    ITimestampProviderPtr timestampProvider,
    TCellDirectoryPtr cellDirectory)
    : Impl_(New<TImpl>(
        std::move(config),
        primaryCellId,
        std::move(connection),
        user,
        std::move(timestampProvider),
        std::move(cellDirectory)))
{ }

TTransactionManager::~TTransactionManager() = default;

TFuture<TTransactionPtr> TTransactionManager::Start(
    ETransactionType type,
    const TTransactionStartOptions& options)
{
    return Impl_->Start(type, options);
}

TTransactionPtr TTransactionManager::Attach(
    const TTransactionId& id,
    const TTransactionAttachOptions& options)
{
    return Impl_->Attach(id, options);
}

void TTransactionManager::AbortAll()
{
    Impl_->AbortAll();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionClient
} // namespace NYT
