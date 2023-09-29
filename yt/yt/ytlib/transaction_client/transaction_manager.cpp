#include "transaction_manager.h"
#include "private.h"
#include "config.h"
#include "action.h"
#include "helpers.h"

#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/cell_master_client/cell_directory_synchronizer.h>
#include <yt/yt/ytlib/cell_master_client/cell_directory.h>

#include <yt/yt/ytlib/cypress_transaction_client/cypress_transaction_service_proxy.h>

#include <yt/yt/ytlib/hive/cell_directory.h>
#include <yt/yt/ytlib/hive/cell_tracker.h>

#include <yt/yt/ytlib/tablet_client/tablet_service_proxy.h>

#include <yt/yt/ytlib/transaction_client/clock_manager.h>
#include <yt/yt/ytlib/transaction_client/transaction_service_proxy.h>

#include <yt/yt/ytlib/transaction_supervisor/transaction_participant_service_proxy.h>
#include <yt/yt/ytlib/transaction_supervisor/transaction_supervisor_service_proxy.h>

#include <yt/yt/client/hive/timestamp_map.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/transaction_client/helpers.h>
#include <yt/yt/client/transaction_client/timestamp_provider.h>

#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/rpc/helpers.h>
#include <yt/yt/core/rpc/retrying_channel.h>

#include <yt/yt/core/ytree/public.h>

#include <util/generic/algorithm.h>

#include <atomic>

namespace NYT::NTransactionClient {

using namespace NApi;
using namespace NCellMasterClient;
using namespace NConcurrency;
using namespace NCypressTransactionClient;
using namespace NHiveClient;
using namespace NHydra;
using namespace NObjectClient;
using namespace NRpc;
using namespace NTabletClient;
using namespace NTransactionSupervisor;
using namespace NYTree;

using NYT::ToProto;
using NYT::FromProto;

using NNative::IConnection;
using NNative::IConnectionPtr;

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
    const IClockManagerPtr ClockManager_;
    const NHiveClient::ICellDirectoryPtr CellDirectory_;
    const TCellTrackerPtr DownedCellTracker_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);
    THashSet<TTransaction::TImpl*> AliveTransactions_;


    static TRetryChecker GetCommitRetryChecker()
    {
        static const auto Result = BIND_NO_PROPAGATE(&IsRetriableError);
        return Result;
    }

    static TRetryChecker GetAbortRetryChecker()
    {
        static const auto Result = BIND_NO_PROPAGATE([] (const TError& error) {
            return
                IsRetriableError(error) ||
                error.FindMatching(NTransactionClient::EErrorCode::InvalidTransactionState);
        });
        return Result;
    }

    static TRetryChecker GetPingRetryChecker()
    {
        static const auto Result = BIND_NO_PROPAGATE(&IsRetriableError);
        return Result;
    }

    static TRetryChecker GetCheckDownedParticipantsRetryChecker()
    {
        static const auto Result = BIND_NO_PROPAGATE(&IsRetriableError);
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
        , Logger(TransactionClientLogger.WithTag("ConnectionCellTag: %v", Owner_->PrimaryCellTag_))
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
            auto optionsCoordinatorCellTagOrError = GetCoordinatorMasterCellTagFromOptions(options);
            if (!optionsCoordinatorCellTagOrError.IsOK()) {
                return MakeFuture(TError(optionsCoordinatorCellTagOrError));
            }

            if (auto optionsCoordinatorCellTag = optionsCoordinatorCellTagOrError.Value()) {
                CoordinatorMasterCellTag_ = *optionsCoordinatorCellTag;
            } else {
                // Need to wait for master cell dir sync.

                auto connectionOrError = TryLockConnection();
                if (!connectionOrError.IsOK()) {
                    return MakeFuture(TError(connectionOrError));
                }
                auto connection = connectionOrError.Value();

                auto syncFuture = connection->GetMasterCellDirectorySynchronizer()->RecentSync();

                if (syncFuture.IsSet()) {
                    if (!syncFuture.Get().IsOK()) {
                        return syncFuture;
                    }
                } else {
                    return syncFuture.Apply(
                        BIND(&TImpl::DoStart, MakeStrong(this), type, options));
                }
            }
        }

        return DoStart(type, options);
    }

    void Attach(
        TTransactionId id,
        const TTransactionAttachOptions& options)
    {
        ValidateAttachOptions(id);

        Type_ = ETransactionType::Master;
        CoordinatorMasterCellTag_ = CellTagFromId(id);
        CoordinatorMasterCellId_ = ReplaceCellTagInId(Owner_->PrimaryCellId_, CoordinatorMasterCellTag_);
        Id_ = id;
        AutoAbort_ = options.AutoAbort;
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
                    return MakeFuture<TTransactionCommitResult>(TError("Transaction %v is already committed",
                        Id_));

                case ETransactionState::Aborted:
                    return MakeFuture<TTransactionCommitResult>(TError("Transaction %v is already aborted",
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
            return VoidFuture;
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
            TypeFromId(cellId) == EObjectType::MasterCell ||
            TypeFromId(cellId) == EObjectType::ChaosCell);

        if (Atomicity_ != EAtomicity::Full) {
            return;
        }

        {
            auto guard = Guard(SpinLock_);

            if (State_ == ETransactionState::Aborted) {
                guard.Release();
                YT_UNUSED_FUTURE(SendAbortToCell(cellId));
                return;
            }

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


    void SubscribeCommitted(const ITransaction::TCommittedHandler& handler)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        Committed_.Subscribe(handler);
    }

    void UnsubscribeCommitted(const ITransaction::TCommittedHandler& handler)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        Committed_.Unsubscribe(handler);
    }


    void SubscribeAborted(const ITransaction::TAbortedHandler& handler)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        Aborted_.Subscribe(handler);
    }

    void UnsubscribeAborted(const ITransaction::TAbortedHandler& handler)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        Aborted_.Unsubscribe(handler);
    }

private:
    friend class TTransactionManager::TImpl;

    const TIntrusivePtr<TTransactionManager::TImpl> Owner_;
    const NLogging::TLogger Logger;

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
    TCellTag ClockClusterTag_ = InvalidCellTag;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);

    ETransactionState State_ = ETransactionState::Initializing;

    TSingleShotCallbackList<void()> Committed_;
    TSingleShotCallbackList<void(const TError& error)> Aborted_;

    THashSet<TCellId> RegisteredParticipantIds_;
    THashSet<TCellId> PrepareOnlyRegisteredParticipantIds_;

    TCellId CoordinatorCellId_;

    TError Error_;

    TTimestamp StartTimestamp_ = NullTimestamp;
    TTransactionId Id_;

    std::atomic<TInstant> PingLeaseDeadline_ = TInstant::Max();

    using TReqStartCypressTransaction = TTypedClientRequest<
        NCypressTransactionClient::NProto::TReqStartTransaction,
        TTypedClientResponse<NCypressTransactionClient::NProto::TRspStartTransaction>>;
    using TReqStartCypressTransactionPtr = TIntrusivePtr<TReqStartCypressTransaction>;

    using TReqStartMasterTransaction = TTypedClientRequest<
        NTransactionClient::NProto::TReqStartTransaction,
        TTypedClientResponse<NTransactionClient::NProto::TRspStartTransaction>>;
    using TReqStartMasterTransactionPtr = TIntrusivePtr<TReqStartMasterTransaction>;


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

    static TErrorOr<std::optional<TCellTag>> GetCoordinatorMasterCellTagFromOptions(const TTransactionStartOptions& options)
    {
        const auto parentId = options.ParentId;
        const auto coordinatorCellTag = options.CoordinatorMasterCellTag;
        const auto& prerequisiteTransactionIds = options.PrerequisiteTransactionIds;

        if (parentId && coordinatorCellTag != InvalidCellTag && CellTagFromId(parentId) != coordinatorCellTag) {
            return TError("Both parent transaction and coordinator master cell tag specified, and they refer to different cells");
        }

        if (prerequisiteTransactionIds.empty()) {
            if (parentId) {
                return std::make_optional(CellTagFromId(parentId));
            }
            if (coordinatorCellTag != InvalidCellTag) {
                return std::make_optional(coordinatorCellTag);
            }
            return std::optional<TCellTag>(std::nullopt);
        }

        std::vector<TCellTag> prerequisiteTransactionCellTags;
        std::transform(
            prerequisiteTransactionIds.begin(),
            prerequisiteTransactionIds.end(),
            std::back_inserter(prerequisiteTransactionCellTags),
            CellTagFromId);
        SortUnique(prerequisiteTransactionCellTags);

        if (prerequisiteTransactionCellTags.size() > 1) {
            return TError("Multiple prerequisite transactions from different cells specified");
        }

        const auto prerequisiteTransactionCellTag = prerequisiteTransactionCellTags.front();

        if (parentId && CellTagFromId(parentId) != prerequisiteTransactionCellTag) {
            return TError("Both parent and prerequisite transactions specified, and they refer to different cells");
        }

        if (coordinatorCellTag != InvalidCellTag && coordinatorCellTag != prerequisiteTransactionCellTag) {
            return TError("Both coordinator master cell tag and a prerequisite transaction specified, and they refer to different cells");
        }

        return std::make_optional(prerequisiteTransactionCellTag);
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

        GetCoordinatorMasterCellTagFromOptions(options)
            .ThrowOnError();
    }

    static void ValidateTabletStartOptions(const TTransactionStartOptions& options)
    {
        if (options.SuppressStartTimestampGeneration) {
            THROW_ERROR_EXCEPTION("Cannot suppress start timestamp generation for tablet transaction");
        }
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

    static void ValidateAttachOptions(TTransactionId id)
    {
        ValidateMasterTransactionId(id);
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
                YT_UNUSED_FUTURE(SendAbort());
            }
        }
    }

    TFuture<void> DoStart(
        ETransactionType type,
        const TTransactionStartOptions& options)
    {
        auto connectionOrError = TryLockConnection();
        if (!connectionOrError.IsOK()) {
            return MakeFuture(TError(connectionOrError));
        }
        auto connection = connectionOrError.Value();

        if (type == ETransactionType::Master) {
            if (CoordinatorMasterCellTag_ == InvalidCellTag) {
                CoordinatorMasterCellId_ = connection->GetMasterCellDirectory()->GetRandomMasterCellWithRoleOrThrow(EMasterCellRole::TransactionCoordinator);
                CoordinatorMasterCellTag_ = CellTagFromId(CoordinatorMasterCellId_);
            } else {
                CoordinatorMasterCellId_ = ReplaceCellTagInId(Owner_->PrimaryCellId_, CoordinatorMasterCellTag_);
            }
        }

        SetReplicatedToMasterCellTags(options.ReplicateToMasterCellTags.value_or(TCellTagList()));

        AutoAbort_ = options.AutoAbort;
        PingPeriod_ = options.PingPeriod;
        Ping_ = options.Ping;
        PingAncestors_ = options.PingAncestors;
        Timeout_ = options.Timeout;
        Atomicity_ = options.Atomicity;
        Durability_ = options.Durability;

        switch (Atomicity_) {
            case EAtomicity::Full:
                if (options.StartTimestamp != NullTimestamp || options.SuppressStartTimestampGeneration) {
                    return OnGotStartTimestamp(options, options.StartTimestamp);
                } else {
                    ClockClusterTag_ = Owner_->ClockManager_->GetCurrentClockTag();
                    YT_LOG_DEBUG("Generating transaction start timestamp (ClockClusterTag: %v)",
                        ClockClusterTag_);
                    return Owner_->ClockManager_->GetTimestampProviderOrThrow(ClockClusterTag_)->GenerateTimestamps()
                        .Apply(BIND(&TImpl::OnGotStartTimestamp, MakeStrong(this), options));
                }

            case EAtomicity::None:
                return StartNonAtomicTabletTransaction();

            default:
                YT_ABORT();
        }
    }

    TFuture<void> OnGotStartTimestamp(
        const TTransactionStartOptions& options,
        TTimestamp timestamp)
    {
        StartTimestamp_ = timestamp;

        Register();

        YT_LOG_DEBUG("Starting transaction (StartTimestamp: %v, Type: %v)",
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

    TErrorOr<IConnectionPtr> TryLockConnection()
    {
        if (auto connection = Owner_->Connection_.Lock(); connection) {
            return connection;
        } else {
            return TError("Unable to start master transaction: connection terminated");
        }
    }

    template<class TReqStartTransactionPtr>
    void FillStartTransactionReq(
        TReqStartTransactionPtr request,
        const TTransactionStartOptions& options)
    {
        request->Header().add_required_server_feature_ids(FeatureIdToInt(EMasterFeature::PortalExitSynchronization));

        request->SetUser(Owner_->User_);
        auto attributes = options.Attributes
            ? options.Attributes->Clone()
            : CreateEphemeralAttributes();
        auto optionalTitle = attributes->FindAndRemove<TString>("title");
        if (optionalTitle) {
            request->set_title(*optionalTitle);
        }
        ToProto(request->mutable_attributes(), *attributes);
        request->set_timeout(ToProto<i64>(GetTimeout()));
        if (options.ParentId) {
            ToProto(request->mutable_parent_id(), options.ParentId);
        }
        ToProto(request->mutable_prerequisite_transaction_ids(), options.PrerequisiteTransactionIds);
        if (options.Deadline) {
            request->set_deadline(ToProto<ui64>(*options.Deadline));
        }
        if (options.ReplicateToMasterCellTags) {
            for (auto tag : *options.ReplicateToMasterCellTags) {
                if (tag != CoordinatorMasterCellTag_) {
                    request->add_replicate_to_cell_tags(ToProto<int>(tag));
                }
            }
        }
        SetOrGenerateMutationId(request, options.MutationId, options.Retry);
    }

    TFuture<void> StartMasterTransaction(const TTransactionStartOptions& options)
    {
        auto connection = TryLockConnection()
            .ValueOrThrow();
        auto channel = connection->GetMasterChannelOrThrow(EMasterChannelKind::Leader, CoordinatorMasterCellTag_);

        if (options.StartCypressTransaction && Owner_->Config_->UseCypressTransactionService) {
            TCypressTransactionServiceProxy proxy(channel);
            auto req = proxy.StartTransaction();

            FillStartTransactionReq<TReqStartCypressTransactionPtr>(req, options);
            return req->Invoke().Apply(
                BIND(
                    &TImpl::OnMasterTransactionStarted<TCypressTransactionServiceProxy::TErrorOrRspStartTransactionPtr>,
                    MakeStrong(this)));
        }

        TTransactionServiceProxy proxy(channel);
        auto req = proxy.StartTransaction();

        FillStartTransactionReq<TReqStartMasterTransactionPtr>(req, options);
        req->set_is_cypress_transaction(options.StartCypressTransaction);
        return req->Invoke().Apply(
            BIND(
                &TImpl::OnMasterTransactionStarted<TTransactionServiceProxy::TErrorOrRspStartTransactionPtr>,
                MakeStrong(this)));
    }

    template<class TErrorOrRsp>
    TError OnMasterTransactionStarted(const TErrorOrRsp& rspOrError)
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
            "StartTimestamp: %v, AutoAbort: %v, Ping: %v, PingAncestors: %v)",
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
                GenerateTabletTransactionHashCounter());

        State_ = ETransactionState::Active;

        YT_LOG_DEBUG("Atomic tablet transaction started (TransactionId: %v, StartTimestamp: %v, AutoAbort: %v)",
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
            GenerateTabletTransactionHashCounter());

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

            auto isCypressTransaction = IsCypressTransactionType(TypeFromId(Id_));
            if (isCypressTransaction && Owner_->Config_->UseCypressTransactionService) {
                return DoCommitCypressTransaction(options);
            }

            return DoCommitTransaction(options);
        } catch (const std::exception& ex) {
            return MakeFuture<TTransactionCommitResult>(TError(ex));
        }
    }

    TFuture<TTransactionCommitResult> DoCommitCypressTransaction(const TTransactionCommitOptions& options)
    {
        YT_LOG_DEBUG("Committing cypress transaction (TransactionId: %v, CoordinatorCellId: %v)",
            Id_,
            CoordinatorCellId_);

        auto connection = TryLockConnection()
            .ValueOrThrow();
        auto channel = connection->GetMasterChannelOrThrow(EMasterChannelKind::Leader, CoordinatorMasterCellTag_);
        TCypressTransactionServiceProxy proxy(channel);
        auto req = proxy.CommitTransaction();

        req->SetUser(Owner_->User_);
        SetPrerequisites(req, options);
        ToProto(req->mutable_transaction_id(), Id_);
        SetOrGenerateMutationId(req, options.MutationId, options.Retry);

        return req->Invoke().Apply(
            BIND(
                &TImpl::OnAtomicTransactionCommitted<TCypressTransactionServiceProxy::TErrorOrRspCommitTransactionPtr>,
                MakeStrong(this),
                CoordinatorCellId_));
    }

    TFuture<TTransactionCommitResult> DoCommitTransaction(const TTransactionCommitOptions& options)
    {
        auto supervisorParticipantCellIds = GetSupervisorParticipantIds();
        auto supervisorPrepareOnlyParticipantCellIds = GetSupervisorPrepareOnlyParticipantIds();
        YT_LOG_DEBUG("Committing transaction (TransactionId: %v, CoordinatorCellId: %v, "
            "ParticipantCellIds: %v, PrepareOnlyParticipantCellIds: %v, CellIdsToSyncWithBeforePrepare: %v)",
            Id_,
            CoordinatorCellId_,
            supervisorParticipantCellIds,
            supervisorPrepareOnlyParticipantCellIds,
            options.CellIdsToSyncWithBeforePrepare);

        auto coordinatorChannel = Owner_->CellDirectory_->GetChannelByCellIdOrThrow(CoordinatorCellId_);
        auto proxy = Owner_->MakeSupervisorProxy(std::move(coordinatorChannel), Owner_->GetCommitRetryChecker());
        auto req = proxy.CommitTransaction();
        req->SetUser(Owner_->User_);
        // NB: the server side only supports these for simple (non-distributed) commits, but set them anyway.
        // COMPAT(h0pless): It should be safe to remove prerequisites here when CTxS will be used on masters.
        SetPrerequisites(req, options);
        ToProto(req->mutable_transaction_id(), Id_);
        ToProto(req->mutable_participant_cell_ids(), supervisorParticipantCellIds);
        ToProto(req->mutable_prepare_only_participant_cell_ids(), supervisorPrepareOnlyParticipantCellIds);
        ToProto(req->mutable_cell_ids_to_sync_with_before_prepare(), options.CellIdsToSyncWithBeforePrepare);
        req->set_force_2pc(options.Force2PC);
        req->set_generate_prepare_timestamp(options.GeneratePrepareTimestamp);
        req->set_inherit_commit_timestamp(options.InheritCommitTimestamp);
        req->set_coordinator_prepare_mode(ToProto<int>(options.CoordinatorPrepareMode));
        req->set_coordinator_commit_mode(ToProto<int>(options.CoordinatorCommitMode));
        req->set_max_allowed_commit_timestamp(options.MaxAllowedCommitTimestamp);
        req->set_clock_cluster_tag(ToProto<int>(ClockClusterTag_));
        SetOrGenerateMutationId(req, options.MutationId, options.Retry);

        return req->Invoke().Apply(
            BIND(
                &TImpl::OnAtomicTransactionCommitted<TTransactionSupervisorServiceProxy::TErrorOrRspCommitTransactionPtr>,
                MakeStrong(this),
                CoordinatorCellId_));
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
            THROW_ERROR_EXCEPTION(NYT::EErrorCode::Canceled, "Connection destroyed");

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
        YT_UNUSED_FUTURE(CheckDownedParticipants(participantIds));
    }


    TFuture<void> CheckDownedParticipants(std::vector<TCellId> participantIds)
    {
        if (participantIds.empty()) {
            return VoidFuture;
        }

        YT_VERIFY(CoordinatorCellId_);
        auto coordinatorChannel = Owner_->CellDirectory_->GetChannelByCellIdOrThrow(CoordinatorCellId_);
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

    template<class TErrorOrRsp>
    TErrorOr<TTransactionCommitResult> OnAtomicTransactionCommitted(
        TCellId cellId,
        const TErrorOrRsp& rspOrError)
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
        if (auto primaryTimestamp = result.CommitTimestamps.FindTimestamp(Owner_->PrimaryCellTag_)) {
            result.PrimaryCommitTimestamp = *primaryTimestamp;
        }
        auto error = SetCommitted(result);
        if (!error.IsOK()) {
            return error;
        }
        return result;
    }

    TFuture<void> SendPing(const TTransactionPingOptions& options = {})
    {
        auto isCypressTransaction = IsCypressTransactionType(TypeFromId(Id_));
        if (isCypressTransaction && Owner_->Config_->UseCypressTransactionService) {
            auto participantIds = GetRegisteredParticipantIds();
            YT_VERIFY(participantIds.size() == 1);

            return DoPingCypressTransaction(participantIds[0], options);
        }

        return DoPingTransaction(options);
    }

    TFuture<void> DoPingCypressTransaction(TCellId cellId, const TTransactionPingOptions& options)
    {
        YT_LOG_DEBUG("Pinging cypress transaction (TransactionId: %v, MasterCellId: %v)",
            Id_,
            cellId);

        auto connection = TryLockConnection()
            .ValueOrThrow();
        auto channel = connection->GetMasterChannelOrThrow(EMasterChannelKind::Leader, CoordinatorMasterCellTag_);

        if (options.EnableRetries) {
            channel = CreateRetryingChannel(Owner_->Config_, std::move(channel), Owner_->GetPingRetryChecker());
        }

        TCypressTransactionServiceProxy proxy(channel);
        proxy.SetDefaultTimeout(Owner_->Config_->RpcTimeout);

        auto req = proxy.PingTransaction();

        req->SetUser(Owner_->User_);
        ToProto(req->mutable_transaction_id(), Id_);
        if (cellId == CoordinatorMasterCellId_) {
            req->set_ping_ancestors(PingAncestors_);
        }

        return req->Invoke().Apply(
            BIND(
                &TImpl::OnTransactionPinged<TCypressTransactionServiceProxy::TErrorOrRspPingTransactionPtr>,
                MakeStrong(this),
                cellId));
    }

    TFuture<void> DoPingTransaction(const TTransactionPingOptions& options = {})
    {
        std::vector<TFuture<void>> asyncResults;
        for (auto cellId : GetRegisteredParticipantIds()) {
            if (IsReplicatedToMasterCell(cellId)) {
                continue;
            }

            YT_LOG_DEBUG("Pinging transaction (TransactionId: %v, CellId: %v)",
                Id_,
                cellId);

            auto channel = Owner_->CellDirectory_->FindChannelByCellId(cellId);
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
                BIND(
                    &TImpl::OnTransactionPinged<TTransactionSupervisorServiceProxy::TErrorOrRspPingTransactionPtr>,
                    MakeStrong(this),
                    cellId)));
        }

        return AllSucceeded(asyncResults);
    }

    template<class TErrorOrRsp>
    TError OnTransactionPinged(
        TCellId cellId,
        const TErrorOrRsp& rspOrError)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        if (rspOrError.IsOK()) {
            YT_LOG_DEBUG("Transaction pinged (TransactionId: %v, CellId: %v)",
                Id_,
                cellId);
            PingLeaseDeadline_.store(TInstant::Now() + GetTimeout());
            return TError();
        } else if (rspOrError.GetCode() == NTransactionClient::EErrorCode::NoSuchTransaction &&
                    TypeFromId(cellId) == EObjectType::MasterCell)
        {
            // Hard error.
            // NB: For tablet cells, transactions are started asynchronously so NoSuchTransaction
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
    }

    void RunPeriodicPings()
    {
        if (!IsPingableState()) {
            YT_LOG_DEBUG("Transaction is not in pingable state (TransactionId: %v, State: %v)",
                Id_,
                GetState());
            return;
        }

        if (IsPingLeaseExpired()) {
            YT_LOG_DEBUG("Transaction could not be pinged within timeout and is considered expired (TransactionId: %v)",
                Id_);
            SetAborted(TError("Transaction could not be pinged within timeout and is considered expired"));
            return;
        }

        SendPing().Subscribe(BIND([=, this, this_ = MakeStrong(this), startTime = TInstant::Now()] (const TError& /*error*/) {
            if (!IsPingableState()) {
                YT_LOG_DEBUG("Transaction is not in pingable state (TransactionId: %v, State: %v)",
                    Id_,
                    GetState());
                return;
            }

            auto pingPeriod = std::min(PingPeriod_.value_or(Owner_->Config_->DefaultPingPeriod), GetTimeout() / 2);
            auto pingDeadline = startTime + pingPeriod;
            TDelayedExecutor::Submit(BIND(&TImpl::RunPeriodicPings, MakeWeak(this)), pingDeadline);
        }));
    }

    bool IsPingableState()
    {
        auto state = GetState();
        // NB: We have to continue pinging the transaction while committing.
        return state == ETransactionState::Active || state == ETransactionState::Committing;
    }

    bool IsPingLeaseExpired()
    {
        return Type_ == ETransactionType::Tablet && PingLeaseDeadline_.load() < TInstant::Now();
    }


    TFuture<void> SendAbort(const TTransactionAbortOptions& options = {})
    {
        YT_LOG_DEBUG("Aborting transaction (TransactionId: %v)",
            Id_);

        std::vector<TFuture<void>> asyncResults;
        auto participantIds = GetRegisteredParticipantIds();
        for (auto cellId : participantIds) {
            asyncResults.push_back(SendAbortToCell(cellId, options));
        }

        return AllSucceeded(asyncResults);
    }

    TFuture<void> SendAbortToCell(TCellId cellId, const TTransactionAbortOptions& options = {})
    {
        auto isCypressTransaction = IsCypressTransactionType(TypeFromId(Id_));
        if (isCypressTransaction && Owner_->Config_->UseCypressTransactionService) {
            return DoAbortCypressTransaction(cellId, options);
        }

        return DoAbortTransaction(cellId, options);
    }

    TFuture<void> DoAbortCypressTransaction(TCellId cellId, const TTransactionAbortOptions& options)
    {
        YT_LOG_DEBUG("Aborting cypress transaction (TransactionId: %v, MasterCellId: %v)",
            Id_,
            cellId);

        auto connection = TryLockConnection()
            .ValueOrThrow();
        auto channel = connection->GetMasterChannelOrThrow(EMasterChannelKind::Leader, CoordinatorMasterCellTag_);
        TCypressTransactionServiceProxy proxy(channel);
        auto req = proxy.AbortTransaction();

        req->SetResponseHeavy(true);
        req->SetUser(Owner_->User_);
        ToProto(req->mutable_transaction_id(), Id_);
        req->set_force(options.Force);
        SetMutationId(req, options.MutationId, options.Retry);

        return req->Invoke().Apply(
            // NB: "this" could be dying; can't capture it.
            BIND([=, transactionId = Id_, Logger = Logger] (const TCypressTransactionServiceProxy::TErrorOrRspAbortTransactionPtr& rspOrError) {
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
            }));
    }

    TFuture<void> DoAbortTransaction(TCellId cellId, const TTransactionAbortOptions& options)
    {
        auto channel = Owner_->CellDirectory_->FindChannelByCellId(cellId);
        if (!channel) {
            return VoidFuture;
        }

        YT_LOG_DEBUG("Sending abort to participant (TransactionId: %v, ParticipantCellId: %v)",
            Id_,
            cellId);

        auto proxy = Owner_->MakeSupervisorProxy(std::move(channel), Owner_->GetAbortRetryChecker());
        auto req = proxy.AbortTransaction();
        req->SetResponseHeavy(true);
        req->SetUser(Owner_->User_);
        ToProto(req->mutable_transaction_id(), Id_);
        req->set_force(options.Force);
        SetMutationId(req, options.MutationId, options.Retry);

        return req->Invoke().Apply(
            // NB: "this" could be dying; can't capture it.
            BIND([=, transactionId = Id_, Logger = Logger] (const TTransactionSupervisorServiceProxy::TErrorOrRspAbortTransactionPtr& rspOrError) {
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
            }));
    }

    void FireAborted(const TError& error)
    {
        Aborted_.Fire(error);
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

        FireAborted(error);
    }

    void OnFailure(const TError& error)
    {
        SetAborted(error);
        // Best-effort, fire-and-forget.
        YT_UNUSED_FUTURE(SendAbort());
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

    void SetReplicatedToMasterCellTags(const TCellTagList& replicatedToCellTags)
    {
        ReplicatedToMasterCellTags_ = replicatedToCellTags;
        ReplicatedToMasterCellTags_.erase(
            std::remove(
                ReplicatedToMasterCellTags_.begin(),
                ReplicatedToMasterCellTags_.end(),
                CoordinatorMasterCellTag_),
            ReplicatedToMasterCellTags_.end());
    }

    bool IsReplicatedToMasterCell(TCellId cellId)
    {
        return
            TypeFromId(cellId) == EObjectType::MasterCell &&
            std::find(ReplicatedToMasterCellTags_.begin(), ReplicatedToMasterCellTags_.end(), CellTagFromId(cellId)) != ReplicatedToMasterCellTags_.end();
    }


    static int GenerateTabletTransactionHashCounter()
    {
        static std::atomic<ui32> Counter = RandomNumber<ui32>();
        return Counter++;
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
    , ClockManager_(connection->GetClockManager())
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
        YT_UNUSED_FUTURE(transaction->Abort());
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

DELEGATE_SIGNAL(TTransaction, ITransaction::TCommittedHandlerSignature, Committed, *Impl_);
DELEGATE_SIGNAL(TTransaction, ITransaction::TAbortedHandlerSignature, Aborted, *Impl_);

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
