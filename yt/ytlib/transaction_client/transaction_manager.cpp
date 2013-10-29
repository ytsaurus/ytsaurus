#include "stdafx.h"
#include "transaction_manager.h"
#include "transaction.h"
#include "config.h"
#include "private.h"

#include <core/concurrency/thread_affinity.h>
#include <core/concurrency/delayed_executor.h>
#include <core/concurrency/parallel_awaiter.h>

#include <core/rpc/helpers.h>

#include <core/ytree/attributes.h>

#include <ytlib/transaction_client/transaction_ypath.pb.h>

#include <ytlib/hydra/rpc_helpers.h>

#include <ytlib/hive/cell_directory.h>
#include <ytlib/hive/timestamp_provider.h>
#include <ytlib/hive/transaction_supervisor_service_proxy.h>

#include <ytlib/tablet_client/tablet_service.pb.h>

#include <atomic>

namespace NYT {
namespace NTransactionClient {

using namespace NYTree;
using namespace NHydra;
using namespace NHive;
using namespace NRpc;
using namespace NConcurrency;
using namespace NObjectClient;
using namespace NTabletClient;

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = TransactionClientLogger;
static std::atomic<ui32> TabletTransactionCounter; // used as a part of transaction id

////////////////////////////////////////////////////////////////////////////////

TTransactionStartOptions::TTransactionStartOptions()
    : AutoAbort(true)
    , Ping(true)
    , PingAncestors(false)
    , EnableUncommittedAccounting(true)
    , EnableStagedAccounting(true)
    , Attributes(CreateEphemeralAttributes())
    , Type(ETransactionType::Master)
{ }

////////////////////////////////////////////////////////////////////////////////

TTransactionAttachOptions::TTransactionAttachOptions(const TTransactionId& id)
    : Id(id)
    , AutoAbort(true)
    , Ping(true)
    , PingAncestors(false)
{ }

////////////////////////////////////////////////////////////////////////////////

class TTransactionManager::TImpl
    : public TRefCounted
{
public:
    TImpl(
        TTransactionManagerConfigPtr config,
        const TCellGuid& masterCellGuid,
        IChannelPtr channel,
        ITimestampProviderPtr timestampProvider,
        TCellDirectoryPtr cellDirectory);

    TFuture<TErrorOr<ITransactionPtr>> AsyncStart(const TTransactionStartOptions& options);

    ITransactionPtr Start(const TTransactionStartOptions& options);

    ITransactionPtr Attach(const TTransactionAttachOptions& options);

    void AsyncAbortAll();

private:
    friend class TTransaction;

    TTransactionManagerConfigPtr Config_;
    IChannelPtr MasterChannel_;
    TCellGuid MasterCellGuid_;
    ITimestampProviderPtr TimestampProvider_;
    TCellDirectoryPtr CellDirectory_;

    TSpinLock SpinLock_;
    yhash_set<TTransaction*> AliveTransactions_;

};

////////////////////////////////////////////////////////////////////////////////

class TTransactionManager::TTransaction
    : public ITransaction
{
public:
    explicit TTransaction(TImplPtr owner)
        : Owner_(owner)
        , AutoAbort_(false)
        , Ping_(false)
        , PingAncestors_(false)
        , MasterProxy_(owner->MasterChannel_)
        , State_(EState::Active)
        , Aborted_(NewPromise())
        , StartTimestamp_(NullTimestamp)
    { }

    ~TTransaction()
    {
        if (AutoAbort_) {
            if (State_ == EState::Active) {
                SendAbort();
            }

            {
                TGuard<TSpinLock> guard(Owner_->SpinLock_);
                YCHECK(Owner_->AliveTransactions_.erase(this) == 1);
            }
        }
    }


    TAsyncError Start(const TTransactionStartOptions& options)
    {
        try {
            ValidateStartOptions(options);
        } catch (const std::exception& ex) {
            return MakeFuture(TError(ex));
        }

        Type_ = options.Type;
        AutoAbort_ = options.AutoAbort;
        Ping_ = options.Ping;
        PingAncestors_ = options.PingAncestors;
        Timeout_ = options.Timeout;

        return Owner_->TimestampProvider_->GenerateNewTimestamp()
            .Apply(BIND(&TTransaction::OnGotStartTimestamp, MakeStrong(this), options));
    }

    void Attach(const TTransactionAttachOptions& options)
    {
        Type_ = ETransactionType::Master;
        Id_ = options.Id;
        AutoAbort_ = options.AutoAbort;
        Ping_ = options.Ping;
        PingAncestors_ = options.PingAncestors;
        Register();

        LOG_INFO("Master transaction attached (TransactionId: %s, AutoAbort: %s, Ping: %s, PingAncestors: %s)",
            ~ToString(Id_),
            ~FormatBool(AutoAbort_),
            ~FormatBool(Ping_),
            ~FormatBool(PingAncestors_));

        if (Ping_) {
            SendPing();
        }
    }

    virtual TAsyncError AsyncCommit(const TMutationId& mutationId) override
    {
        VERIFY_THREAD_AFFINITY(ClientThread);

        {
            TGuard<TSpinLock> guard(SpinLock_);
            if (!Error_.IsOK()) {
                return MakeFuture(Error_);
            }
            switch (State_) {
                case EState::Committing:
                    return MakeFuture(TError("Transaction is already being committed"));
                    break;

                case EState::Committed:
                    return MakeFuture(TError("Transaction is already committed"));
                    break;

                case EState::Aborted:
                    return MakeFuture(TError("Transaction is already aborted"));
                    break;

                case EState::Active:
                    State_ = EState::Committing;
                    break;

                default:
                    YUNREACHABLE();
            }
        }

        auto cellGuid = GetCoordinatorCellGuid();
        if (!cellGuid) {
            {
                TGuard<TSpinLock> guard(SpinLock_);
                if (State_ != EState::Committing)
                    return MakeFuture(Error_);
                State_ = EState::Committed;
            }

            LOG_INFO("Trivial transaction committed (TransactionId: %s)",
                ~ToString(Id_));
            return MakeFuture(TError());
        }

        LOG_INFO("Committing transaction (TransactionId: %s, CoordinatorCellGuid: %s)",
            ~ToString(Id_),
            ~ToString(*cellGuid));

        auto channel = Owner_->CellDirectory_->GetChannel(*cellGuid);
        if (!channel) {
            return MakeFuture(TError("Unknown coordinator cell %s",
                ~ToString(*cellGuid)));
        }

        TTransactionSupervisorServiceProxy proxy(channel);
        auto req = proxy.CommitTransaction();
        ToProto(req->mutable_transaction_id(), Id_);
        SetOrGenerateMutationId(req, mutationId);

        return req->Invoke().Apply(
            BIND(&TTransaction::OnTransactionCommitted, MakeStrong(this), *cellGuid));
    }

    virtual TAsyncError AsyncAbort(const TMutationId& mutationId) override
    {
        return SendAbort(mutationId);
    }

    virtual TAsyncError AsyncPing() override
    {
        return SendPing();
    }

    virtual void Detach() override
    {
        VERIFY_THREAD_AFFINITY(ClientThread);

        {
            TGuard<TSpinLock> guard(SpinLock_);
            switch (State_) {
                case EState::Committed:
                    THROW_ERROR_EXCEPTION("Transaction is already committed");
                    break;

                case EState::Aborted:
                    THROW_ERROR_EXCEPTION("Transaction is already aborted");
                    break;

                case EState::Active:
                    State_ = EState::Detached;
                    break;

                case EState::Detached:
                    return;

                default:
                    YUNREACHABLE();
            }
        }

        LOG_INFO("Transaction detached (TransactionId: %s)",
            ~ToString(Id_));
    }


    virtual TTransactionId GetId() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();
        return Id_;
    }

    virtual TTimestamp GetStartTimestamp() const override
    {
        return StartTimestamp_;
    }

    
    virtual void AddParticipant(const NElection::TCellGuid& cellGuid) override
    {
        YCHECK(TypeFromId(cellGuid) == EObjectType::TabletCell);

        {
            TGuard<TSpinLock> guard(SpinLock_);
            if (State_ != EState::Active)
                return;
            if (!Error_.IsOK())
                THROW_ERROR Error_;
            if (!ParticipantGuids_.insert(cellGuid).second)
                return;
        }

        auto channel = Owner_->CellDirectory_->GetChannel(cellGuid);
        if (!channel) {
            THROW_ERROR_EXCEPTION("Unknown participant cell %s",
                ~ToString(cellGuid));
        }

        LOG_DEBUG("Adding transaction participant (TransactionId: %s, CellGuid: %s)",
            ~ToString(Id_),
            ~ToString(cellGuid));

        TTransactionSupervisorServiceProxy proxy(channel);
        auto req = proxy.StartTransaction();
        req->set_start_timestamp(StartTimestamp_);

        auto* reqExt = req->MutableExtension(NTabletClient::NProto::TReqStartTransactionExt::start_transaction_ext);
        reqExt->set_start_timestamp(StartTimestamp_);
        ToProto(reqExt->mutable_transaction_id(), Id_);
        if (Timeout_) {
            reqExt->set_timeout(Timeout_->MilliSeconds());
        }

        req->Invoke().Subscribe(
            BIND(&TTransaction::OnParticipantAdded, MakeStrong(this), cellGuid));
    }


    virtual void SubscribeAborted(const TCallback<void()>& handler) override
    {
        VERIFY_THREAD_AFFINITY_ANY();
        Aborted_.Subscribe(handler);
    }

    virtual void UnsubscribeAborted(const TCallback<void()>& handler) override
    {
        VERIFY_THREAD_AFFINITY_ANY();
        YUNREACHABLE();
    }

private:
    DECLARE_ENUM(EState,
        (Active)
        (Aborted)
        (Committing)
        (Committed)
        (Detached)
   );

    TImplPtr Owner_;
    ETransactionType Type_;
    bool AutoAbort_;
    bool Ping_;
    bool PingAncestors_;
    TNullable<TDuration> Timeout_;

    TTransactionSupervisorServiceProxy MasterProxy_;

    //! Protects state transitions.
    TSpinLock SpinLock_;
    EState State_;
    TPromise<void> Aborted_;
    yhash_set<TCellGuid> ParticipantGuids_;
    TError Error_;

    TTimestamp StartTimestamp_;
    TTransactionId Id_;

    DECLARE_THREAD_AFFINITY_SLOT(ClientThread);

    

    static void ValidateStartOptions(const TTransactionStartOptions& options)
    {
        switch (options.Type)
        {
            case ETransactionType::Master:
                ValidateMasterStartOptions(options);
                break;
            case ETransactionType::Tablet:
                ValidateTabletStartOptions(options);
                break;
            default:
                YUNREACHABLE();
        }
    }

    static void ValidateMasterStartOptions(const TTransactionStartOptions& options)
    {
        // Everything is valid.
    }

    static void ValidateTabletStartOptions(const TTransactionStartOptions& options)
    {
        if (options.ParentId != NullTransactionId) {
            THROW_ERROR_EXCEPTION("Tablet transaction cannot have a parent");
        }
        if (!options.Ping) {
            THROW_ERROR_EXCEPTION("Cannot switch off pings for a tablet transaction");
        }
        if (options.PingAncestors) {
            THROW_ERROR_EXCEPTION("Cannot ping ancestors for a tablet transaction");
        }
    }


    void Register()
    {
        if (AutoAbort_) {
            TGuard<TSpinLock> guard(Owner_->SpinLock_);
            YCHECK(Owner_->AliveTransactions_.insert(this).second);
        }        
    }

    void SchedulePing()
    {
        TDelayedExecutor::Submit(
            BIND(IgnoreResult(&TTransaction::SendPing), MakeWeak(this)),
            Owner_->Config_->PingPeriod);
    }


    TAsyncError OnGotStartTimestamp(
        const TTransactionStartOptions& options,
        TErrorOr<TTimestamp> timestampOrError)
    {
        if (!timestampOrError.IsOK()) {
            return MakeFuture(TError(timestampOrError));
        }
        StartTimestamp_ = timestampOrError.GetValue();

        Register();

        LOG_INFO("Starting transaction (StartTimestamp: %" PRId64 ", Type: %s)",
            StartTimestamp_,
            ~Type_.ToString());

        switch (options.Type) {
            case ETransactionType::Master:
                return StartMasterTransaction(options);
            case ETransactionType::Tablet:
                return StartTabletTransaction(options);
            default:
                YUNREACHABLE();
        }
    }

    TAsyncError StartMasterTransaction(const TTransactionStartOptions& options)
    {
        auto req = MasterProxy_.StartTransaction();
        req->set_start_timestamp(StartTimestamp_);

        auto* reqExt = req->MutableExtension(NTransactionClient::NProto::TReqStartTransactionExt::start_transaction_ext);
        if (!options.Attributes->List().empty()) {
            ToProto(reqExt->mutable_attributes(), *options.Attributes);
        }
        if (options.ParentId != NullTransactionId) {
            ToProto(reqExt->mutable_parent_transaction_id(), options.ParentId);
        }
        reqExt->set_enable_uncommitted_accounting(options.EnableUncommittedAccounting);
        reqExt->set_enable_staged_accounting(options.EnableStagedAccounting);
        if (options.Timeout) {
            reqExt->set_timeout(options.Timeout->MilliSeconds());
        }

        if (options.ParentId != NullTransactionId) {
            SetOrGenerateMutationId(req, options.MutationId);
        }

        return req->Invoke().Apply(
            BIND(&TTransaction::OnTransactionStarted, MakeStrong(this)));
    }

    TError OnTransactionStarted(TTransactionSupervisorServiceProxy::TRspStartTransactionPtr rsp)
    {
        if (!rsp->IsOK()) {
            State_ = EState::Aborted;
            return rsp->GetError();
        }

        State_ = EState::Active;
        Id_ = FromProto<TTransactionId>(rsp->transaction_id());
        YCHECK(ParticipantGuids_.insert(Owner_->MasterCellGuid_).second);

        LOG_INFO("Master transaction started (TransactionId: %s, StartTimestamp: %" PRId64 ", AutoAbort: %s, Ping: %s, PingAncestors: %s)",
            ~ToString(Id_),
            StartTimestamp_,
            ~FormatBool(AutoAbort_),
            ~FormatBool(Ping_),
            ~FormatBool(PingAncestors_));

        if (Ping_) {
            SendPing();
        }

        return TError();
    }

    TAsyncError StartTabletTransaction(const TTransactionStartOptions& options)
    {
        Id_ = MakeId(
            EObjectType::TabletTransaction,
            0, // TODO(babenko): cell id?
            static_cast<ui64>(StartTimestamp_),
            TabletTransactionCounter++);

        LOG_INFO("Tablet transaction started (TransactionId: %s, StartTimestamp: %" PRId64 ", AutoAbort: %s)",
            ~ToString(Id_),
            StartTimestamp_,
            ~FormatBool(AutoAbort_));

        // Start ping scheduling.
        // Participants will be added into it upon arrival.
        YCHECK(Ping_);
        SendPing();

        return MakeFuture(TError());
    }


    TError OnTransactionCommitted(const TCellGuid& cellGuid, TTransactionSupervisorServiceProxy::TRspCommitTransactionPtr rsp)
    {
        if (!rsp->IsOK()) {
            auto error = TError("Error committing transaction at cell %s",
                ~ToString(cellGuid))
                << *rsp;
            DoAbort(error);
            return error;
        }

        {
            TGuard<TSpinLock> guard(SpinLock_);
            if (State_ != EState::Committing)
                return Error_;
            State_ = EState::Committed;
        }

        LOG_INFO("Transaction committed (TransactionId: %s)",
            ~ToString(Id_));
        return TError();
    }


    void OnParticipantAdded(const TCellGuid& cellGuid, TTransactionSupervisorServiceProxy::TRspStartTransactionPtr rsp)
    {
        if (rsp->IsOK()) {
            LOG_DEBUG("Transaction participant added (TransactionId: %s, CellGuid: %s)",
                ~ToString(Id_),
                ~ToString(cellGuid));
        } else {
            LOG_DEBUG(*rsp, "Error adding transaction participant (TransactionId: %s, CellGuid: %s)",
                ~ToString(Id_),
                ~ToString(cellGuid));

            DoAbort(TError("Error adding participant cell %s",
                ~ToString(cellGuid))
                << *rsp);
        }
    }


    class TPingSession
        : public TRefCounted
    {
    public:
        explicit TPingSession(TTransactionPtr transaction)
            : Transaction_(transaction)
            , Promise_(NewPromise<TError>())
            , Awaiter_(New<TParallelAwaiter>(GetSyncInvoker()))
        { }

        TAsyncError Run()
        {
            auto participantGuids = Transaction_->GetParticipantGuids();
            for (const auto& cellGuid : participantGuids) {
                LOG_DEBUG("Pinging transaction (TransactionId: %s, CellGuid: %s)",
                    ~ToString(Transaction_->Id_),
                    ~ToString(cellGuid));

                auto channel = Transaction_->Owner_->CellDirectory_->GetChannel(cellGuid);
                if (!channel) {
                    auto error = TError("Unknown participant cell %s",
                        ~ToString(cellGuid));
                    OnError(error);
                    return MakeFuture(error);
                }

                TTransactionSupervisorServiceProxy proxy(channel);
                auto req = proxy.PingTransaction();
                ToProto(req->mutable_transaction_id(), Transaction_->Id_);

                if (cellGuid == Transaction_->Owner_->MasterCellGuid_) {
                    auto* reqExt = req->MutableExtension(NProto::TReqPingTransactionExt::ping_transaction_ext);
                    reqExt->set_ping_ancestors(Transaction_->PingAncestors_);
                }

                Awaiter_->Await(
                    req->Invoke(),
                    BIND(&TPingSession::OnResponse, MakeStrong(this), cellGuid));
            }

            Awaiter_->Complete(
                BIND(&TPingSession::OnComplete, MakeStrong(this)));

            return Promise_;
        }

    private:
        TTransactionPtr Transaction_;
        TAsyncErrorPromise Promise_;
        TParallelAwaiterPtr Awaiter_;


        void OnResponse(const TCellGuid& cellGuid, TTransactionSupervisorServiceProxy::TRspPingTransactionPtr rsp)
        {
            if (rsp->IsOK()) {
                LOG_DEBUG("Transaction pinged (TransactionId: %s, CellGuid: %s)",
                    ~ToString(Transaction_->Id_),
                    ~ToString(cellGuid));

            } else {
                if (rsp->GetError().GetCode() == NYTree::EErrorCode::ResolveError) {
                    // Hard error.
                    LOG_WARNING("Transaction has expired or was aborted (TransactionId: %s, CellGuid: %s)",
                        ~ToString(Transaction_->Id_),
                        ~ToString(cellGuid));
                    OnError(TError("Transaction has expired or was aborted at cell %s",
                        ~ToString(cellGuid)));
                } else {
                    // Soft error.
                    LOG_WARNING(*rsp, "Error pinging transaction (TransactionId: %s, CellGuid: %s)",
                        ~ToString(Transaction_->Id_),
                        ~ToString(cellGuid));
                }
            }
        }

        void OnError(const TError& error)
        {
            if (!Promise_.TrySet(error))
                return;

            Awaiter_->Cancel();
            Transaction_->DoAbort(error);
        }

        void OnComplete()
        {
            if (!Promise_.TrySet(TError()))
                return;
            
            if (Transaction_->Ping_) {
                Transaction_->SchedulePing();
            }
        }
    };

    TAsyncError SendPing()
    {
        return New<TPingSession>(this)->Run();
    }


    class TAbortSession
        : public TRefCounted
    {
    public:
        explicit TAbortSession(TTransactionPtr transaction, const TMutationId& mutationId)
            : Transaction_(transaction)
            , MutationId_(mutationId)
            , Promise_(NewPromise<TError>())
            , Awaiter_(New<TParallelAwaiter>(GetSyncInvoker()))
        { }

        TAsyncError Run()
        {
            auto participantGuids = Transaction_->GetParticipantGuids();
            for (const auto& cellGuid : participantGuids) {
                LOG_DEBUG("Aborting transaction (TransactionId: %s, CellGuid: %s)",
                    ~ToString(Transaction_->Id_),
                    ~ToString(cellGuid));

                auto channel = Transaction_->Owner_->CellDirectory_->GetChannel(cellGuid);
                if (!channel)
                    continue; // better skip

                TTransactionSupervisorServiceProxy proxy(channel);
                auto req = proxy.AbortTransaction();
                ToProto(req->mutable_transaction_id(), Transaction_->Id_);

                if (MutationId_ != NullMutationId) {
                    SetMutationId(req, MutationId_);
                }

                Awaiter_->Await(
                    req->Invoke(),
                    BIND(&TAbortSession::OnResponse, MakeStrong(this), cellGuid));
            }

            Awaiter_->Complete(
                BIND(&TAbortSession::OnComplete, MakeStrong(this)));

            return Promise_;
        }

    private:
        TTransactionPtr Transaction_;
        TMutationId MutationId_;
        TAsyncErrorPromise Promise_;
        TParallelAwaiterPtr Awaiter_;


        void OnResponse(const TCellGuid& cellGuid, TTransactionSupervisorServiceProxy::TRspAbortTransactionPtr rsp)
        {
            if (rsp->IsOK()) {
                LOG_DEBUG("Transaction aborted (TransactionId: %s, CellGuid: %s)",
                    ~ToString(Transaction_->Id_),
                    ~ToString(cellGuid));

            } else {
                if (rsp->GetError().GetCode() == NYTree::EErrorCode::ResolveError) {
                    LOG_DEBUG("Transaction has expired or was already aborted, ignored (TransactionId: %s, CellGuid: %s)",
                        ~ToString(Transaction_->Id_),
                        ~ToString(cellGuid));
                } else {
                    LOG_WARNING(*rsp, "Error aborting transaction (TransactionId: %s, CellGuid: %s)",
                        ~ToString(Transaction_->Id_),
                        ~ToString(cellGuid));
                    OnError(TError("Error aborting transaction at cell %s",
                        ~ToString(cellGuid))
                        << *rsp);
                }
            }
        }

        void OnError(const TError& error)
        {
            if (!Promise_.TrySet(error))
                return;

            Awaiter_->Cancel();
        }

        void OnComplete()
        {
            if (!Promise_.TrySet(TError()))
                return;

            Transaction_->DoAbort(TError("Transaction aborted by user request"));
        }
    };

    TAsyncError SendAbort(const TMutationId& mutationId = NullMutationId)
    {
        return New<TAbortSession>(this, mutationId)->Run();
    }


    void FireAborted()
    {
        Aborted_.Set();
    }

    void DoAbort(const TError& error)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        {
            TGuard<TSpinLock> guard(SpinLock_);
            if (State_ == EState::Aborted)
                return;
            State_ = EState::Aborted;
            Error_ = error;
        }

        FireAborted();
    }


    TNullable<TCellGuid> GetCoordinatorCellGuid()
    {
        switch (Type_) {
            case ETransactionType::Master:
                return Owner_->MasterCellGuid_;
            
            case ETransactionType::Tablet: {
                auto participantGuids = GetParticipantGuids();
                if (participantGuids.empty()) {
                    return Null;
                }
                return participantGuids[0];
            }

            default:
                YUNREACHABLE();
        }
    }

    std::vector<TCellGuid> GetParticipantGuids()
    {
        TGuard<TSpinLock> guard(SpinLock_);
        return std::vector<TCellGuid>(
                ParticipantGuids_.begin(),
                ParticipantGuids_.end());
    }

};

////////////////////////////////////////////////////////////////////////////////

TTransactionManager::TImpl::TImpl(
    TTransactionManagerConfigPtr config,
    const TCellGuid& masterCellGuid,
    IChannelPtr masterChannel,
    ITimestampProviderPtr timestampProvider,
    TCellDirectoryPtr cellDirectory)
    : Config_(config)
    , MasterChannel_(masterChannel)
    , MasterCellGuid_(masterCellGuid)
    , TimestampProvider_(timestampProvider)
    , CellDirectory_(cellDirectory)
{
    YCHECK(Config_);
    YCHECK(MasterChannel_);
    YCHECK(TimestampProvider_);
    YCHECK(CellDirectory_);
}

TFuture<TErrorOr<ITransactionPtr>> TTransactionManager::TImpl::AsyncStart(const TTransactionStartOptions& options)
{
    VERIFY_THREAD_AFFINITY_ANY();

    typedef TErrorOr<ITransactionPtr> TOutput;

    auto transaction = New<TTransaction>(this);
    return transaction->Start(options).Apply(
        BIND([=] (TError error) -> TOutput {
            if (error.IsOK()) {
                return TOutput(transaction);
            }
            return TOutput(error);
    }));
}

ITransactionPtr TTransactionManager::TImpl::Start(const TTransactionStartOptions& options)
{
    auto transactionOrError = AsyncStart(options).Get();
    THROW_ERROR_EXCEPTION_IF_FAILED(transactionOrError, "Error starting transaction");
    return transactionOrError.GetValue();
}

ITransactionPtr TTransactionManager::TImpl::Attach(const TTransactionAttachOptions& options)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto transaction = New<TTransaction>(this);
    transaction->Attach(options);
    return transaction;
}

void TTransactionManager::TImpl::AsyncAbortAll()
{
    VERIFY_THREAD_AFFINITY_ANY();

    std::vector<ITransactionPtr> transactions;
    {
        TGuard<TSpinLock> guard(SpinLock_);
        FOREACH (auto* rawTransaction, AliveTransactions_) {
            auto transaction = TRefCounted::DangerousGetPtr(rawTransaction);
            if (transaction) {
                transactions.push_back(transaction);
            }
        }
    }

    FOREACH (const auto& transaction, transactions) {
        transaction->AsyncAbort();
    }
}

////////////////////////////////////////////////////////////////////////////////

TTransactionManager::TTransactionManager(
    TTransactionManagerConfigPtr config,
    const NHive::TCellGuid& masterCellGuid,
    IChannelPtr masterChannel,
    ITimestampProviderPtr timestampProvider,
    TCellDirectoryPtr cellDirectory)
    : Impl(New<TImpl>(
        config,
        masterCellGuid,
        masterChannel,
        timestampProvider,
        cellDirectory))
{ }

TFuture<TErrorOr<ITransactionPtr>> TTransactionManager::AsyncStart(const TTransactionStartOptions& options)
{
    return Impl->AsyncStart(options);
}

ITransactionPtr TTransactionManager::Start(const TTransactionStartOptions& options)
{
    return Impl->Start(options);
}

ITransactionPtr TTransactionManager::Attach(const TTransactionAttachOptions& options)
{
    return Impl->Attach(options);
}

void TTransactionManager::AsyncAbortAll()
{
    Impl->AsyncAbortAll();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionClient
} // namespace NYT
