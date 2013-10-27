#include "stdafx.h"
#include "transaction_manager.h"
#include "transaction.h"
#include "config.h"
#include "private.h"

#include <core/concurrency/thread_affinity.h>
#include <core/concurrency/delayed_executor.h>

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
        IChannelPtr channel,
        ITimestampProviderPtr timestampProvider,
        TCellDirectoryPtr cellDirectory);

    TFuture<TErrorOr<ITransactionPtr>> AsyncStart(const TTransactionStartOptions& options);

    ITransactionPtr Start(const TTransactionStartOptions& options);

    ITransactionPtr Attach(const TTransactionAttachOptions& options);

    void AsyncAbortAll();

private:
    friend class TTransaction;

    TTransactionManagerConfigPtr Config;
    IChannelPtr Channel;
    ITimestampProviderPtr TimestampProvider;
    TCellDirectoryPtr CellDirectory;

    TSpinLock SpinLock;
    yhash_set<TTransaction*> AliveTransactions;

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
        , Proxy_(owner->Channel)
        , State_(EState::Active)
        , Aborted_(NewPromise())
        , StartTimestamp_(NullTimestamp)
    { }

    ~TTransaction()
    {
        if (AutoAbort_) {
            if (State_ == EState::Active) {
                InvokeAbort();
            }

            {
                TGuard<TSpinLock> guard(Owner_->SpinLock);
                YCHECK(Owner_->AliveTransactions.erase(this) == 1);
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

        AutoAbort_ = options.AutoAbort;
        Ping_ = options.Ping;
        PingAncestors_ = options.PingAncestors;
        Timeout_ = options.Timeout;

        return Owner_->TimestampProvider->GenerateNewTimestamp()
            .Apply(BIND(&TTransaction::OnGotStartTimestamp, MakeStrong(this), options));
    }

    void Attach(const TTransactionAttachOptions& options)
    {
        Id_ = options.Id;
        AutoAbort_ = options.AutoAbort;
        Ping_ = options.Ping;
        PingAncestors_ = options.PingAncestors;
        Register();

        LOG_INFO("Transaction attached (TransactionId: %s, AutoAbort: %s, Ping: %s, PingAncestors: %s)",
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
            if (!BackgroundError_.IsOK()) {
                return MakeFuture(BackgroundError_);
            }
            switch (State_) {
                case EState::Committed:
                    return MakeFuture(TError("Transaction is already committed"));
                    break;

                case EState::Aborted:
                    return MakeFuture(TError("Transaction is already aborted"));
                    break;

                case EState::Active:
                    State_ = EState::Committed;
                    break;

                default:
                    YUNREACHABLE();
            }
        }

        LOG_INFO("Committing transaction (TransactionId: %s)", ~ToString(Id_));

        auto req = Proxy_.CommitTransaction();
        ToProto(req->mutable_transaction_id(), Id_);
        SetOrGenerateMutationId(req, mutationId);

        return req->Invoke().Apply(
            BIND(&TTransaction::OnTransactionCommitted, MakeStrong(this)));
    }

    virtual TAsyncError AsyncAbort(const TMutationId& mutationId) override
    {
        return InvokeAbort(mutationId)
            .Apply(BIND(&TTransaction::OnTransactionAborted, MakeStrong(this)));
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

        LOG_INFO("Transaction detached (TransactionId: %s)", ~ToString(Id_));
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

        bool newParticipant;
        {
            TGuard<TSpinLock> guard(SpinLock_);
            if (!BackgroundError_.IsOK()) {
                THROW_ERROR BackgroundError_;
            }
            newParticipant =  ParticipantGuids_.insert(cellGuid).second;
        }

        if (newParticipant) {
            auto channel = Owner_->CellDirectory->GetChannel(cellGuid);
            if (!channel) {
                THROW_ERROR_EXCEPTION("Unknown tablet cell %s",
                    ~ToString(cellGuid));
            }

            LOG_DEBUG("Adding transaction participant (TransactionId: %s, CellGuid: %s)",
                ~ToString(Id_),
                ~ToString(cellGuid));

            TTransactionSupervisorServiceProxy proxy(channel);
            auto req = proxy.StartTransaction();
            req->set_start_timestamp(StartTimestamp_);

            auto* reqExt = req->MutableExtension(NTabletClient::NProto::TReqStartTransactionExt::start_transaction_ext);
            ToProto(reqExt->mutable_transaction_id(), Id_);
            if (Timeout_) {
                reqExt->set_timeout(Timeout_->MilliSeconds());
            }

            req->Invoke().Subscribe(
                BIND(&TTransaction::OnParticipantAdded, MakeStrong(this), cellGuid));
        }
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
    bool AutoAbort_;
    bool Ping_;
    bool PingAncestors_;
    TNullable<TDuration> Timeout_;

    TTransactionSupervisorServiceProxy Proxy_;

    //! Protects state transitions.
    TSpinLock SpinLock_;
    EState State_;
    TPromise<void> Aborted_;
    yhash_set<TCellGuid> ParticipantGuids_;
    TError BackgroundError_;

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
            TGuard<TSpinLock> guard(Owner_->SpinLock);
            YCHECK(Owner_->AliveTransactions.insert(this).second);

            LOG_DEBUG("Transaction registered");
        }        
    }

    void SchedulePing()
    {
        TDelayedExecutor::Submit(
            BIND(IgnoreResult(&TTransaction::SendPing), MakeWeak(this)),
            Owner_->Config->PingPeriod);
    }


    TAsyncError OnGotStartTimestamp(
        const TTransactionStartOptions& options,
        TErrorOr<TTimestamp> timestampOrError)
    {
        if (!timestampOrError.IsOK()) {
            return MakeFuture(TError(timestampOrError));
        }

        Register();

        auto startTimestamp = timestampOrError.GetValue();

        LOG_INFO("Starting transaction (StartTimestamp: %" PRId64 ", Type: %s)",
            startTimestamp,
            ~FormatEnum(options.Type));

        switch (options.Type) {
            case ETransactionType::Master:
                return StartMasterTransaction(options, startTimestamp);
            case ETransactionType::Tablet:
                return StartTabletTransaction(options, startTimestamp);
            default:
                YUNREACHABLE();
        }
    }

    TAsyncError StartMasterTransaction(const TTransactionStartOptions& options, TTimestamp startTimestamp)
    {
        auto req = Proxy_.StartTransaction();
        req->set_start_timestamp(startTimestamp);

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

    TAsyncError StartTabletTransaction(const TTransactionStartOptions& options, TTimestamp startTimestamp)
    {
        Id_ = MakeId(
            EObjectType::TabletTransaction,
            0, // TODO(babenko): cell id?
            static_cast<ui64>(startTimestamp),
            TabletTransactionCounter++);

        LOG_INFO("Tablet Transaction started (TransactionId: %s, StartTimestamp: %" PRId64 ", AutoAbort: %s)",
            ~ToString(Id_),
            StartTimestamp_,
            ~FormatBool(AutoAbort_));

        if (Ping_) {
            SendPing();
        }

        return MakeFuture(TError());
    }


    TError OnTransactionCommitted(TTransactionSupervisorServiceProxy::TRspCommitTransactionPtr rsp)
    {
        if (!rsp->IsOK()) {
            // Let's pretend the transaction was aborted.
            // No sync here, should be safe.
            State_ = EState::Aborted;
            FireAbort();
            return TError("Error committing transaction %s", ~ToString(Id_))
                << rsp->GetError();
        }
        LOG_INFO("Transaction committed (TransactionId: %s)", ~ToString(Id_));
        return TError();
    }


    TError OnTransactionAborted(TTransactionSupervisorServiceProxy::TRspAbortTransactionPtr rsp)
    {
        if (!rsp->IsOK()) {
            return TError("Error aborting transaction") << *rsp;
        }
        HandleAbort();
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

            SetBackgroundError(TError("Error adding participant cell %s",
                ~ToString(cellGuid))
                << *rsp);
        }
    }


    TAsyncError SendPing()
    {
        LOG_DEBUG("Pinging transaction (TransactionId: %s)", ~ToString(Id_));

        auto req = Proxy_.PingTransaction();
        ToProto(req->mutable_transaction_id(), Id_);

        auto* reqExt = req->MutableExtension(NProto::TReqPingTransactionExt::ping_transaction_ext);
        reqExt->set_ping_ancestors(PingAncestors_);
        
        return req->Invoke().Apply(
            BIND(&TTransaction::OnPingResponse, MakeStrong(this)));
    }

    TError OnPingResponse(TTransactionSupervisorServiceProxy::TRspPingTransactionPtr rsp)
    {
        if (rsp->IsOK()) {
            LOG_DEBUG("Transaction pinged (TransactionId: %s)", ~ToString(Id_));

            if (Ping_) {
                SchedulePing();
            }
        } else {
            if (rsp->GetError().GetCode() == NYTree::EErrorCode::ResolveError) {
                LOG_WARNING("Transaction has expired or was aborted (TransactionId: %s)",
                    ~ToString(Id_));
                HandleAbort();
            } else {
                LOG_WARNING(*rsp, "Error pinging transaction (TransactionId: %s)",
                    ~ToString(Id_));
            }
        }

        return *rsp;
    }


    TFuture<TTransactionSupervisorServiceProxy::TRspAbortTransactionPtr> InvokeAbort(
        const TMutationId& mutationId = NullMutationId)
    {
        auto req = Proxy_.AbortTransaction();
        ToProto(req->mutable_transaction_id(), Id_);
        if (mutationId != NullMutationId) {
            SetMutationId(req, mutationId);
        }
        return req->Invoke();
    }

    void FireAbort()
    {
        Aborted_.Set();
    }

    void HandleAbort()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        {
            TGuard<TSpinLock> guard(SpinLock_);
            if (State_ != EState::Active) {
                return;
            }
            State_ = EState::Aborted;
        }

        FireAbort();
    }

    void SetBackgroundError(const TError& error)
    {
        {
            TGuard<TSpinLock> guard(SpinLock_);
            if (State_ != EState::Active)
                return;
            if (!BackgroundError_.IsOK())
                return;
            BackgroundError_ = error;
        }

        HandleAbort();
    }

};

////////////////////////////////////////////////////////////////////////////////

TTransactionManager::TImpl::TImpl(
    TTransactionManagerConfigPtr config,
    IChannelPtr channel,
    ITimestampProviderPtr timestampProvider,
    TCellDirectoryPtr cellDirectory)
    : Config(config)
    , Channel(channel)
    , TimestampProvider(timestampProvider)
    , CellDirectory(cellDirectory)
{
    YCHECK(Config);
    YCHECK(Channel);
    YCHECK(TimestampProvider);
    YCHECK(CellDirectory);
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
        TGuard<TSpinLock> guard(SpinLock);
        FOREACH (auto* rawTransaction, AliveTransactions) {
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
    IChannelPtr channel,
    ITimestampProviderPtr timestampProvider,
    TCellDirectoryPtr cellDirectory)
    : Impl(New<TImpl>(
        config,
        channel,
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
