#include "stdafx.h"
#include "transaction_manager.h"
#include "timestamp_provider.h"
#include "config.h"
#include "private.h"

#include <core/concurrency/thread_affinity.h>
#include <core/concurrency/delayed_executor.h>
#include <core/concurrency/parallel_awaiter.h>

#include <core/ytree/public.h>

#include <core/rpc/helpers.h>

#include <ytlib/transaction_client/transaction_ypath_proxy.h>

#include <ytlib/object_client/helpers.h>
#include <ytlib/object_client/master_ypath_proxy.h>
#include <ytlib/object_client/object_service_proxy.h>

#include <ytlib/hive/cell_directory.h>
#include <ytlib/hive/transaction_supervisor_service_proxy.h>

#include <ytlib/tablet_client/tablet_service_proxy.h>

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

static const auto& Logger = TransactionClientLogger;
static std::atomic<ui32> TabletTransactionCounter; // used as a part of transaction id

////////////////////////////////////////////////////////////////////////////////

class TTransactionManager::TImpl
    : public TRefCounted
{
public:
    TImpl(
        TTransactionManagerConfigPtr config,
        TCellTag cellTag,
        const TCellId& cellId,
        IChannelPtr channel,
        ITimestampProviderPtr timestampProvider,
        TCellDirectoryPtr cellDirectory);

    TFuture<TTransactionPtr> Start(
        ETransactionType type,
        const TTransactionStartOptions& options);

    TTransactionPtr Attach(const TTransactionAttachOptions& options);

    void AbortAll();

private:
    friend class TTransaction;

    TTransactionManagerConfigPtr Config_;
    IChannelPtr MasterChannel_;
    TCellTag CellTag_;
    TCellId CellId_;
    ITimestampProviderPtr TimestampProvider_;
    TCellDirectoryPtr CellDirectory_;

    TSpinLock SpinLock_;
    yhash_set<TTransaction::TImpl*> AliveTransactions_;

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
        , AutoAbort_(false)
        , Ping_(false)
        , PingAncestors_(false)
        , State_(ETransactionState::Initializing)
        , Aborted_(NewPromise<void>())
        , StartTimestamp_(NullTimestamp)
    { }

    ~TImpl()
    {
        Unregister();
    }


    TFuture<void> Start(
        ETransactionType type,
        const TTransactionStartOptions& options)
    {
        try {
            ValidateStartOptions(type, options);
        } catch (const std::exception& ex) {
            return MakeFuture(TError(ex));
        }

        Type_ = type;
        AutoAbort_ = options.AutoAbort;
        Ping_ = options.Ping;
        PingAncestors_ = options.PingAncestors;
        Timeout_ = options.Timeout;

        return Owner_->TimestampProvider_->GenerateTimestamps()
            .Apply(BIND(&TImpl::OnGotStartTimestamp, MakeStrong(this), options));
    }

    void Attach(const TTransactionAttachOptions& options)
    {
        YCHECK(TypeFromId(options.Id) == EObjectType::Transaction);

        Type_ = ETransactionType::Master;
        Id_ = options.Id;
        AutoAbort_ = options.AutoAbort;
        Ping_ = options.Ping;
        PingAncestors_ = options.PingAncestors;
        State_ = ETransactionState::Active;

        YCHECK(CellIdToStartTransactionResult_.insert(std::make_pair(
            Owner_->CellId_,
            MakePromise<void>(TError()))).second);
    
        Register();

        LOG_INFO("Master transaction attached (TransactionId: %v, AutoAbort: %v, Ping: %v, PingAncestors: %v)",
            Id_,
            AutoAbort_,
            Ping_,
            PingAncestors_);

        if (Ping_) {
            RunPeriodicPings();
        }
    }

    TFuture<void> Commit(const TTransactionCommitOptions& options)
    {
        VERIFY_THREAD_AFFINITY(ClientThread);

        {
            TGuard<TSpinLock> guard(SpinLock_);
            if (!Error_.IsOK()) {
                return MakeFuture(Error_);
            }
            switch (State_) {
                case ETransactionState::Committing:
                    return MakeFuture(TError("Transaction is already being committed"));
                    break;

                case ETransactionState::Committed:
                    return MakeFuture(TError("Transaction is already committed"));
                    break;

                case ETransactionState::Aborted:
                    return MakeFuture(TError("Transaction is already aborted"));
                    break;

                case ETransactionState::Active:
                    State_ = ETransactionState::Committing;
                    break;

                default:
                    YUNREACHABLE();
            }
        }

        auto participantGuids = GetParticipantGuids();
        if (participantGuids.empty()) {
            {
                TGuard<TSpinLock> guard(SpinLock_);
                if (State_ != ETransactionState::Committing) {
                    return MakeFuture(Error_);
                }
                State_ = ETransactionState::Committed;
            }

            LOG_INFO("Trivial transaction committed (TransactionId: %v)",
                Id_);
            return VoidFuture;
        }

        auto coordinatorCellId = Type_ == ETransactionType::Master
            ? Owner_->CellId_
            : *participantGuids.begin();

        LOG_INFO("Committing transaction (TransactionId: %v, CoordinatorCellId: %v)",
            Id_,
            coordinatorCellId);

        auto channel = Owner_->CellDirectory_->GetChannelOrThrow(coordinatorCellId);
        TTransactionSupervisorServiceProxy proxy(channel);

        auto req = proxy.CommitTransaction();
        ToProto(req->mutable_transaction_id(), Id_);
        for (const auto& cellId : participantGuids) {
            if (cellId != coordinatorCellId) {
                ToProto(req->add_participant_cell_ids(), cellId);
            }
        }
        SetOrGenerateMutationId(req, options.MutationId);

        return req->Invoke().Apply(
            BIND(&TImpl::OnTransactionCommitted, MakeStrong(this), coordinatorCellId));
    }

    TFuture<void> Abort(const TTransactionAbortOptions& options = TTransactionAbortOptions())
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto this_ = MakeStrong(this);
        return SendAbort(options).Apply(BIND([=] () {
            UNUSED(this_);
            DoAbort(TError("Transaction aborted by user request"));
        }));
    }

    TFuture<void> Ping()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return SendPing();
    }

    void Detach()
    {
        VERIFY_THREAD_AFFINITY(ClientThread);

        {
            TGuard<TSpinLock> guard(SpinLock_);
            switch (State_) {
                case ETransactionState::Committed:
                    THROW_ERROR_EXCEPTION("Transaction is already committed (TransactionId: %v)", Id_);
                    break;

                case ETransactionState::Aborted:
                    THROW_ERROR_EXCEPTION("Transaction is already aborted (TransactionId: %v)", Id_);
                    break;

                case ETransactionState::Active:
                    State_ = ETransactionState::Detached;
                    break;

                case ETransactionState::Detached:
                    return;

                default:
                    YUNREACHABLE();
            }
        }

        LOG_INFO("Transaction detached (TransactionId: %v)",
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

    
    TFuture<void> AddTabletParticipant(const TCellId& cellId)
    {
        VERIFY_THREAD_AFFINITY(ClientThread);
        YCHECK(TypeFromId(cellId) == EObjectType::TabletCell);

        TPromise<void> promise;
        {
            TGuard<TSpinLock> guard(SpinLock_);
            
            if (State_ != ETransactionState::Active) {
                return MakeFuture(TError("Transaction is not active"));
            }
            
            if (!Error_.IsOK()) {
                THROW_ERROR Error_;
            }

            auto it = CellIdToStartTransactionResult_.find(cellId);
            if (it != CellIdToStartTransactionResult_.end()) {
                return it->second;
            }

            promise = NewPromise<void>();
            YCHECK(CellIdToStartTransactionResult_.insert(std::make_pair(cellId, promise)).second);
        }

        LOG_DEBUG("Adding transaction tablet participant (TransactionId: %v, CellId: %v)",
            Id_,
            cellId);

        auto channel = Owner_->CellDirectory_->GetChannelOrThrow(cellId);
        TTabletServiceProxy proxy(channel);

        auto req = proxy.StartTransaction();
        ToProto(req->mutable_transaction_id(), Id_);
        req->set_start_timestamp(StartTimestamp_);
        req->set_start_timestamp(StartTimestamp_);
        req->set_timeout(Timeout_.Get(Owner_->Config_->DefaultTransactionTimeout).MilliSeconds());

        req->Invoke().Subscribe(
            BIND(&TImpl::OnTabletParticipantAdded, MakeStrong(this), cellId, promise));

        return promise;
    }


    void SubscribeAborted(const TCallback<void()>& handler)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        Aborted_.ToFuture().Subscribe(BIND([=] (const TError& error) {
            if (error.IsOK()) {
                handler.Run();
            }
        }));
    }

    void UnsubscribeAborted(const TCallback<void()>& handler)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        YUNREACHABLE();
    }

private:
    friend class TTransactionManager::TImpl;

    TIntrusivePtr<TTransactionManager::TImpl> Owner_;
    ETransactionType Type_;
    bool AutoAbort_;
    bool Ping_;
    bool PingAncestors_;
    TNullable<TDuration> Timeout_;

    TSpinLock SpinLock_;
    ETransactionState State_;
    TPromise<void> Aborted_;
    yhash_map<TCellId, TPromise<void>> CellIdToStartTransactionResult_;
    TError Error_;

    TTimestamp StartTimestamp_;
    TTransactionId Id_;

    DECLARE_THREAD_AFFINITY_SLOT(ClientThread);

    

    static void ValidateStartOptions(
        ETransactionType type,
        const TTransactionStartOptions& options)
    {
        switch (type)
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
    }


    void Register()
    {
        if (AutoAbort_) {
            TGuard<TSpinLock> guard(Owner_->SpinLock_);
            YCHECK(Owner_->AliveTransactions_.insert(this).second);
        }        
    }

    void Unregister()
    {
        if (AutoAbort_) {
            {
                TGuard<TSpinLock> guard(Owner_->SpinLock_);
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

        LOG_INFO("Starting transaction (StartTimestamp: %v, Type: %v)",
            StartTimestamp_,
            Type_);

        switch (Type_) {
            case ETransactionType::Master:
                return StartMasterTransaction(options);
            case ETransactionType::Tablet:
                return StartTabletTransaction(options);
            default:
                YUNREACHABLE();
        }
    }

    TFuture<void> StartMasterTransaction(const TTransactionStartOptions& options)
    {
        TObjectServiceProxy proxy(Owner_->MasterChannel_);
        auto req = TMasterYPathProxy::CreateObjects();
        req->set_type(static_cast<int>(EObjectType::Transaction));
        if (options.Attributes) {
            ToProto(req->mutable_object_attributes(), *options.Attributes);
        }
        if (options.ParentId != NullTransactionId) {
            ToProto(req->mutable_transaction_id(), options.ParentId);
        }

        auto* reqExt = req->MutableExtension(NTransactionClient::NProto::TReqStartTransactionExt::create_transaction_ext);
        reqExt->set_enable_uncommitted_accounting(options.EnableUncommittedAccounting);
        reqExt->set_enable_staged_accounting(options.EnableStagedAccounting);
        reqExt->set_timeout(options.Timeout.Get(Owner_->Config_->DefaultTransactionTimeout).MilliSeconds());

        if (options.ParentId != NullTransactionId) {
            SetOrGenerateMutationId(req, options.MutationId);
        }

        return proxy.Execute(req).Apply(
            BIND(&TImpl::OnMasterTransactionStarted, MakeStrong(this)));
    }

    void OnMasterTransactionStarted(const TMasterYPathProxy::TErrorOrRspCreateObjectsPtr& rspOrError)
    {
        if (!rspOrError.IsOK()) {
            State_ = ETransactionState::Aborted;
            THROW_ERROR rspOrError;
        }

        State_ = ETransactionState::Active;

        const auto& rsp = rspOrError.Value();
        YCHECK(rsp->object_ids_size() == 1);
        Id_ = FromProto<TTransactionId>(rsp->object_ids(0));
        
        YCHECK(CellIdToStartTransactionResult_.insert(std::make_pair(
            Owner_->CellId_,
            MakePromise<void>(TError()))).second);

        LOG_INFO("Master transaction started (TransactionId: %v, StartTimestamp: %v, AutoAbort: %v, Ping: %v, PingAncestors: %v)",
            Id_,
            StartTimestamp_,
            AutoAbort_,
            Ping_,
            PingAncestors_);

        if (Ping_) {
            RunPeriodicPings();
        }
    }

    TFuture<void> StartTabletTransaction(const TTransactionStartOptions& options)
    {
        Id_ = MakeId(
            EObjectType::TabletTransaction,
            Owner_->CellTag_,
            static_cast<ui64>(StartTimestamp_),
            TabletTransactionCounter++);

        State_ = ETransactionState::Active;

        LOG_INFO("Tablet transaction started (TransactionId: %v, StartTimestamp: %v, AutoAbort: %v)",
            Id_,
            StartTimestamp_,
            AutoAbort_);

        // Start ping scheduling.
        // Participants will be added into it upon arrival.
        YCHECK(Ping_);
        RunPeriodicPings();

        return VoidFuture;
    }

    void OnTabletParticipantAdded(
        const TCellId& cellId,
        TPromise<void> promise,
        const TTabletServiceProxy::TErrorOrRspStartTransactionPtr& rspOrError)
    {
        if (rspOrError.IsOK()) {
            LOG_DEBUG("Transaction tablet participant added (TransactionId: %v, CellId: %v)",
                Id_,
                cellId);
        } else {
            LOG_DEBUG(rspOrError, "Error adding transaction tablet participant (TransactionId: %v, CellId: %v)",
                Id_,
                cellId);

            DoAbort(TError("Error adding participant %v to transaction %v",
                cellId,
                Id_)
                << rspOrError);
        }

        promise.Set(rspOrError);
    }

    void OnTransactionCommitted(
        const TCellId& cellId,
        const TTransactionSupervisorServiceProxy::TErrorOrRspCommitTransactionPtr& rspOrError)
    {
        if (!rspOrError.IsOK()) {
            auto error = TError("Error committing transaction at cell %v",
                cellId)
                << rspOrError;
            DoAbort(error);
            THROW_ERROR error;
        }

        {
            TGuard<TSpinLock> guard(SpinLock_);
            if (State_ != ETransactionState::Committing) {
                THROW_ERROR Error_;
            }
            State_ = ETransactionState::Committed;
        }

        LOG_INFO("Transaction committed (TransactionId: %v)",
            Id_);
    }


    class TPingSession
        : public TRefCounted
    {
    public:
        explicit TPingSession(TIntrusivePtr<TTransaction::TImpl> transaction)
            : Transaction_(transaction)
            , Promise_(NewPromise<void>())
            , Awaiter_(New<TParallelAwaiter>(GetSyncInvoker()))
        { }

        TFuture<void> Run()
        {
            auto participantGuids = Transaction_->GetParticipantGuids();
            for (const auto& cellId : participantGuids) {
                LOG_DEBUG("Pinging transaction (TransactionId: %v, CellId: %v)",
                    Transaction_->Id_,
                    cellId);

                auto channel = Transaction_->Owner_->CellDirectory_->GetChannelOrThrow(cellId);
                TTransactionSupervisorServiceProxy proxy(channel);

                auto req = proxy.PingTransaction();
                ToProto(req->mutable_transaction_id(), Transaction_->Id_);

                if (cellId == Transaction_->Owner_->CellId_) {
                    auto* reqExt = req->MutableExtension(NProto::TReqPingTransactionExt::ping_transaction_ext);
                    reqExt->set_ping_ancestors(Transaction_->PingAncestors_);
                }

                Awaiter_->Await(
                    req->Invoke(),
                    BIND(&TPingSession::OnResponse, MakeStrong(this), cellId));
            }

            Awaiter_->Complete(
                BIND(&TPingSession::OnComplete, MakeStrong(this)));

            return Promise_;
        }

    private:
        TIntrusivePtr<TTransaction::TImpl> Transaction_;
        TPromise<void> Promise_;
        TParallelAwaiterPtr Awaiter_;


        void OnResponse(
            const TCellId& cellId,
            const TTransactionSupervisorServiceProxy::TErrorOrRspPingTransactionPtr& rspOrError)
        {
            if (rspOrError.IsOK()) {
                LOG_DEBUG("Transaction pinged (TransactionId: %v, CellId: %v)",
                    Transaction_->Id_,
                    cellId);

            } else {
                if (rspOrError.GetCode() == NYTree::EErrorCode::ResolveError) {
                    // Hard error.
                    LOG_WARNING("Transaction has expired or was aborted (TransactionId: %v, CellId: %v)",
                        Transaction_->Id_,
                        cellId);
                    OnError(TError("Transaction %v has expired or was aborted at cell %v",
                        Transaction_->Id_,
                        cellId));
                } else {
                    // Soft error.
                    LOG_WARNING(rspOrError, "Error pinging transaction (TransactionId: %v, CellId: %v)",
                        Transaction_->Id_,
                        cellId);
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
            Promise_.TrySet(TError());
        }
        
    };

    TFuture<void> SendPing()
    {
        return New<TPingSession>(this)->Run();
    }

    void RunPeriodicPings()
    {
        {
            TGuard<TSpinLock> guard(SpinLock_);
            if (State_ != ETransactionState::Active)
                return;           
        }

        auto this_ = MakeStrong(this);
        SendPing().Subscribe(BIND([this, this_] (const TError& error) {
            if (!error.IsOK())
                return;
            TDelayedExecutor::Submit(
                BIND(IgnoreResult(&TImpl::RunPeriodicPings), MakeWeak(this)),
                Owner_->Config_->PingPeriod);
        }));
    }


    class TAbortSession
        : public TRefCounted
    {
    public:
        // NB: Avoid passing TIntrusivePtr here since destruction might be in progress.
        explicit TAbortSession(
            TTransaction::TImpl* transaction,
            const TTransactionAbortOptions& options)
            : Transaction_(transaction)
            , TransactionId_(transaction->GetId())
            , Options_(options)
        { }

        TFuture<void> Run()
        {
            auto participantGuids = Transaction_->GetParticipantGuids();
            for (const auto& cellId : participantGuids) {
                LOG_DEBUG("Aborting transaction (TransactionId: %v, CellId: %v)",
                    TransactionId_,
                    cellId);

                auto channel = Transaction_->Owner_->CellDirectory_->FindChannel(cellId);
                if (!channel)
                    continue; // better skip

                TTransactionSupervisorServiceProxy proxy(channel);
                auto req = proxy.AbortTransaction();
                ToProto(req->mutable_transaction_id(), TransactionId_);
                req->set_force(Options_.Force);
                if (Options_.MutationId != NullMutationId) {
                    SetMutationId(req, Options_.MutationId);
                }

                Awaiter_->Await(
                    req->Invoke(),
                    BIND(&TAbortSession::OnResponse, MakeStrong(this), cellId));
            }

            Transaction_ = nullptr; // avoid producing dangling referencep

            Awaiter_->Complete(
                BIND(&TAbortSession::OnComplete, MakeStrong(this)));

            return Promise_;
        }

    private:
        TTransaction::TImpl* Transaction_;
        TTransactionId TransactionId_;
        TTransactionAbortOptions Options_;

        TPromise<void> Promise_ = NewPromise<void>();
        TParallelAwaiterPtr Awaiter_ = New<TParallelAwaiter>(GetSyncInvoker());


        void OnResponse(
            const TCellId& cellId,
            const TTransactionSupervisorServiceProxy::TErrorOrRspAbortTransactionPtr& rspOrError)
        {
            if (rspOrError.IsOK()) {
                LOG_DEBUG("Transaction aborted (TransactionId: %v, CellId: %v)",
                    TransactionId_,
                    cellId);
            } else {
                if (rspOrError.GetCode() == NYTree::EErrorCode::ResolveError) {
                    LOG_DEBUG("Transaction has expired or was already aborted, ignored (TransactionId: %v, CellId: %v)",
                        TransactionId_,
                        cellId);
                } else {
                    LOG_WARNING(rspOrError, "Error aborting transaction (TransactionId: %v, CellId: %v)",
                        TransactionId_,
                        cellId);
                    OnError(TError("Error aborting transaction at cell %v",
                        cellId)
                        << rspOrError);
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
            Promise_.TrySet(TError());
        }
    };

    TFuture<void> SendAbort(const TTransactionAbortOptions& options = TTransactionAbortOptions())
    {
        return New<TAbortSession>(this, options)->Run();
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
            if (State_ == ETransactionState::Aborted)
                return;
            State_ = ETransactionState::Aborted;
            Error_ = error;
        }

        FireAborted();
    }


    TNullable<TCellId> GetCoordinatorCellId()
    {
        switch (Type_) {
            case ETransactionType::Master:
                return Owner_->CellId_;
            
            case ETransactionType::Tablet: {
                auto ids = GetParticipantGuids();
                if (ids.empty()) {
                    // NB: NullCellId is a valid cell guid.
                    return Null;
                }
                return ids[0];
            }

            default:
                YUNREACHABLE();
        }
    }

    std::vector<TCellId> GetParticipantGuids()
    {
        TGuard<TSpinLock> guard(SpinLock_);
        std::vector<TCellId> result;
        for (const auto& pair : CellIdToStartTransactionResult_) {
            if (pair.second.IsSet()) {
                result.push_back(pair.first);
            }
        }
        return result;
    }

};

////////////////////////////////////////////////////////////////////////////////

TTransactionManager::TImpl::TImpl(
    TTransactionManagerConfigPtr config,
    TCellTag cellTag,
    const TCellId& cellId,
    IChannelPtr masterChannel,
    ITimestampProviderPtr timestampProvider,
    TCellDirectoryPtr cellDirectory)
    : Config_(config)
    , MasterChannel_(masterChannel)
    , CellTag_(cellTag)
    , CellId_(cellId)
    , TimestampProvider_(timestampProvider)
    , CellDirectory_(cellDirectory)
{
    YCHECK(Config_);
    YCHECK(MasterChannel_);
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

TTransactionPtr TTransactionManager::TImpl::Attach(const TTransactionAttachOptions& options)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto transaction = New<TTransaction::TImpl>(this);
    transaction->Attach(options);
    return TTransaction::Create(transaction);
}

void TTransactionManager::TImpl::AbortAll()
{
    VERIFY_THREAD_AFFINITY_ANY();

    std::vector<TIntrusivePtr<TTransaction::TImpl>> transactions;
    {
        TGuard<TSpinLock> guard(SpinLock_);
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
    : Impl_(impl)
{ }

TTransactionPtr TTransaction::Create(TIntrusivePtr<TImpl> impl)
{
    return New<TTransaction>(impl);
}

TTransaction::~TTransaction()
{ }

TFuture<void> TTransaction::Commit(const TTransactionCommitOptions& options)
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

TFuture<void> TTransaction::AddTabletParticipant(const TCellId& cellId)
{
    return Impl_->AddTabletParticipant(cellId);
}

DELEGATE_SIGNAL(TTransaction, void(), Aborted, *Impl_);

////////////////////////////////////////////////////////////////////////////////

TTransactionManager::TTransactionManager(
    TTransactionManagerConfigPtr config,
    TCellTag cellTag,
    const TCellId& cellId,
    IChannelPtr masterChannel,
    ITimestampProviderPtr timestampProvider,
    TCellDirectoryPtr cellDirectory)
    : Impl_(New<TImpl>(
        config,
        cellTag,
        cellId,
        masterChannel,
        timestampProvider,
        cellDirectory))
{ }

TTransactionManager::~TTransactionManager()
{ }

TFuture<TTransactionPtr> TTransactionManager::Start(
    ETransactionType type,
    const TTransactionStartOptions& options)
{
    return Impl_->Start(type, options);
}

TTransactionPtr TTransactionManager::Attach(const TTransactionAttachOptions& options)
{
    return Impl_->Attach(options);
}

void TTransactionManager::AbortAll()
{
    Impl_->AbortAll();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionClient
} // namespace NYT
