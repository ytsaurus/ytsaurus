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

TTransactionStartOptions::TTransactionStartOptions()
    : EnableUncommittedAccounting(true)
    , EnableStagedAccounting(true)
{ }

TTransactionStartOptions::TTransactionStartOptions(const NApi::TTransactionStartOptions& other)
    : TTransactionStartOptions()
{
    static_cast<NApi::TTransactionStartOptions&>(*this) = other;
}

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
        TCellTag cellTag,
        const TCellId& cellId,
        IChannelPtr channel,
        ITimestampProviderPtr timestampProvider,
        TCellDirectoryPtr cellDirectory);

    TFuture<TErrorOr<TTransactionPtr>> Start(
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

class TTransaction::TImpl
    : public TRefCounted
{
public:
    explicit TImpl(TIntrusivePtr<TTransactionManager::TImpl> owner)
        : Owner_(owner)
        , AutoAbort_(false)
        , Ping_(false)
        , PingAncestors_(false)
        , State_(EState::Initializing)
        , Aborted_(NewPromise())
        , StartTimestamp_(NullTimestamp)
    { }

    ~TImpl()
    {
        Unregister();
    }


    TAsyncError Start(
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
        State_ = EState::Active;

        YCHECK(CellIdToStartTransactionResult_.insert(std::make_pair(
            Owner_->CellId_,
            MakePromise<TError>(TError()))).second);
    
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

    TAsyncError Commit(const TMutationId& mutationId)
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

        auto participantGuids = GetParticipantGuids();
        if (participantGuids.empty()) {
            {
                TGuard<TSpinLock> guard(SpinLock_);
                if (State_ != EState::Committing) {
                    return MakeFuture(Error_);
                }
                State_ = EState::Committed;
            }

            LOG_INFO("Trivial transaction committed (TransactionId: %v)",
                Id_);
            return OKFuture;
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
        SetOrGenerateMutationId(req, mutationId);

        return req->Invoke().Apply(
            BIND(&TImpl::OnTransactionCommitted, MakeStrong(this), coordinatorCellId));
    }

    TAsyncError Abort(bool force, const TMutationId& mutationId)
    {
        auto this_ = MakeStrong(this);
        return SendAbort(force, mutationId).Apply(BIND([this, this_] (const TError& error) -> TError {
            if (error.IsOK()) {
                DoAbort(TError("Transaction aborted by user request"));
            }
            return error;
        }));
    }

    TAsyncError Ping()
    {
        return SendPing();
    }

    void Detach()
    {
        VERIFY_THREAD_AFFINITY(ClientThread);

        {
            TGuard<TSpinLock> guard(SpinLock_);
            switch (State_) {
                case EState::Committed:
                    THROW_ERROR_EXCEPTION("Transaction is already committed (TransactionId: %v)", Id_);
                    break;

                case EState::Aborted:
                    THROW_ERROR_EXCEPTION("Transaction is already aborted (TransactionId: %v)", Id_);
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

        LOG_INFO("Transaction detached (TransactionId: %v)",
            Id_);
    }


    ETransactionType GetType() const
    {
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

    
    TAsyncError AddTabletParticipant(const NElection::TCellId& cellId)
    {
        VERIFY_THREAD_AFFINITY(ClientThread);
        YCHECK(TypeFromId(cellId) == EObjectType::TabletCell);

        TAsyncErrorPromise promise;
        {
            TGuard<TSpinLock> guard(SpinLock_);
            
            if (State_ != EState::Active) {
                return MakeFuture(TError("Transaction is not active"));
            }
            
            if (!Error_.IsOK()) {
                THROW_ERROR Error_;
            }

            auto it = CellIdToStartTransactionResult_.find(cellId);
            if (it != CellIdToStartTransactionResult_.end()) {
                return it->second;
            }

            promise = NewPromise<TError>();
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
        if (Timeout_) {
            req->set_timeout(Timeout_->MilliSeconds());
        }
        
        req->Invoke().Subscribe(
            BIND(&TImpl::OnTabletParticipantAdded, MakeStrong(this), cellId, promise));

        return promise;
    }


    void SubscribeAborted(const TCallback<void()>& handler)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        Aborted_.Subscribe(handler);
    }

    void UnsubscribeAborted(const TCallback<void()>& handler)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        YUNREACHABLE();
    }

private:
    friend class TTransactionManager::TImpl;

    DECLARE_ENUM(EState,
        (Initializing)
        (Active)
        (Aborted)
        (Committing)
        (Committed)
        (Detached)
   );

    TIntrusivePtr<TTransactionManager::TImpl> Owner_;
    ETransactionType Type_;
    bool AutoAbort_;
    bool Ping_;
    bool PingAncestors_;
    TNullable<TDuration> Timeout_;

    TSpinLock SpinLock_;
    EState State_;
    TPromise<void> Aborted_;
    yhash_map<TCellId, TAsyncErrorPromise> CellIdToStartTransactionResult_;
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

            if (State_ == EState::Active) {
                SendAbort();
            }
        }
    }


    TAsyncError OnGotStartTimestamp(const TTransactionStartOptions& options, TErrorOr<TTimestamp> timestampOrError)
    {
        if (!timestampOrError.IsOK()) {
            return MakeFuture(TError(timestampOrError));
        }
        StartTimestamp_ = timestampOrError.Value();

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

    TAsyncError StartMasterTransaction(const TTransactionStartOptions& options)
    {
        TObjectServiceProxy proxy(Owner_->MasterChannel_);
        auto req = TMasterYPathProxy::CreateObjects();
        req->set_type(EObjectType::Transaction);
        if (options.Attributes) {
            ToProto(req->mutable_object_attributes(), *options.Attributes);
        }
        if (options.ParentId != NullTransactionId) {
            ToProto(req->mutable_transaction_id(), options.ParentId);
        }

        auto* reqExt = req->MutableExtension(NTransactionClient::NProto::TReqStartTransactionExt::create_transaction_ext);
        reqExt->set_enable_uncommitted_accounting(options.EnableUncommittedAccounting);
        reqExt->set_enable_staged_accounting(options.EnableStagedAccounting);
        if (options.Timeout) {
            reqExt->set_timeout(options.Timeout->MilliSeconds());
        }

        if (options.ParentId != NullTransactionId) {
            SetOrGenerateMutationId(req, options.MutationId);
        }

        return proxy.Execute(req).Apply(
            BIND(&TImpl::OnMasterTransactionStarted, MakeStrong(this)));
    }

    TError OnMasterTransactionStarted(TMasterYPathProxy::TRspCreateObjectsPtr rsp)
    {
        if (!rsp->IsOK()) {
            State_ = EState::Aborted;
            return rsp->GetError();
        }

        State_ = EState::Active;
        
        YCHECK(rsp->object_ids_size() == 1);
        Id_ = FromProto<TTransactionId>(rsp->object_ids(0));
        
        YCHECK(CellIdToStartTransactionResult_.insert(std::make_pair(
            Owner_->CellId_,
            MakePromise<TError>(TError()))).second);

        LOG_INFO("Master transaction started (TransactionId: %v, StartTimestamp: %v, AutoAbort: %v, Ping: %v, PingAncestors: %v)",
            Id_,
            StartTimestamp_,
            AutoAbort_,
            Ping_,
            PingAncestors_);

        if (Ping_) {
            RunPeriodicPings();
        }

        return TError();
    }

    TAsyncError StartTabletTransaction(const TTransactionStartOptions& options)
    {
        Id_ = MakeId(
            EObjectType::TabletTransaction,
            Owner_->CellTag_,
            static_cast<ui64>(StartTimestamp_),
            TabletTransactionCounter++);

        State_ = EState::Active;

        LOG_INFO("Tablet transaction started (TransactionId: %v, StartTimestamp: %v, AutoAbort: %v)",
            Id_,
            StartTimestamp_,
            AutoAbort_);

        // Start ping scheduling.
        // Participants will be added into it upon arrival.
        YCHECK(Ping_);
        RunPeriodicPings();

        return OKFuture;
    }

    void OnTabletParticipantAdded(
        const TCellId& cellId,
        TAsyncErrorPromise promise,
        TTabletServiceProxy::TRspStartTransactionPtr rsp)
    {
        if (rsp->IsOK()) {
            LOG_DEBUG("Transaction tablet participant added (TransactionId: %v, CellId: %v)",
                Id_,
                cellId);
        } else {
            LOG_DEBUG(*rsp, "Error adding transaction tablet participant (TransactionId: %v, CellId: %v)",
                Id_,
                cellId);

            DoAbort(TError("Error adding participant %v to transaction %v",
                cellId,
                Id_)
                << *rsp);
        }

        promise.Set(rsp->GetError());
    }

    TError OnTransactionCommitted(const TCellId& cellId, TTransactionSupervisorServiceProxy::TRspCommitTransactionPtr rsp)
    {
        if (!rsp->IsOK()) {
            auto error = TError("Error committing transaction at cell %v",
                cellId)
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

        LOG_INFO("Transaction committed (TransactionId: %v)",
            Id_);
        return TError();
    }


    class TPingSession
        : public TRefCounted
    {
    public:
        explicit TPingSession(TIntrusivePtr<TTransaction::TImpl> transaction)
            : Transaction_(transaction)
            , Promise_(NewPromise<TError>())
            , Awaiter_(New<TParallelAwaiter>(GetSyncInvoker()))
        { }

        TAsyncError Run()
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
        TAsyncErrorPromise Promise_;
        TParallelAwaiterPtr Awaiter_;


        void OnResponse(const TCellId& cellId, TTransactionSupervisorServiceProxy::TRspPingTransactionPtr rsp)
        {
            if (rsp->IsOK()) {
                LOG_DEBUG("Transaction pinged (TransactionId: %v, CellId: %v)",
                    Transaction_->Id_,
                    cellId);

            } else {
                if (rsp->GetError().GetCode() == NYTree::EErrorCode::ResolveError) {
                    // Hard error.
                    LOG_WARNING("Transaction has expired or was aborted (TransactionId: %v, CellId: %v)",
                        Transaction_->Id_,
                        cellId);
                    OnError(TError("Transaction %v has expired or was aborted at cell %v",
                        Transaction_->Id_,
                        cellId));
                } else {
                    // Soft error.
                    LOG_WARNING(*rsp, "Error pinging transaction (TransactionId: %v, CellId: %v)",
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

    TAsyncError SendPing()
    {
        return New<TPingSession>(this)->Run();
    }

    void RunPeriodicPings()
    {
        {
            TGuard<TSpinLock> guard(SpinLock_);
            if (State_ != EState::Active)
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
            bool force,
            const TMutationId& mutationId)
            : Transaction_(transaction)
            , TransactionId_(transaction->GetId())
            , Force_(force)
            , MutationId_(mutationId)
            , Promise_(NewPromise<TError>())
            , Awaiter_(New<TParallelAwaiter>(GetSyncInvoker()))
        { }

        TAsyncError Run()
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
                req->set_force(Force_);

                if (MutationId_ != NullMutationId) {
                    SetMutationId(req, MutationId_);
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
        bool Force_;
        TMutationId MutationId_;
        TAsyncErrorPromise Promise_;
        TParallelAwaiterPtr Awaiter_;


        void OnResponse(const TCellId& cellId, TTransactionSupervisorServiceProxy::TRspAbortTransactionPtr rsp)
        {
            if (rsp->IsOK()) {
                LOG_DEBUG("Transaction aborted (TransactionId: %v, CellId: %v)",
                    TransactionId_,
                    cellId);

            } else {
                if (rsp->GetError().GetCode() == NYTree::EErrorCode::ResolveError) {
                    LOG_DEBUG("Transaction has expired or was already aborted, ignored (TransactionId: %v, CellId: %v)",
                        TransactionId_,
                        cellId);
                } else {
                    LOG_WARNING(*rsp, "Error aborting transaction (TransactionId: %v, CellId: %v)",
                        TransactionId_,
                        cellId);
                    OnError(TError("Error aborting transaction at cell %v",
                        cellId)
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
            Promise_.TrySet(TError());
        }
    };

    TAsyncError SendAbort(bool force = false, const TMutationId& mutationId = NullMutationId)
    {
        return New<TAbortSession>(this, force, mutationId)->Run();
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

TFuture<TErrorOr<TTransactionPtr>> TTransactionManager::TImpl::Start(
    ETransactionType type,
    const TTransactionStartOptions& options)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto transaction = New<TTransaction::TImpl>(this);
    return transaction->Start(type, options).Apply(
        BIND([=] (const TError& error) -> TErrorOr<TTransactionPtr> {
            if (!error.IsOK()) {
                return error;
            }
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
        transaction->Abort(false, NullMutationId);
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

TAsyncError TTransaction::Commit(
    const TMutationId& mutationId /*= NullMutationId*/)
{
    return Impl_->Commit(mutationId);
}

TAsyncError TTransaction::Abort(
    bool force, /*= false */
    const TMutationId& mutationId /*= NullMutationId*/)
{
    return Impl_->Abort(force, mutationId);
}

void TTransaction::Detach()
{
    Impl_->Detach();
}

TAsyncError TTransaction::Ping()
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

TAsyncError TTransaction::AddTabletParticipant(const NElection::TCellId& cellId)
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

TFuture<TErrorOr<TTransactionPtr>> TTransactionManager::Start(
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
