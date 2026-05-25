#include "cell_commit_session.h"

#include "client.h"
#include "config.h"
#include "connection.h"

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/actions/future.h>

namespace NYT::NApi::NNative {

using namespace NHiveClient;
using namespace NLogging;
using namespace NObjectClient;
using namespace NRpc;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

class TCellCommitSession
    : public ICellCommitSession
{
public:
    TCellCommitSession(
        IRegisterTransactionActionsRequestFactoryPtr requestFactory,
        TWeakPtr<TTransaction> transaction,
        TCellId cellId,
        TLogger logger,
        bool useUniformPrepareSignatures)
        : RequestFactory_(std::move(requestFactory))
        , Transaction_(std::move(transaction))
        , CellId_(cellId)
        , PrepareSignatureGenerator_(
            useUniformPrepareSignatures
                ? std::make_unique<TUniformSignatureGenerator>()
                : std::make_unique<TTransactionSignatureGenerator>(FinalTransactionSignature))
        , CommitSignatureGenerator_(/*targetSignature*/ FinalTransactionSignature)
        , Logger(logger.WithTag("CellId: %v", cellId))
    { }

    TTransactionSignatureGenerator* GetPrepareSignatureGenerator() override
    {
        return PrepareSignatureGenerator_.get();
    }

    TTransactionSignatureGenerator* GetCommitSignatureGenerator() override
    {
        return &CommitSignatureGenerator_;
    }

    void RegisterAction(NTransactionClient::TTransactionActionData data) override
    {
        if (Actions_.empty()) {
            PrepareSignatureGenerator_->RegisterRequest();
            CommitSignatureGenerator_.RegisterRequest();
        }
        Actions_.push_back(data);
    }

    TFuture<void> Invoke() override
    {
        if (Actions_.empty()) {
            return OKFuture;
        }

        auto transaction = Transaction_.Lock();
        if (!transaction) {
            return MakeFuture(TError(NYT::EErrorCode::Canceled, "Transaction destroyed"));
        }

        YT_LOG_DEBUG("Sending transaction actions (ActionCount: %v)",
            Actions_.size());

        TFuture<void> future;
        switch (TypeFromId(CellId_)) {
            case EObjectType::TabletCell:
                future = SendTabletActions(transaction);
                break;
            case EObjectType::MasterCell:
                future = SendMasterActions(transaction);
                break;
            case EObjectType::ChaosCell:
                future = SendChaosActions(transaction);
                break;
            default:
                YT_ABORT();
        }

        return future.Apply(BIND(&TCellCommitSession::OnResponse, MakeStrong(this)));
    }

private:
    const IRegisterTransactionActionsRequestFactoryPtr RequestFactory_;
    const TWeakPtr<TTransaction> Transaction_;
    const TCellId CellId_;

    std::unique_ptr<TTransactionSignatureGenerator> PrepareSignatureGenerator_;
    TTransactionSignatureGenerator CommitSignatureGenerator_;

    const TLogger Logger;

    std::vector<TTransactionActionData> Actions_;

    TFuture<void> SendTabletActions(const TTransactionPtr& owner)
    {
        auto req = RequestFactory_->CreateRegisterTransactionActionsTabletCellRequest(CellId_);
        ToProto(req->mutable_transaction_id(), owner->GetId());
        req->set_transaction_start_timestamp(owner->GetStartTimestamp());
        req->set_transaction_timeout(ToProto(owner->GetTimeout()));
        req->set_prepare_signature(PrepareSignatureGenerator_->GenerateSignature());
        req->set_commit_signature(CommitSignatureGenerator_.GenerateSignature());
        ToProto(req->mutable_actions(), Actions_);
        return req->Invoke().As<void>();
    }

    TFuture<void> SendMasterActions(const TTransactionPtr& owner)
    {
        auto req = RequestFactory_->CreateRegisterTransactionActionsMasterCellRequest(CellId_);
        ToProto(req->mutable_transaction_id(), owner->GetId());
        ToProto(req->mutable_actions(), Actions_);
        return req->Invoke().As<void>();
    }

    TFuture<void> SendChaosActions(const TTransactionPtr& owner)
    {
        auto req = RequestFactory_->CreateRegisterTransactionActionsChaosCellRequest(CellId_);
        ToProto(req->mutable_transaction_id(), owner->GetId());
        req->set_transaction_start_timestamp(owner->GetStartTimestamp());
        req->set_transaction_timeout(ToProto(owner->GetTimeout()));
        req->set_signature(PrepareSignatureGenerator_->GenerateSignature());
        ToProto(req->mutable_actions(), Actions_);
        return req->Invoke().As<void>();
    }

    void OnResponse(const TError& result)
    {
        if (!result.IsOK()) {
            auto error = TError("Error sending transaction actions")
                << TErrorAttribute("cell_id", CellId_)
                << result;
            YT_LOG_DEBUG(error);
            THROW_ERROR(error);
        }

        YT_LOG_DEBUG("Transaction actions sent successfully");
    }
};

////////////////////////////////////////////////////////////////////////////////

ICellCommitSessionPtr CreateCellCommitSession(
    IRegisterTransactionActionsRequestFactoryPtr requestFactory,
    TWeakPtr<TTransaction> transaction,
    TCellId cellId,
    TLogger logger,
    bool useUniformPrepareSignatures)
{
    return New<TCellCommitSession>(
        std::move(requestFactory),
        std::move(transaction),
        cellId,
        std::move(logger),
        useUniformPrepareSignatures);
}

////////////////////////////////////////////////////////////////////////////////

class TCellCommitSessionProvider
    : public ICellCommitSessionProvider
{
public:
    TCellCommitSessionProvider(
        IRegisterTransactionActionsRequestFactoryPtr requestFactory,
        TWeakPtr<TTransaction> transaction,
        TLogger logger,
        bool useUniformPrepareSignatures)
        : RequestFactory_(std::move(requestFactory))
        , Transaction_(std::move(transaction))
        , Logger(std::move(logger))
        , UseUniformPrepareSignatures_(useUniformPrepareSignatures)
    { }

    ICellCommitSessionPtr GetCellCommitSession(TCellId cellId) override
    {
        auto guard = Guard(Lock_);

        return GetOrCrash(CellIdToCommitSession_, cellId);
    }

    ICellCommitSessionPtr GetOrCreateCellCommitSession(TCellId cellId) override
    {
        auto guard = Guard(Lock_);

        auto cellIt = CellIdToCommitSession_.find(cellId);
        if (cellIt == CellIdToCommitSession_.end()) {
            auto session = CreateCellCommitSession(
                RequestFactory_,
                Transaction_,
                cellId,
                Logger,
                UseUniformPrepareSignatures_);
            EmplaceOrCrash(CellIdToCommitSession_, cellId, session);
            if (auto transaction = Transaction_.Lock()) {
                transaction->RegisterParticipant(cellId);
            }
            return session;
        } else {
            return cellIt->second;
        }
    }

    std::vector<TCellId> GetParticipantCellIds() const override
    {
        auto guard = Guard(Lock_);

        return GetKeys(CellIdToCommitSession_);
    }

    TFuture<void> InvokeAll() override
    {
        auto guard = Guard(Lock_);

        std::vector<TFuture<void>> futures;
        futures.reserve(CellIdToCommitSession_.size());
        for (const auto& [cellId, session] : CellIdToCommitSession_) {
            futures.push_back(session->Invoke());
        }

        return AllSucceeded(std::move(futures));
    }

    THashMap<TCellId, ICellCommitSessionPtr> GetCellCommitSessions() override
    {
        auto guard = Guard(Lock_);
        return CellIdToCommitSession_;
    }

private:
    const IRegisterTransactionActionsRequestFactoryPtr RequestFactory_;
    const TWeakPtr<TTransaction> Transaction_;

    const TLogger Logger;
    const bool UseUniformPrepareSignatures_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    THashMap<TCellId, ICellCommitSessionPtr> CellIdToCommitSession_;
};

////////////////////////////////////////////////////////////////////////////////

ICellCommitSessionProviderPtr CreateCellCommitSessionProvider(
    IRegisterTransactionActionsRequestFactoryPtr requestFactory,
    TWeakPtr<TTransaction> transaction,
    NLogging::TLogger logger,
    bool useUniformPrepareSignatures)
{
    return New<TCellCommitSessionProvider>(
        std::move(requestFactory),
        std::move(transaction),
        std::move(logger),
        useUniformPrepareSignatures);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
