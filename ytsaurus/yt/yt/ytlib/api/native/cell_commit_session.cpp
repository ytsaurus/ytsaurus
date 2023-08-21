#include "cell_commit_session.h"

#include "client.h"

#include <yt/yt/ytlib/chaos_client/coordinator_service_proxy.h>

#include <yt/yt/ytlib/tablet_client/tablet_service_proxy.h>

#include <yt/yt/ytlib/transaction_client/transaction_manager.h>
#include <yt/yt/ytlib/transaction_client/transaction_service_proxy.h>

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
        IClientPtr client,
        TWeakPtr<TTransaction> transaction,
        TCellId cellId,
        TLogger logger)
        : Client_(std::move(client))
        , Transaction_(std::move(transaction))
        , CellId_(cellId)
        , PrepareSignatureGenerator_(/*targetSignature*/ FinalTransactionSignature)
        , CommitSignatureGenerator_(/*targetSignature*/ FinalTransactionSignature)
        , Logger(logger.WithTag("CellId: %v", cellId))
    { }

    TTransactionSignatureGenerator* GetPrepareSignatureGenerator() override
    {
        return &PrepareSignatureGenerator_;
    }

    TTransactionSignatureGenerator* GetCommitSignatureGenerator() override
    {
        return &CommitSignatureGenerator_;
    }

    virtual void RegisterAction(NTransactionClient::TTransactionActionData data) override
    {
        if (Actions_.empty()) {
            PrepareSignatureGenerator_.RegisterRequest();
            CommitSignatureGenerator_.RegisterRequest();
        }
        Actions_.push_back(data);
    }

    virtual TFuture<void> Invoke() override
    {
        if (Actions_.empty()) {
            return VoidFuture;
        }

        auto transaction = Transaction_.Lock();
        if (!transaction) {
            return MakeFuture(TError(NYT::EErrorCode::Canceled, "Transaction destroyed"));
        }

        auto channel = Client_->GetCellChannelOrThrow(CellId_);

        YT_LOG_DEBUG("Sending transaction actions (ActionCount: %v)",
            Actions_.size());

        TFuture<void> future;
        switch (TypeFromId(CellId_)) {
            case EObjectType::TabletCell:
                future = SendTabletActions(transaction, channel);
                break;
            case EObjectType::MasterCell:
                future = SendMasterActions(transaction, channel);
                break;
            case EObjectType::ChaosCell:
                future = SendChaosActions(transaction, channel);
                break;
            default:
                YT_ABORT();
        }

        return future.Apply(BIND(&TCellCommitSession::OnResponse, MakeStrong(this)));
    }

private:
    const IClientPtr Client_;
    const TWeakPtr<TTransaction> Transaction_;
    const TCellId CellId_;

    TTransactionSignatureGenerator PrepareSignatureGenerator_;
    TTransactionSignatureGenerator CommitSignatureGenerator_;

    const TLogger Logger;

    std::vector<TTransactionActionData> Actions_;

    TFuture<void> SendTabletActions(const TTransactionPtr& owner, const IChannelPtr& channel)
    {
        NTabletClient::TTabletServiceProxy proxy(channel);
        auto req = proxy.RegisterTransactionActions();
        req->SetResponseHeavy(true);
        ToProto(req->mutable_transaction_id(), owner->GetId());
        req->set_transaction_start_timestamp(owner->GetStartTimestamp());
        req->set_transaction_timeout(ToProto<i64>(owner->GetTimeout()));
        req->set_signature(PrepareSignatureGenerator_.GenerateSignature());
        ToProto(req->mutable_actions(), Actions_);
        return req->Invoke().As<void>();
    }

    TFuture<void> SendMasterActions(const TTransactionPtr& owner, const IChannelPtr& channel)
    {
        TTransactionServiceProxy proxy(channel);
        auto req = proxy.RegisterTransactionActions();
        req->SetResponseHeavy(true);
        ToProto(req->mutable_transaction_id(), owner->GetId());
        ToProto(req->mutable_actions(), Actions_);
        return req->Invoke().As<void>();
    }

    TFuture<void> SendChaosActions(const TTransactionPtr& owner, const IChannelPtr& channel)
    {
        NChaosClient::TCoordinatorServiceProxy proxy(channel);
        auto req = proxy.RegisterTransactionActions();
        ToProto(req->mutable_transaction_id(), owner->GetId());
        req->set_transaction_start_timestamp(owner->GetStartTimestamp());
        req->set_transaction_timeout(ToProto<i64>(owner->GetTimeout()));
        req->set_signature(PrepareSignatureGenerator_.GenerateSignature());
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
    IClientPtr client,
    TWeakPtr<TTransaction> transaction,
    TCellId cellId,
    TLogger logger)
{
    return New<TCellCommitSession>(
        std::move(client),
        std::move(transaction),
        cellId,
        std::move(logger));
}

////////////////////////////////////////////////////////////////////////////////

class TCellCommitSessionProvider
    : public ICellCommitSessionProvider
{
public:
    TCellCommitSessionProvider(
        IClientPtr client,
        TWeakPtr<TTransaction> transaction,
        TLogger logger)
        : Client_(std::move(client))
        , Transaction_(std::move(transaction))
        , Logger(std::move(logger))
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
                Client_,
                Transaction_,
                cellId,
                Logger);
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

private:
    const IClientPtr Client_;
    const TWeakPtr<TTransaction> Transaction_;

    const TLogger Logger;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    THashMap<TCellId, ICellCommitSessionPtr> CellIdToCommitSession_;
};

////////////////////////////////////////////////////////////////////////////////

ICellCommitSessionProviderPtr CreateCellCommitSessionProvider(
    IClientPtr client,
    TWeakPtr<NTransactionClient::TTransaction> transaction,
    NLogging::TLogger logger)
{
    return New<TCellCommitSessionProvider>(
        std::move(client),
        std::move(transaction),
        std::move(logger));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
