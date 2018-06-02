#include "connection.h"
#include "transaction_participant.h"

#include <yt/ytlib/hive/cell_directory.h>
#include <yt/ytlib/hive/cell_directory_synchronizer.h>
#include <yt/ytlib/hive/transaction_participant_service_proxy.h>

#include <yt/client/hive/transaction_participant.h>

#include <yt/client/api/connection.h>

namespace NYT {
namespace NApi {
namespace NNative {

using namespace NHiveClient;
using namespace NTransactionClient;
using namespace NElection;

////////////////////////////////////////////////////////////////////////////////

class TTransactionParticipant
    : public ITransactionParticipant
{
public:
    TTransactionParticipant(
        TCellDirectoryPtr cellDirectory,
        TCellDirectorySynchronizerPtr cellDirectorySynchronizer,
        ITimestampProviderPtr timestampProvider,
        IConnectionPtr connection,
        const TCellId& cellId,
        const TTransactionParticipantOptions& options)
        : CellDirectory_(std::move(cellDirectory))
        , CellDirectorySynchronizer_(std::move(cellDirectorySynchronizer))
        , TimestampProvider_(std::move(timestampProvider))
        , Connection_(std::move(connection))
        , CellId_(cellId)
        , Options_(options)
    { }

    virtual const TCellId& GetCellId() const override
    {
        return CellId_;
    }

    virtual const ITimestampProviderPtr& GetTimestampProvider() const override
    {
        return TimestampProvider_;
    }

    virtual ETransactionParticipantState GetState() const override
    {
        if (CellDirectory_->IsCellUnregistered(CellId_)) {
            return ETransactionParticipantState::Unregistered;
        }
        if (!Connection_) {
            return ETransactionParticipantState::Valid;
        }
        if (Connection_->IsTerminated()) {
            return ETransactionParticipantState::Invalid;
        }
        return ETransactionParticipantState::Valid;
    }

    virtual TFuture<void> PrepareTransaction(const TTransactionId& transactionId, TTimestamp prepareTimestamp, TString user) override
    {
        return SendRequest<TTransactionParticipantServiceProxy::TReqPrepareTransaction>(
            [=] (TTransactionParticipantServiceProxy* proxy) {
                auto req = proxy->PrepareTransaction();
                PrepareRequest(req);
                ToProto(req->mutable_transaction_id(), transactionId);
                req->set_prepare_timestamp(prepareTimestamp);
                req->SetUser(user);
                return req;
            });
    }

    virtual TFuture<void> CommitTransaction(const TTransactionId& transactionId, TTimestamp commitTimestamp) override
    {
        return SendRequest<TTransactionParticipantServiceProxy::TReqCommitTransaction>(
            [=] (TTransactionParticipantServiceProxy* proxy) {
                auto req = proxy->CommitTransaction();
                PrepareRequest(req);
                ToProto(req->mutable_transaction_id(), transactionId);
                req->set_commit_timestamp(commitTimestamp);
                return req;
            });
    }

    virtual TFuture<void> AbortTransaction(const TTransactionId& transactionId) override
    {
        return SendRequest<TTransactionParticipantServiceProxy::TReqAbortTransaction>(
            [=] (TTransactionParticipantServiceProxy* proxy) {
                auto req = proxy->AbortTransaction();
                req->SetHeavy(true);
                PrepareRequest(req);
                ToProto(req->mutable_transaction_id(), transactionId);
                return req;
            });
    }

private:
    const TCellDirectoryPtr CellDirectory_;
    const TCellDirectorySynchronizerPtr CellDirectorySynchronizer_;
    const ITimestampProviderPtr TimestampProvider_;
    const IConnectionPtr Connection_;
    const TCellId CellId_;
    const TTransactionParticipantOptions Options_;


    template <class TRequest>
    TFuture<void> SendRequest(std::function<TIntrusivePtr<TRequest>(TTransactionParticipantServiceProxy*)> builder)
    {
        return GetChannel().Apply(BIND([=] (const NRpc::IChannelPtr& channel) {
            TTransactionParticipantServiceProxy proxy(channel);
            auto req = builder(&proxy);
            return req->Invoke().template As<void>();
        }));
    }

    void PrepareRequest(const TIntrusivePtr<NRpc::TClientRequest>& request)
    {
        request->SetTimeout(Options_.RpcTimeout);
    }

    TFuture<NRpc::IChannelPtr> GetChannel()
    {
        auto channel = CellDirectory_->FindChannel(CellId_);
        if (channel) {
            return MakeFuture(channel);
        }
        if (!CellDirectorySynchronizer_) {
            return MakeNoChannelError();
        }
        return CellDirectorySynchronizer_->Sync().Apply(BIND([=, this_ = MakeStrong(this)] () {
            auto channel = CellDirectory_->FindChannel(CellId_);
            if (channel) {
                return MakeFuture(channel);
            }
            return MakeNoChannelError();
        }));
    }

    TFuture<NRpc::IChannelPtr> MakeNoChannelError()
    {
        return MakeFuture<NRpc::IChannelPtr>(TError(
            NRpc::EErrorCode::Unavailable,
            "No such participant cell %v",
            CellId_));
    }
};

ITransactionParticipantPtr CreateTransactionParticipant(
    TCellDirectoryPtr cellDirectory,
    TCellDirectorySynchronizerPtr cellDirectorySynchronizer,
    ITimestampProviderPtr timestampProvider,
    IConnectionPtr connection,
    const TCellId& cellId,
    const TTransactionParticipantOptions& options)
{
    return New<TTransactionParticipant>(
        std::move(cellDirectory),
        std::move(cellDirectorySynchronizer),
        std::move(timestampProvider),
        std::move(connection),
        cellId,
        options);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NNative
} // namespace NApi
} // namespace NYT
