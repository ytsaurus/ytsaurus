#include "client_impl.h"
#include "transaction.h"

#include <yt/ytlib/transaction_client/transaction_manager.h>

namespace NYT::NApi::NNative {

using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

TFuture<ITransactionPtr> TClient::StartNativeTransaction(
    ETransactionType type,
    const TTransactionStartOptions& options)
{
    return TransactionManager_->Start(type, options).Apply(
        BIND([=, this_ = MakeStrong(this)] (const NTransactionClient::TTransactionPtr& transaction) {
            auto wrappedTransaction = CreateTransaction(this_, transaction, Logger);
            return wrappedTransaction;
        }));
}

ITransactionPtr TClient::AttachNativeTransaction(
    TTransactionId transactionId,
    const TTransactionAttachOptions& options)
{
    auto wrappedTransaction = TransactionManager_->Attach(transactionId, options);
    return CreateTransaction(this, std::move(wrappedTransaction), Logger);
}

TFuture<NApi::ITransactionPtr> TClient::StartTransaction(
    ETransactionType type,
    const TTransactionStartOptions& options)
{
    return StartNativeTransaction(type, options).As<NApi::ITransactionPtr>();
}

NApi::ITransactionPtr TClient::AttachTransaction(
    TTransactionId transactionId,
    const TTransactionAttachOptions& options)
{
    return AttachNativeTransaction(transactionId, options);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
