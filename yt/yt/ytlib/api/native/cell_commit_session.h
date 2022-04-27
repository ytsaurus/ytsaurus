#include "public.h"

#include "transaction_helpers.h"

#include <yt/yt/ytlib/transaction_client/action.h>
#include <yt/yt/ytlib/transaction_client/transaction_manager.h>

#include <yt/yt/core/logging/log.h>

namespace NYT::NApi::NNative {

////////////////////////////////////////////////////////////////////////////////

struct ICellCommitSession
    : public TRefCounted
{
    virtual TTransactionSignatureGenerator* GetPrepareSignatureGenerator() = 0;
    virtual TTransactionSignatureGenerator* GetCommitSignatureGenerator() = 0;

    virtual void RegisterAction(NTransactionClient::TTransactionActionData data) = 0;

    virtual TFuture<void> Invoke() = 0;
};

DEFINE_REFCOUNTED_TYPE(ICellCommitSession)

////////////////////////////////////////////////////////////////////////////////

ICellCommitSessionPtr CreateCellCommitSession(
    IClientPtr client,
    TWeakPtr<NTransactionClient::TTransaction> transaction,
    NHiveClient::TCellId cellId,
    NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

struct ICellCommitSessionProvider
    : public TRefCounted
{
    virtual ICellCommitSessionPtr GetCellCommitSession(NHiveClient::TCellId cellId) = 0;
    virtual ICellCommitSessionPtr GetOrCreateCellCommitSession(NHiveClient::TCellId cellId) = 0;

    virtual std::vector<NHiveClient::TCellId> GetParticipantCellIds() const = 0;

    virtual TFuture<void> InvokeAll() = 0;
};

DEFINE_REFCOUNTED_TYPE(ICellCommitSessionProvider)

////////////////////////////////////////////////////////////////////////////////

ICellCommitSessionProviderPtr CreateCellCommitSessionProvider(
    IClientPtr client,
    TWeakPtr<NTransactionClient::TTransaction> transaction,
    NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
