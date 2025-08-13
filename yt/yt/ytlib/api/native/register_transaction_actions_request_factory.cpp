#include "register_transaction_actions_request_factory.h"

#include "private.h"

#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/transaction_client/transaction_manager.h>

#include <yt/yt/core/rpc/retrying_channel.h>

namespace NYT::NApi::NNative {

///////////////////////////////////////////////////////////////////////////////

using namespace NChaosClient;
using namespace NCypressClient;
using namespace NHiveClient;
using namespace NLogging;
using namespace NObjectClient;
using namespace NTabletClient;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

class TRegisterTransactionActionsRequestFactory
    : public IRegisterTransactionActionsRequestFactory
{
public:
    TRegisterTransactionActionsRequestFactory(
        IClientPtr client,
        TLogger logger)
        : Client_(std::move(client))
        , Logger(std::move(logger))
    { }

    TTabletServiceProxy::TReqRegisterTransactionActionsPtr
    CreateRegisterTransactionActionsTabletCellRequest(TCellId cellId) override
    {
        ValidateCellType(cellId, EObjectType::TabletCell);

        auto channel = Client_->GetCellChannelOrThrow(cellId);

        TTabletServiceProxy proxy(channel);
        auto req = proxy.RegisterTransactionActions();
        req->SetResponseHeavy(true);
        req->SetTimeout(Client_->GetNativeConnection()->GetConfig()->DefaultRegisterTransactionActionsTimeout);
        return req;
    }

    TTransactionServiceProxy::TReqRegisterTransactionActionsPtr
    CreateRegisterTransactionActionsMasterCellRequest(TCellId cellId) override
    {
        ValidateCellType(cellId, EObjectType::MasterCell);

        auto channel = Client_->GetCellChannelOrThrow(cellId);

        const auto& config = Client_->GetNativeConnection()->GetConfig()->TransactionManager;
        auto retryingChannel = CreateRetryingChannel(config, std::move(channel));

        TTransactionServiceProxy proxy(retryingChannel);
        auto req = proxy.RegisterTransactionActions();
        req->SetResponseHeavy(true);
        req->SetTimeout(Client_->GetNativeConnection()->GetConfig()->DefaultRegisterTransactionActionsTimeout);
        GenerateMutationId(req);
        req->SetRetry(true);

        return req;
    }

    TCoordinatorServiceProxy::TReqRegisterTransactionActionsPtr
    CreateRegisterTransactionActionsChaosCellRequest(TCellId cellId) override
    {
        ValidateCellType(cellId, EObjectType::ChaosCell);

        auto channel = Client_->GetCellChannelOrThrow(cellId);

        NChaosClient::TCoordinatorServiceProxy proxy(channel);
        auto req = proxy.RegisterTransactionActions();
        req->SetTimeout(Client_->GetNativeConnection()->GetConfig()->DefaultRegisterTransactionActionsTimeout);

        return req;
    }

private:
    IClientPtr Client_;
    TLogger Logger;

    void ValidateCellType(TCellId cellId, EObjectType expectedCellType)
    {
        auto cellType = TypeFromId(cellId);
        YT_LOG_ALERT_AND_THROW_UNLESS(cellType == expectedCellType,
            "Cell type mismatch while registering transaction actions (CellId: %v, CellType: %v, ExpectedCellType: %v)",
            cellId,
            cellType,
            expectedCellType);
    }
};

IRegisterTransactionActionsRequestFactoryPtr CreateRegisterTransactionActionsRequestFactory(
    IClientPtr client,
    TLogger logger)
{
    return New<TRegisterTransactionActionsRequestFactory>(std::move(client), std::move(logger));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
