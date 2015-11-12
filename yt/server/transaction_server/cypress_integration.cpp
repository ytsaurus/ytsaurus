#include "cypress_integration.h"
#include "transaction.h"

#include <yt/server/cell_master/bootstrap.h>

#include <yt/server/cypress_server/virtual.h>

#include <yt/server/object_server/object_manager.h>

#include <yt/server/transaction_server/transaction_manager.h>

#include <yt/core/misc/common.h>
#include <yt/core/misc/string.h>

#include <yt/core/ytree/virtual.h>

namespace NYT {
namespace NTransactionServer {

using namespace NYTree;
using namespace NCypressServer;
using namespace NCellMaster;
using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

INodeTypeHandlerPtr CreateTransactionMapTypeHandler(TBootstrap* bootstrap)
{
    YCHECK(bootstrap);

    auto service = CreateVirtualObjectMap(
        bootstrap,
        bootstrap->GetTransactionManager()->Transactions());
    return CreateVirtualTypeHandler(
        bootstrap,
        EObjectType::TransactionMap,
        service,
        EVirtualNodeOptions::RequireLeader | EVirtualNodeOptions::RedirectSelf);
}

////////////////////////////////////////////////////////////////////////////////

class TVirtualTopmostTransactionMap
    : public TVirtualMapBase
{
public:
    explicit TVirtualTopmostTransactionMap(TBootstrap* bootstrap)
        : Bootstrap(bootstrap)
    { }

private:
    TBootstrap* Bootstrap;

    virtual std::vector<Stroka> GetKeys(size_t sizeLimit) const override
    {
        auto transactionManager = Bootstrap->GetTransactionManager();
        auto ids = ToObjectIds(transactionManager->TopmostTransactions(), sizeLimit);
        // NB: No size limit is needed here.
        return ConvertToStrings(ids);
    }

    virtual size_t GetSize() const override
    {
        auto transactionManager = Bootstrap->GetTransactionManager();
        return transactionManager->TopmostTransactions().size();
    }

    virtual IYPathServicePtr FindItemService(const TStringBuf& key) const override
    {
        auto id = TTransactionId::FromString(key);

        auto transactionManager = Bootstrap->GetTransactionManager();
        auto* transaction = transactionManager->FindTransaction(id);
        if (!IsObjectAlive(transaction)) {
            return nullptr;
        }

        if (transaction->GetParent()) {
            return nullptr;
        }

        auto objectManager = Bootstrap->GetObjectManager();
        return objectManager->GetProxy(transaction);
    }
};

INodeTypeHandlerPtr CreateTopmostTransactionMapTypeHandler(TBootstrap* bootstrap)
{
    YCHECK(bootstrap);

    auto service = New<TVirtualTopmostTransactionMap>(bootstrap);
    return CreateVirtualTypeHandler(
        bootstrap,
        EObjectType::TopmostTransactionMap,
        service,
        EVirtualNodeOptions::RequireLeader | EVirtualNodeOptions::RedirectSelf);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionServer
} // namespace NYT
