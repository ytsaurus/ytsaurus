#include "stdafx.h"
#include "cypress_integration.h"
#include "transaction.h"

#include <core/misc/string.h>

#include <core/ytree/virtual.h>

#include <server/cypress_server/virtual.h>

#include <server/cell_master/bootstrap.h>

#include <server/transaction_server/transaction_manager.h>

#include <server/object_server/object_manager.h>

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

    return CreateVirtualTypeHandler(
        bootstrap,
        EObjectType::TransactionMap,
        BIND([=] (INodePtr owningNode) -> IYPathServicePtr {
            return CreateVirtualObjectMap(
                bootstrap,
                bootstrap->GetTransactionManager()->Transactions(),
                owningNode);
        }),
        EVirtualNodeOptions::RequireLeader | EVirtualNodeOptions::RedirectSelf);
}

////////////////////////////////////////////////////////////////////////////////

class TVirtualTopmostTransactionMap
    : public TVirtualMapBase
{
public:
    TVirtualTopmostTransactionMap(TBootstrap* bootstrap, INodePtr owningNode)
        : TVirtualMapBase(owningNode)
        , Bootstrap_(bootstrap)
    { }

private:
    TBootstrap* const Bootstrap_;

    virtual std::vector<Stroka> GetKeys(i64 sizeLimit) const override
    {
        auto transactionManager = Bootstrap_->GetTransactionManager();
        auto ids = ToObjectIds(transactionManager->TopmostTransactions(), sizeLimit);
        // NB: No size limit is needed here.
        return ConvertToStrings(ids);
    }

    virtual i64 GetSize() const override
    {
        auto transactionManager = Bootstrap_->GetTransactionManager();
        return transactionManager->TopmostTransactions().size();
    }

    virtual IYPathServicePtr FindItemService(const TStringBuf& key) const override
    {
        auto id = TTransactionId::FromString(key);

        auto transactionManager = Bootstrap_->GetTransactionManager();
        auto* transaction = transactionManager->FindTransaction(id);
        if (!IsObjectAlive(transaction)) {
            return nullptr;
        }

        if (transaction->GetParent()) {
            return nullptr;
        }

        auto objectManager = Bootstrap_->GetObjectManager();
        return objectManager->GetProxy(transaction);
    }
};

INodeTypeHandlerPtr CreateTopmostTransactionMapTypeHandler(TBootstrap* bootstrap)
{
    YCHECK(bootstrap);

    return CreateVirtualTypeHandler(
        bootstrap,
        EObjectType::TopmostTransactionMap,
        BIND([=] (INodePtr owningNode) -> IYPathServicePtr {
            return New<TVirtualTopmostTransactionMap>(bootstrap, owningNode);
        }),
        EVirtualNodeOptions::RequireLeader | EVirtualNodeOptions::RedirectSelf);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionServer
} // namespace NYT
