#include "cypress_integration.h"
#include "transaction.h"

#include <yt/server/cell_master/bootstrap.h>

#include <yt/server/cypress_server/virtual.h>

#include <yt/server/object_server/object_manager.h>

#include <yt/server/transaction_server/transaction_manager.h>

#include <yt/ytlib/object_client/helpers.h>

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

    return CreateVirtualTypeHandler(
        bootstrap,
        EObjectType::TransactionMap,
        BIND([=] (INodePtr owningNode) -> IYPathServicePtr {
            return CreateVirtualObjectMap(
                bootstrap,
                bootstrap->GetTransactionManager()->Transactions(),
                owningNode);
        }),
        EVirtualNodeOptions::RedirectSelf);
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

    virtual std::vector<TString> GetKeys(i64 sizeLimit) const override
    {
        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        auto ids = ToObjectIds(transactionManager->TopmostTransactions(), sizeLimit);
        // NB: No size limit is needed here.
        return ConvertToStrings(ids);
    }

    virtual i64 GetSize() const override
    {
        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        return transactionManager->TopmostTransactions().size();
    }

    virtual IYPathServicePtr FindItemService(TStringBuf key) const override
    {
        auto id = TTransactionId::FromString(key);

        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        auto* transaction = transactionManager->FindTransaction(id);
        if (!IsObjectAlive(transaction)) {
            return nullptr;
        }

        if (transaction->GetParent()) {
            return nullptr;
        }

        const auto& objectManager = Bootstrap_->GetObjectManager();
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
        EVirtualNodeOptions::RedirectSelf);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionServer
} // namespace NYT
