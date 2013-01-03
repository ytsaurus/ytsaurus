#include "stdafx.h"
#include "cypress_integration.h"
#include "transaction.h"

#include <ytlib/misc/string.h>

#include <ytlib/ytree/virtual.h>

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

class TVirtualTransactionMap
    : public TVirtualMapBase
{
public:
    explicit TVirtualTransactionMap(TBootstrap* bootstrap)
        : Bootstrap(bootstrap)
    { }

private:
    TBootstrap* Bootstrap;

    virtual std::vector<Stroka> GetKeys(size_t sizeLimit) const override
    {
        auto transactionManager = Bootstrap->GetTransactionManager();
        auto ids = ToObjectIds(transactionManager->GetTransactions(sizeLimit));
        // NB: No size limit is needed here.
        return ConvertToStrings(ids.begin(), ids.end());
    }

    virtual size_t GetSize() const override
    {
        auto transactionManager = Bootstrap->GetTransactionManager();
        return transactionManager->GetTransactionCount();
    }

    virtual IYPathServicePtr FindItemService(const TStringBuf& key) const override
    {
        auto transactionManager = Bootstrap->GetTransactionManager();
        auto id = TTransactionId::FromString(key);
        auto* transaction = transactionManager->FindTransaction(id);
        if (!transaction || !transaction->IsAlive()) {
            return nullptr;
        }

        auto objectManager = Bootstrap->GetObjectManager();
        return objectManager->GetProxy(transaction);
    }
};

INodeTypeHandlerPtr CreateTransactionMapTypeHandler(TBootstrap* bootstrap)
{
    YCHECK(bootstrap);

    return CreateVirtualTypeHandler(
        bootstrap,
        EObjectType::TransactionMap,
        New<TVirtualTransactionMap>(bootstrap));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionServer
} // namespace NYT
