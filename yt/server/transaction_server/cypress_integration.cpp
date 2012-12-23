#include "stdafx.h"
#include "cypress_integration.h"

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
        auto ids = transactionManager->GetTransactionIds(sizeLimit);
        return ConvertToStrings(ids.begin(), ids.end(), sizeLimit);
    }

    virtual size_t GetSize() const override
    {
        auto transactionManager = Bootstrap->GetTransactionManager();
        return transactionManager->GetTransactionCount();
    }

    virtual IYPathServicePtr GetItemService(const TStringBuf& key) const override
    {
        auto id = TTransactionId::FromString(key);
        if (TypeFromId(id) != EObjectType::Transaction) {
            return NULL;
        }
        auto objectManager = Bootstrap->GetObjectManager();
        return objectManager->FindProxy(id);
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
