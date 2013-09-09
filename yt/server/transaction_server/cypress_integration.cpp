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

class TVirtualTransactionMap
    : public TVirtualMapBase
{
public:
    TVirtualTransactionMap(TBootstrap* bootstrap, EObjectType type)
        : Bootstrap(bootstrap)
        , Type(type)
    { }

private:
    TBootstrap* Bootstrap;
    EObjectType Type;

    virtual std::vector<Stroka> GetKeys(size_t sizeLimit) const override
    {
        auto transactionManager = Bootstrap->GetTransactionManager();
        std::vector<TObjectId> ids;
        switch (Type) {
            case EObjectType::TransactionMap:
                ids = ToObjectIds(transactionManager->GetTransactions(sizeLimit));
                break;
            case EObjectType::TopmostTransactionMap:
                ids = ToObjectIds(transactionManager->TopmostTransactions(), sizeLimit);
                break;
            default:
                YUNREACHABLE();
        }
        // NB: No size limit is needed here.
        return ConvertToStrings(ids);
    }

    virtual size_t GetSize() const override
    {
        auto transactionManager = Bootstrap->GetTransactionManager();
        switch (Type) {
            case EObjectType::TransactionMap:
                return transactionManager->GetTransactionCount();
            case EObjectType::TopmostTransactionMap:
                return transactionManager->TopmostTransactions().size();
            default:
                YUNREACHABLE();
        }
    }

    virtual IYPathServicePtr FindItemService(const TStringBuf& key) const override
    {
        auto id = TTransactionId::FromString(key);

        auto transactionManager = Bootstrap->GetTransactionManager();
        auto* transaction = transactionManager->FindTransaction(id);
        if (!IsObjectAlive(transaction)) {
            return nullptr;
        }

        if (Type == EObjectType::TopmostTransactionMap && transaction->GetParent()) {
            return nullptr;
        }

        auto objectManager = Bootstrap->GetObjectManager();
        return objectManager->GetProxy(transaction);
    }
};

INodeTypeHandlerPtr CreateTransactionMapTypeHandler(TBootstrap* bootstrap, EObjectType type)
{
    YCHECK(bootstrap);

    auto service = New<TVirtualTransactionMap>(bootstrap, type);
    return CreateVirtualTypeHandler(
        bootstrap,
        type,
        service,
        EVirtualNodeOptions(EVirtualNodeOptions::RequireLeader | EVirtualNodeOptions::RedirectSelf));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionServer
} // namespace NYT
