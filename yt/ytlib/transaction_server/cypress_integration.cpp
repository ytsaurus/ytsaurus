#include "stdafx.h"
#include "cypress_integration.h"

#include "../cypress/virtual.h"
#include "../ytree/virtual.h"
#include "../ytree/fluent.h"


namespace NYT {
namespace NTransaction {

using namespace NYTree;
using namespace NCypress;

////////////////////////////////////////////////////////////////////////////////

class TVirtualTransactionMap
    : public TVirtualMapBase
{
public:
    TVirtualTransactionMap(TTransactionManager* transactionManager)
        : TransactionManager(transactionManager)
    { }

private:
    TTransactionManager::TPtr TransactionManager;

    virtual yvector<Stroka> GetKeys()
    {
        auto ids = TransactionManager->GetTransactionIds();
        yvector<Stroka> keys;
        keys.reserve(ids.ysize());
        FOREACH(const auto& id, ids) {
            keys.push_back(id.ToString());
        }
        return keys;
    }

    virtual IYPathService::TPtr GetItemService(const Stroka& key)
    {
        auto id = TChunkId::FromString(key);
        auto* transaction = TransactionManager->FindTransaction(id);
        if (transaction == NULL) {
            return NULL;
        }

        return IYPathService::FromProducer(~FromFunctor([=] (IYsonConsumer* consumer)
            {
                // TODO: add vectors
                BuildYsonFluently(consumer)
                    .BeginMap()
                    .EndMap();
            }));
    }
};

NCypress::INodeTypeHandler::TPtr CreateTransactionMapTypeHandler(
    NCypress::TCypressManager* cypressManager,
    TTransactionManager* transactionManager)
{
    YASSERT(cypressManager != NULL);
    YASSERT(transactionManager != NULL);

    return CreateVirtualTypeHandler(
        cypressManager,
        ERuntimeNodeType::TransactionMap,
        // TODO: extract type name
        "transaction_map",
        ~New<TVirtualTransactionMap>(transactionManager));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransaction
} // namespace NYT
