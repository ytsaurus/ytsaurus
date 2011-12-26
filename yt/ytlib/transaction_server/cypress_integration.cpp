#include "stdafx.h"
#include "cypress_integration.h"

#include "../cypress/virtual.h"
#include "../ytree/virtual.h"
#include "../ytree/fluent.h"
#include "../misc/string.h"

namespace NYT {
namespace NTransactionServer {

using namespace NYTree;
using namespace NCypress;
using NChunkClient::TChunkId;

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

    virtual yvector<Stroka> GetKeys() const
    {
        const auto& ids = TransactionManager->GetTransactionIds();
        return ConvertToStrings(ids.begin(), ids.end(), ids.size());
    }

    virtual IYPathService::TPtr GetItemService(const Stroka& key) const
    {
        auto id = TChunkId::FromString(key);
        auto* transaction = TransactionManager->FindTransaction(id);
        if (!transaction) {
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
    YASSERT(cypressManager);
    YASSERT(transactionManager);

    return CreateVirtualTypeHandler(
        cypressManager,
        ERuntimeNodeType::TransactionMap,
        // TODO: extract type name
        "transaction_map",
        ~New<TVirtualTransactionMap>(transactionManager));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionServer
} // namespace NYT
