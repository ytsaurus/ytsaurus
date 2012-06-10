#include "stdafx.h"
#include "cypress_integration.h"

#include <ytlib/cypress/virtual.h>
#include <ytlib/ytree/virtual.h>
#include <ytlib/ytree/fluent.h>
#include <ytlib/misc/string.h>
#include <ytlib/cell_master/bootstrap.h>
#include <ytlib/transaction_server/transaction_manager.h>

namespace NYT {
namespace NTransactionServer {

using namespace NYTree;
using namespace NCypress;
using namespace NCellMaster;
using namespace NObjectServer;

////////////////////////////////////////////////////////////////////////////////

class TVirtualTransactionMap
    : public TVirtualMapBase
{
public:
    TVirtualTransactionMap(TBootstrap* bootstrap)
        : Bootstrap(bootstrap)
    { }

private:
    TBootstrap* Bootstrap;

    virtual yvector<Stroka> GetKeys(size_t sizeLimit) const
    {
        const auto& ids = Bootstrap->GetTransactionManager()->GetTransactionIds(sizeLimit);
        return ConvertToStrings(ids.begin(), ids.end(), sizeLimit);
    }

    virtual size_t GetSize() const
    {
        return Bootstrap->GetTransactionManager()->GetTransactionCount();
    }

    virtual IYPathServicePtr GetItemService(const TStringBuf& key) const
    {
        auto id = TTransactionId::FromString(key);
        if (TypeFromId(id) != EObjectType::Transaction) {
            return NULL;
        }
        return Bootstrap->GetObjectManager()->FindProxy(id);
    }
};

NCypress::INodeTypeHandlerPtr CreateTransactionMapTypeHandler(TBootstrap* bootstrap)
{
    YASSERT(bootstrap);

    return CreateVirtualTypeHandler(
        bootstrap,
        EObjectType::TransactionMap,
        ~New<TVirtualTransactionMap>(bootstrap));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionServer
} // namespace NYT
