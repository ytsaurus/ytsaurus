#include "stdafx.h"
#include "cypress_integration.h"

#include <ytlib/cypress/virtual.h>
#include <ytlib/cypress/cypress_manager.h>
#include <ytlib/ytree/virtual.h>
#include <ytlib/ytree/fluent.h>
#include <ytlib/misc/string.h>
#include <ytlib/cell_master/bootstrap.h>

namespace NYT {
namespace NCypress {

using namespace NYTree;
using namespace NCellMaster;
using namespace NObjectServer;

////////////////////////////////////////////////////////////////////////////////

class TVirtualNodeMap
    : public TVirtualMapBase
{
public:
    TVirtualNodeMap(TBootstrap* bootstrap)
        : Bootstrap(bootstrap)
    { }

private:
    TBootstrap* Bootstrap;

    virtual std::vector<Stroka> GetKeys(size_t sizeLimit) const
    {
        const auto& ids = Bootstrap->GetCypressManager()->GetNodeIds(sizeLimit);
        return ConvertToStrings(ids.begin(), ids.end(), sizeLimit);
    }

    virtual size_t GetSize() const
    {
        return Bootstrap->GetCypressManager()->GetNodeCount();
    }

    virtual IYPathServicePtr GetItemService(const TStringBuf& key) const
    {
        auto id = TVersionedNodeId::FromString(key);
        auto transaction = Bootstrap->GetTransactionManager()->FindTransaction(id.TransactionId);
        if (!transaction) {
            return NULL;
        }
        return Bootstrap->GetCypressManager()->FindVersionedNodeProxy(id.ObjectId, transaction);
    }
};

INodeTypeHandlerPtr CreateNodeMapTypeHandler(TBootstrap* bootstrap)
{
    YASSERT(bootstrap);

    return CreateVirtualTypeHandler(
        bootstrap,
        EObjectType::NodeMap,
        New<TVirtualNodeMap>(bootstrap));
}

////////////////////////////////////////////////////////////////////////////////

class TVirtualLockMap
    : public TVirtualMapBase
{
public:
    TVirtualLockMap(TBootstrap* bootstrap)
        : Bootstrap(bootstrap)
    { }

private:
    TBootstrap* Bootstrap;

    virtual std::vector<Stroka> GetKeys(size_t sizeLimit) const
    {
        const auto& locks = Bootstrap->GetCypressManager()->GetLocks(sizeLimit);
        std::vector<TLockId> ids;
        ids.reserve(locks.size());
        FOREACH (const auto& lock, locks) {
            ids.push_back(lock->GetId());
        }
        return ConvertToStrings(ids.begin(), ids.end(), sizeLimit);
    }

    virtual size_t GetSize() const
    {
        return Bootstrap->GetCypressManager()->GetLockCount();
    }

    virtual IYPathServicePtr GetItemService(const TStringBuf& key) const
    {
        auto id = TLockId::FromString(key);
        if (TypeFromId(id) != EObjectType::Lock) {
            return NULL;
        }
        return Bootstrap->GetObjectManager()->FindProxy(id);
    }
};

INodeTypeHandlerPtr CreateLockMapTypeHandler(TBootstrap* bootstrap)
{
    YASSERT(bootstrap);

    return CreateVirtualTypeHandler(
        bootstrap,
        EObjectType::LockMap,
        New<TVirtualLockMap>(bootstrap));
}

////////////////////////////////////////////////////////////////////////////////
} // namespace NChunkServer
} // namespace NYT
