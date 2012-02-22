#include "stdafx.h"
#include "cypress_integration.h"

#include <ytlib/cypress/virtual.h>
#include <ytlib/ytree/virtual.h>
#include <ytlib/ytree/fluent.h>
#include <ytlib/misc/string.h>

namespace NYT {
namespace NCypress {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TVirtualNodeMap
    : public TVirtualMapBase
{
public:
    TVirtualNodeMap(TCypressManager* cypressManager)
        : CypressManager(cypressManager)
    { }

private:
    TCypressManager::TPtr CypressManager;

    virtual yvector<Stroka> GetKeys(size_t sizeLimit) const
    {
        const auto& ids = CypressManager->GetNodeIds(sizeLimit);
        return ConvertToStrings(ids.begin(), ids.end(), sizeLimit);
    }

    virtual size_t GetSize() const
    {
        return CypressManager->GetNodeCount();
    }

    virtual IYPathServicePtr GetItemService(const Stroka& key) const
    {
        auto id = TVersionedNodeId::FromString(key);
        return CypressManager->FindVersionedNodeProxy(id.ObjectId, id.TransactionId);
    }
};

INodeTypeHandler::TPtr CreateNodeMapTypeHandler(
    TCypressManager* cypressManager)
{
    YASSERT(cypressManager);

    return CreateVirtualTypeHandler(
        cypressManager,
        EObjectType::NodeMap,
        ~New<TVirtualNodeMap>(cypressManager));
}

////////////////////////////////////////////////////////////////////////////////

class TVirtualLockMap
    : public TVirtualMapBase
{
public:
    TVirtualLockMap(TCypressManager* cypressManager)
        : CypressManager(cypressManager)
    { }

private:
    TCypressManager::TPtr CypressManager;

    virtual yvector<Stroka> GetKeys(size_t sizeLimit) const
    {
        const auto& ids = CypressManager->GetLockIds(sizeLimit);
        return ConvertToStrings(ids.begin(), ids.end(), sizeLimit);
    }

    virtual size_t GetSize() const
    {
        return CypressManager->GetLockCount();
    }

    virtual IYPathServicePtr GetItemService(const Stroka& key) const
    {
        auto id = TLockId::FromString(key);
        auto* lock = CypressManager->FindLock(id);
        if (!lock) {
            return NULL;
        }

        return IYPathService::FromProducer(FromFunctor([=] (IYsonConsumer* consumer)
            {
                BuildYsonFluently(consumer)
                    .BeginMap()
                        .Item("node_id").Scalar(lock->GetNodeId().ToString())
                        .Item("transaction_id").Scalar(lock->GetTransactionId().ToString())
                        .Item("mode").Scalar(lock->GetMode().ToString())
                    .EndMap();
            }));
    }
};

INodeTypeHandler::TPtr CreateLockMapTypeHandler(
    TCypressManager* cypressManager)
{
    YASSERT(cypressManager);

    return CreateVirtualTypeHandler(
        cypressManager,
        EObjectType::LockMap,
        ~New<TVirtualLockMap>(cypressManager));
}

////////////////////////////////////////////////////////////////////////////////
} // namespace NChunkServer
} // namespace NYT
