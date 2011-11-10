#include "stdafx.h"
#include "cypress_integration.h"

#include "../cypress/virtual.h"
#include "../ytree/virtual.h"
#include "../ytree/fluent.h"
#include "../misc/string.h"

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

    virtual yvector<Stroka> GetKeys()
    {
        return ConvertToStrings(CypressManager->GetNodeIds());
    }

    virtual IYPathService::TPtr GetItemService(const Stroka& key)
    {
        auto branchedNodeId = TBranchedNodeId::FromString(key);
        auto* node = CypressManager->FindNode(branchedNodeId);
        if (node == NULL) {
            return NULL;
        }

        return IYPathService::FromProducer(~FromFunctor([=] (IYsonConsumer* consumer)
            {
                BuildYsonFluently(consumer)
                    .BeginMap()
                        .Item("state").Scalar(node->GetState().ToString())
                        .Item("parent_id").Scalar(node->GetParentId().ToString())
                        .Item("attributes_id").Scalar(node->GetAttributesId().ToString())
                    .EndMap();
            }));
    }
};

INodeTypeHandler::TPtr CreateNodeMapTypeHandler(
    TCypressManager* cypressManager)
{
    YASSERT(cypressManager != NULL);

    return CreateVirtualTypeHandler(
        cypressManager,
        ERuntimeNodeType::NodeMap,
        // TODO: extract type name
        "node_map",
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

    virtual yvector<Stroka> GetKeys()
    {
        return ConvertToStrings(CypressManager->GetLockIds());
    }

    virtual IYPathService::TPtr GetItemService(const Stroka& key)
    {
        auto id = TLockId::FromString(key);
        auto* lock = CypressManager->FindLock(id);
        if (lock == NULL) {
            return NULL;
        }

        return IYPathService::FromProducer(~FromFunctor([=] (IYsonConsumer* consumer)
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
    YASSERT(cypressManager != NULL);

    return CreateVirtualTypeHandler(
        cypressManager,
        ERuntimeNodeType::LockMap,
        // TODO: extract type name
        "lock_map",
        ~New<TVirtualLockMap>(cypressManager));
}

////////////////////////////////////////////////////////////////////////////////
} // namespace NChunkServer
} // namespace NYT
