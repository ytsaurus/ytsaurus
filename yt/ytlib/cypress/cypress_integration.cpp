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
        const auto& ids = CypressManager->GetNodeIds();
        return ConvertToStrings(ids.begin(), Min(ids.size(), sizeLimit));
    }

    virtual size_t GetSize() const
    {
        return CypressManager->GetNodeCount();
    }

    virtual IYPathService::TPtr GetItemService(const Stroka& key) const
    {
        auto branchedNodeId = TBranchedNodeId::FromString(key);
        auto* node = CypressManager->FindNode(branchedNodeId);
        if (!node) {
            return NULL;
        }

        return IYPathService::FromProducer(~FromFunctor([=] (IYsonConsumer* consumer)
            {
                BuildYsonFluently(consumer)
                    .BeginMap()
                        .Item("state").Scalar(node->GetState().ToString())
                        .Item("parent_id").Scalar(node->GetParentId().ToString())
                        .Item("attributes_id").Scalar(node->GetAttributesId().ToString())
                        .Item("ref_counter").Scalar(node->GetRefCounter())
                    .EndMap();
            }));
    }
};

INodeTypeHandler::TPtr CreateNodeMapTypeHandler(
    TCypressManager* cypressManager)
{
    YASSERT(cypressManager);

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

    virtual yvector<Stroka> GetKeys(size_t sizeLimit) const
    {
        const auto& ids = CypressManager->GetLockIds();
        return ConvertToStrings(ids.begin(), Min(ids.size(), sizeLimit));
    }

    virtual size_t GetSize() const
    {
        return CypressManager->GetLockCount();
    }

    virtual IYPathService::TPtr GetItemService(const Stroka& key) const
    {
        auto id = TLockId::FromString(key);
        auto* lock = CypressManager->FindLock(id);
        if (!lock) {
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
    YASSERT(cypressManager);

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
