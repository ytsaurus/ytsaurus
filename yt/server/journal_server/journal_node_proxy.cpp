#include "stdafx.h"
#include "journal_node_proxy.h"
#include "journal_node.h"
#include "private.h"

#include <server/chunk_server/chunk_owner_node_proxy.h>

namespace NYT {
namespace NJournalServer {

using namespace NChunkClient;
using namespace NChunkServer;
using namespace NCypressServer;
using namespace NYTree;
using namespace NYson;
using namespace NTransactionServer;
using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

class TJournalNodeProxy
    : public TCypressNodeProxyBase<TChunkOwnerNodeProxy, IEntityNode, TJournalNode>
{
public:
    TJournalNodeProxy(
        INodeTypeHandlerPtr typeHandler,
        TBootstrap* bootstrap,
        TTransaction* transaction,
        TJournalNode* trunkNode)
        : TCypressNodeProxyBase(
            typeHandler,
            bootstrap,
            transaction,
            trunkNode)
    { }

private:
    virtual NLog::TLogger CreateLogger() const override
    {
        return JournalServerLogger;
    }

    virtual ELockMode GetLockMode(EUpdateMode updateMode) override
    {
        return ELockMode::Exclusive;
    }

    virtual void ListSystemAttributes(std::vector<TAttributeInfo>* attributes) override
    {
        attributes->push_back("read_concern");
        attributes->push_back("write_concern");
        TCypressNodeProxyBase::ListSystemAttributes(attributes);
    }

    virtual bool GetSystemAttribute(const Stroka& key, IYsonConsumer* consumer) override
    {
        const auto* node = GetThisTypedImpl();

        if (key == "read_concern") {
            BuildYsonFluently(consumer)
                .Value(node->GetReadConcern());
            return true;
        }

        if (key == "write_concern") {
            BuildYsonFluently(consumer)
                .Value(node->GetWriteConcern());
            return true;
        }

        return TCypressNodeProxyBase::GetSystemAttribute(key, consumer);
    }

    virtual bool SetSystemAttribute(const Stroka& key, const TYsonString& value) override
    {
        if (key == "replication_factor") {
            // Prevent changing replication factor after construction.
            ValidateNoTransaction();
            auto* node = GetThisTypedImpl();
            YCHECK(node->IsTrunk());
            if (node->GetReplicationFactor() != 0) {
                ThrowCannotSetSystemAttribute("replication_factor");
            } else {
                return TCypressNodeProxyBase::SetSystemAttribute(key, value);
            }
        }

        if (key == "read_concern") {
            int readConcern = NYTree::ConvertTo<int>(value);
            if (readConcern < 1) {
                THROW_ERROR_EXCEPTION("Value must be positive");
            }

            ValidateNoTransaction();
            auto* node = GetThisTypedImpl();
            YCHECK(node->IsTrunk());

            if (node->GetReadConcern() != 0) {
                ThrowCannotSetSystemAttribute("read_concern");
            }
            node->SetReadConcern(readConcern);
            return true;
        }

        if (key == "write_concern") {
            int writeConcern = NYTree::ConvertTo<int>(value);
            if (writeConcern < 1) {
                THROW_ERROR_EXCEPTION("Value must be positive");
            }

            ValidateNoTransaction();
            auto* node = GetThisTypedImpl();
            YCHECK(node->IsTrunk());

            if (node->GetWriteConcern() != 0) {
                ThrowCannotSetSystemAttribute("write_concern");
            }
            node->SetWriteConcern(writeConcern);
            return true;
        }

        return TNontemplateCypressNodeProxyBase::SetSystemAttribute(key, value);
    }

};

////////////////////////////////////////////////////////////////////////////////

ICypressNodeProxyPtr CreateJournalNodeProxy(
    INodeTypeHandlerPtr typeHandler,
    TBootstrap* bootstrap,
    TTransaction* transaction,
    TJournalNode* trunkNode)
{

    return New<TJournalNodeProxy>(
        typeHandler,
        bootstrap,
        transaction,
        trunkNode);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NJournalServer
} // namespace NYT
