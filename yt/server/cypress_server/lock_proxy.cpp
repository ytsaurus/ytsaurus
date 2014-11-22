#include "stdafx.h"
#include "lock_proxy.h"
#include "lock.h"
#include "node.h"
#include "private.h"

#include <core/ytree/fluent.h>

#include <ytlib/cypress_client/lock_ypath.pb.h>

#include <server/object_server/object_detail.h>

#include <server/transaction_server/transaction.h>

namespace NYT {
namespace NCypressServer {

using namespace NYson;
using namespace NYTree;
using namespace NObjectServer;

////////////////////////////////////////////////////////////////////////////////

class TLockProxy
    : public TNonversionedObjectProxyBase<TLock>
{
public:
    TLockProxy(NCellMaster::TBootstrap* bootstrap, TLock* lock)
        : TBase(bootstrap, lock)
    { }

private:
    typedef TNonversionedObjectProxyBase<TLock> TBase;

    virtual NLog::TLogger CreateLogger() const override
    {
        return CypressServerLogger;
    }

    virtual void ListSystemAttributes(std::vector<TAttributeInfo>* attributes) override
    {
        const auto* lock = GetThisTypedImpl();
        attributes->push_back("state");
        attributes->push_back("transaction_id");
        attributes->push_back("node_id");
        attributes->push_back("mode");
        attributes->push_back(TAttributeInfo("child_key", lock->Request().ChildKey.HasValue()));
        attributes->push_back(TAttributeInfo("attribute_key", lock->Request().AttributeKey.HasValue()));
        TBase::ListSystemAttributes(attributes);
    }

    virtual bool GetBuiltinAttribute(const Stroka& key, IYsonConsumer* consumer) override
    {
        const auto* lock = GetThisTypedImpl();

        if (key == "state") {
            BuildYsonFluently(consumer)
                .Value(lock->GetState());
            return true;
        }

        if (key == "transaction_id") {
            BuildYsonFluently(consumer)
                .Value(lock->GetTransaction()->GetId());
            return true;
        }

        if (key == "node_id") {
            BuildYsonFluently(consumer)
                .Value(lock->GetTrunkNode()->GetId());
            return true;
        }

        if (key == "mode") {
            BuildYsonFluently(consumer)
                .Value(lock->Request().Mode);
            return true;
        }

        if (key == "child_key" && lock->Request().ChildKey) {
            BuildYsonFluently(consumer)
                .Value(*lock->Request().ChildKey);
            return true;
        }

        if (key == "attribute_key" && lock->Request().AttributeKey) {
            BuildYsonFluently(consumer)
                .Value(*lock->Request().AttributeKey);
            return true;
        }

        return TBase::GetBuiltinAttribute(key, consumer);
    }

};

IObjectProxyPtr CreateLockProxy(
    NCellMaster::TBootstrap* bootstrap,
    TLock* lock)
{
    return New<TLockProxy>(bootstrap, lock);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressServer
} // namespace NYT

