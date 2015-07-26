#include "stdafx.h"
#include "lock_proxy.h"
#include "lock.h"
#include "node.h"
#include "private.h"

#include <core/ytree/fluent.h>

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

    virtual NLogging::TLogger CreateLogger() const override
    {
        return CypressServerLogger;
    }

    virtual void ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors) override
    {
        TBase::ListSystemAttributes(descriptors);

        const auto* lock = GetThisTypedImpl();

        descriptors->push_back("state");
        descriptors->push_back("transaction_id");
        descriptors->push_back("node_id");
        descriptors->push_back("mode");
        descriptors->push_back(TAttributeDescriptor("child_key")
            .SetPresent(lock->Request().ChildKey.HasValue()));
        descriptors->push_back(TAttributeDescriptor("attribute_key")
            .SetPresent(lock->Request().AttributeKey.HasValue()));
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

