#include "lock_proxy.h"
#include "private.h"
#include "lock.h"
#include "node.h"

#include <yt/server/object_server/object_detail.h>

#include <yt/server/transaction_server/transaction.h>

#include <yt/core/ytree/fluent.h>

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
    TLockProxy(
        NCellMaster::TBootstrap* bootstrap,
        TObjectTypeMetadata* metadata,
        TLock* lock)
        : TBase(bootstrap, metadata, lock)
    { }

private:
    typedef TNonversionedObjectProxyBase<TLock> TBase;

    virtual void ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors) override
    {
        TBase::ListSystemAttributes(descriptors);

        const auto* lock = GetThisImpl();
        const auto& request = lock->Request();

        descriptors->push_back("implicit");
        descriptors->push_back("state");
        descriptors->push_back("transaction_id");
        descriptors->push_back("node_id");
        descriptors->push_back("mode");
        descriptors->push_back(TAttributeDescriptor("child_key")
            .SetPresent(request.Key.Kind == ELockKeyKind::Child));
        descriptors->push_back(TAttributeDescriptor("attribute_key")
            .SetPresent(request.Key.Kind == ELockKeyKind::Attribute));
    }

    virtual bool GetBuiltinAttribute(const TString& key, IYsonConsumer* consumer) override
    {
        const auto* lock = GetThisImpl();
        const auto& request = lock->Request();

        if (key == "implicit") {
            BuildYsonFluently(consumer)
                .Value(lock->GetImplicit());
            return true;
        }

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

        if (key == "child_key" && request.Key.Kind == ELockKeyKind::Child) {
            BuildYsonFluently(consumer)
                .Value(request.Key.Name);
            return true;
        }

        if (key == "attribute_key" && request.Key.Kind == ELockKeyKind::Attribute) {
            BuildYsonFluently(consumer)
                .Value(request.Key.Name);
            return true;
        }

        return TBase::GetBuiltinAttribute(key, consumer);
    }

};

IObjectProxyPtr CreateLockProxy(
    NCellMaster::TBootstrap* bootstrap,
    TObjectTypeMetadata* metadata,
    TLock* lock)
{
    return New<TLockProxy>(bootstrap, metadata, lock);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressServer
} // namespace NYT

