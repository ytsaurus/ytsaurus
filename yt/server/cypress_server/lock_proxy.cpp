#include "lock_proxy.h"
#include "private.h"
#include "lock.h"
#include "node.h"

#include <yt/server/object_server/interned_attributes.h>
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

        descriptors->push_back(EInternedAttributeKey::Implicit);
        descriptors->push_back(EInternedAttributeKey::State);
        descriptors->push_back(EInternedAttributeKey::TransactionId);
        descriptors->push_back(EInternedAttributeKey::NodeId);
        descriptors->push_back(EInternedAttributeKey::Mode);
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ChildKey)
            .SetPresent(request.Key.Kind == ELockKeyKind::Child));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::AttributeKey)
            .SetPresent(request.Key.Kind == ELockKeyKind::Attribute));
    }

    virtual bool GetBuiltinAttribute(TInternedAttributeKey key, IYsonConsumer* consumer) override
    {
        const auto* lock = GetThisImpl();
        const auto& request = lock->Request();

        switch (key) {
            case EInternedAttributeKey::Implicit:
                BuildYsonFluently(consumer)
                    .Value(lock->GetImplicit());
                return true;

            case EInternedAttributeKey::State:
                BuildYsonFluently(consumer)
                    .Value(lock->GetState());
                return true;

            case EInternedAttributeKey::TransactionId:
                BuildYsonFluently(consumer)
                    .Value(lock->GetTransaction()->GetId());
                return true;

            case EInternedAttributeKey::NodeId:
                BuildYsonFluently(consumer)
                    .Value(lock->GetTrunkNode()->GetId());
                return true;

            case EInternedAttributeKey::Mode:
                BuildYsonFluently(consumer)
                    .Value(lock->Request().Mode);
                return true;

            case EInternedAttributeKey::ChildKey:
                if (request.Key.Kind != ELockKeyKind::Child) {
                    break;
                }
                BuildYsonFluently(consumer)
                    .Value(request.Key.Name);
                return true;

            case EInternedAttributeKey::AttributeKey:
                if (request.Key.Kind != ELockKeyKind::Attribute) {
                    break;
                }
                BuildYsonFluently(consumer)
                    .Value(request.Key.Name);
                return true;

            default:
                break;
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

