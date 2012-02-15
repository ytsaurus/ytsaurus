#include "stdafx.h"
#include "object_detail.h"
#include "object_manager.h"

#include <ytlib/misc/string.h>
#include <ytlib/ytree/fluent.h>
#include <ytlib/ytree/ypath_client.h>
#include <ytlib/ytree/serialize.h>

namespace NYT {
namespace NObjectServer {

using namespace NRpc;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TObjectBase::TObjectBase()
    : RefCounter(0)
{ }

TObjectBase::TObjectBase(const TObjectBase& other)
    : RefCounter(other.RefCounter)
{ }

i32 TObjectBase::RefObject()
{
    return ++RefCounter;
}

i32 TObjectBase::UnrefObject()
{
    YASSERT(RefCounter > 0);
    return --RefCounter;
}

i32 TObjectBase::GetObjectRefCounter() const
{
    return RefCounter;
}

void TObjectBase::Save(TOutputStream* output) const
{
    ::Save(output, RefCounter);
}

void TObjectBase::Load(TInputStream* input)
{
    ::Load(input, RefCounter);
}

////////////////////////////////////////////////////////////////////////////////

TObjectWithIdBase::TObjectWithIdBase()
{ }

TObjectWithIdBase::TObjectWithIdBase(const TObjectId& id)
    : Id_(id)
{ }

TObjectWithIdBase::TObjectWithIdBase(const TObjectWithIdBase& other)
    : TObjectBase(other)
    , Id_(other.Id_)
{ }

////////////////////////////////////////////////////////////////////////////////

TObjectProxyBase::TObjectProxyBase(
    TObjectManager* objectManager,
    const TObjectId& id,
    const Stroka& loggingCategory)
    : TYPathServiceBase(loggingCategory)
    , ObjectManager(objectManager)
    , Id(id)
{ }

TObjectId TObjectProxyBase::GetId() const
{
    return Id;
}

IAttributeDictionary* TObjectProxyBase::Attributes()
{
    return GetUserAttributes();
}

DEFINE_RPC_SERVICE_METHOD(TObjectProxyBase, GetId)
{
    UNUSED(request);

    response->set_object_id(Id.ToProto());
    context->Reply();
}

void TObjectProxyBase::Invoke(IServiceContext* context)
{
    ObjectManager->ExecuteVerb(
        GetVersionedId(),
        IsWriteRequest(context),
        context,
        ~FromMethod(&TYPathServiceBase::GuardedInvoke, TYPathServiceBase::TPtr(this)));
}

void TObjectProxyBase::DoInvoke(IServiceContext* context)
{
    DISPATCH_YPATH_SERVICE_METHOD(GetId);
    DISPATCH_YPATH_SERVICE_METHOD(Get);
    DISPATCH_YPATH_SERVICE_METHOD(List);
    DISPATCH_YPATH_SERVICE_METHOD(Set);
    DISPATCH_YPATH_SERVICE_METHOD(Remove);
    TYPathServiceBase::DoInvoke(context);
}

bool TObjectProxyBase::IsWriteRequest(NRpc::IServiceContext* context) const
{
    DECLARE_YPATH_SERVICE_WRITE_METHOD(Set);
    DECLARE_YPATH_SERVICE_WRITE_METHOD(Remove);
    return TYPathServiceBase::IsWriteRequest(context);
}

IAttributeDictionary* TObjectProxyBase::GetUserAttributes()
{
    if (!UserAttributes) {
        UserAttributes = DoCreateUserAttributes();
    }
    return UserAttributes.Get();
}

ISystemAttributeProvider* TObjectProxyBase::GetSystemAttributeProvider()
{
    return this;
}

TAutoPtr<IAttributeDictionary> TObjectProxyBase::DoCreateUserAttributes()
{
    return new TUserAttributeDictionary(Id, ~ObjectManager);
}

void TObjectProxyBase::GetSystemAttributes(std::vector<TAttributeInfo>* names)
{
    names->push_back("id");
    names->push_back("type");
    names->push_back("ref_counter");
}

bool TObjectProxyBase::GetSystemAttribute(const Stroka& key, IYsonConsumer* consumer)
{
    if (key == "id") {
        BuildYsonFluently(consumer)
            .Scalar(GetId().ToString());
        return true;
    }

    if (key == "type") {
        BuildYsonFluently(consumer)
            .Scalar(CamelCaseToUnderscoreCase(TypeFromId(Id).ToString()));
        return true;
    }

    if (key == "ref_counter") {
        BuildYsonFluently(consumer)
            .Scalar(ObjectManager->GetObjectRefCounter(Id));
        return true;
    }

    return false;
}

bool TObjectProxyBase::SetSystemAttribute(const Stroka& key, TYsonProducer producer)
{
    UNUSED(producer);

    return false;
}

TVersionedObjectId TObjectProxyBase::GetVersionedId() const
{
    return Id;
}

////////////////////////////////////////////////////////////////////////////////

TObjectProxyBase::TUserAttributeDictionary::TUserAttributeDictionary(
    const TObjectId& objectId,
    TObjectManager* objectManager)
    : ObjectId(objectId)
    , ObjectManager(objectManager)
{ }

yhash_set<Stroka> TObjectProxyBase::TUserAttributeDictionary::List() const
{
    yhash_set<Stroka> attributes;
    const auto* attributeSet = ObjectManager->FindAttributes(ObjectId);
    if (attributeSet) {
        FOREACH (const auto& pair, attributeSet->Attributes()) {
            // Attribute cannot be empty (i.e. deleted) in null transaction.
            YASSERT(!pair.second.empty());
            attributes.insert(pair.first);
        }
    }
    return attributes;
}

TNullable<TYson> TObjectProxyBase::TUserAttributeDictionary::FindYson(const Stroka& key) const
{
    const auto* attributeSet = ObjectManager->FindAttributes(ObjectId);
    if (!attributeSet) {
        return Null;
    }
    auto it = attributeSet->Attributes().find(key);
    if (it == attributeSet->Attributes().end()) {
        return Null;
    }
    // Attribute cannot be empty (i.e. deleted) in null transaction.
    YASSERT(!it->second.empty());
    return it->second;
}

void TObjectProxyBase::TUserAttributeDictionary::SetYson(
    const Stroka& key,
    const NYTree::TYson& value)
{
    auto* attributeSet = ObjectManager->FindAttributesForUpdate(ObjectId);
    if (!attributeSet) {
        attributeSet = ObjectManager->CreateAttributes(ObjectId);
    }
    attributeSet->Attributes()[key] = value;
}

bool TObjectProxyBase::TUserAttributeDictionary::Remove(const Stroka& key)
{
    auto* attributeSet = ObjectManager->FindAttributesForUpdate(ObjectId);
    if (!attributeSet) {
        return false;
    }
    auto it = attributeSet->Attributes().find(key);
    if (it == attributeSet->Attributes().end()) {
        return false;
    }
    // Attribute cannot be empty (i.e. deleted) in null transaction.
    YASSERT(!it->second.empty());
    attributeSet->Attributes().erase(it);
    if (attributeSet->Attributes().empty()) {
        ObjectManager->RemoveAttributes(ObjectId);
    }
    return true;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT

