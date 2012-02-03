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

DEFINE_RPC_SERVICE_METHOD(TObjectProxyBase, GetId)
{
    UNUSED(request);

    response->set_object_id(Id.ToProto());
    context->Reply();
}

void TObjectProxyBase::DoInvoke(NRpc::IServiceContext* context)
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

IAttributeDictionary::TPtr TObjectProxyBase::GetUserAttributeDictionary()
{
    return New<TUserAttributeDictionary>(Id, ~ObjectManager);
}

ISystemAttributeProvider::TPtr TObjectProxyBase::GetSystemAttributeProvider()
{
    return this;
}

IYPathService::TResolveResult TObjectProxyBase::ResolveAttributes(
    const TYPath& path,
    const Stroka& verb)
{
    UNUSED(path);
    if (verb != "Get" &&
        verb != "Set" &&
        verb != "List" &&
        verb != "Remove")
    {
        ythrow TServiceException(EErrorCode::NoSuchVerb) <<
            "Verb is not supported";
    }
    return TResolveResult::Here(AttributeMarker + path);
}

void TObjectProxyBase::GetSystemAttributes(yvector<TAttributeInfo>* names)
{
    names->push_back("id");
    names->push_back("type");
    names->push_back("ref_counter");
}

bool TObjectProxyBase::GetSystemAttribute(const Stroka& name, IYsonConsumer* consumer)
{
    if (name == "id") {
        BuildYsonFluently(consumer)
            .Scalar(GetId().ToString());
        return true;
    }

    if (name == "type") {
        BuildYsonFluently(consumer)
            .Scalar(CamelCaseToUnderscoreCase(TypeFromId(Id).ToString()));
        return true;
    }

    if (name == "ref_counter") {
        BuildYsonFluently(consumer)
            .Scalar(ObjectManager->GetObjectRefCounter(Id));
        return true;
    }

    return false;
}

bool TObjectProxyBase::SetSystemAttribute(const Stroka& name, TYsonProducer* producer)
{
    UNUSED(producer);

    return false;
}

////////////////////////////////////////////////////////////////////////////////

TObjectProxyBase::TUserAttributeDictionary::TUserAttributeDictionary(
    TObjectId objectId,
    TObjectManager* objectManager)
    : ObjectId(objectId)
    , ObjectManager(objectManager)
{ }

yhash_set<Stroka> TObjectProxyBase::TUserAttributeDictionary::ListAttributes()
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

TYson TObjectProxyBase::TUserAttributeDictionary::FindAttribute(const Stroka& name)
{
    const auto* attributeSet = ObjectManager->FindAttributes(ObjectId);
    if (!attributeSet) {
        return "";
    }
    auto it = attributeSet->Attributes().find(name);
    if (it == attributeSet->Attributes().end()) {
        return "";
    }
    // Attribute cannot be empty (i.e. deleted) in null transaction.
    YASSERT(!it->second.empty());
    return it->second;
}

void TObjectProxyBase::TUserAttributeDictionary::SetAttribute(
    const Stroka& name,
    const NYTree::TYson& value)
{
    auto* attributeSet = ObjectManager->FindAttributesForUpdate(ObjectId);
    if (!attributeSet) {
        attributeSet = ObjectManager->CreateAttributes(ObjectId);
    }
    attributeSet->Attributes()[name] = value;
}

bool TObjectProxyBase::TUserAttributeDictionary::RemoveAttribute( const Stroka& name )
{
    auto* attributeSet = ObjectManager->FindAttributesForUpdate(ObjectId);
    if (!attributeSet) {
        return false;
    }
    auto it = attributeSet->Attributes().find(name);
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

