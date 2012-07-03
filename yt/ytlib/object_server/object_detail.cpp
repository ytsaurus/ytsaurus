#include "stdafx.h"
#include "object_detail.h"
#include "object_manager.h"

#include <ytlib/misc/string.h>
#include <ytlib/ytree/fluent.h>
#include <ytlib/ytree/ypath_client.h>
#include <ytlib/ytree/tokenizer.h>
#include <ytlib/cell_master/bootstrap.h>

namespace NYT {
namespace NObjectServer {

using namespace NRpc;
using namespace NYTree;
using namespace NCellMaster;

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
    TBootstrap* bootstrap,
    const TObjectId& id)
    : Bootstrap(bootstrap)
    , Id(id)
{ }

TObjectProxyBase::~TObjectProxyBase()
{ }

TObjectId TObjectProxyBase::GetId() const
{
    return Id;
}

IAttributeDictionary& TObjectProxyBase::Attributes()
{
    return CombinedAttributes();
}

const IAttributeDictionary& TObjectProxyBase::Attributes() const
{
    return CombinedAttributes();
}

DEFINE_RPC_SERVICE_METHOD(TObjectProxyBase, GetId)
{
    context->SetRequestInfo("AllowNonemptyPathSuffix: ",
        ~FormatBool(request->allow_nonempty_path_suffix()));

    if (!request->allow_nonempty_path_suffix()) {
        TTokenizer tokenizer(context->GetPath());
        if (tokenizer.ParseNext()) {
            ythrow yexception() << Sprintf("Unexpected path suffix %s", ~context->GetPath());
        }
    }

    *response->mutable_object_id() = Id.ToProto();
    context->Reply();
}

void TObjectProxyBase::Invoke(IServiceContextPtr context)
{
    Bootstrap->GetObjectManager()->ExecuteVerb(
        GetVersionedId(),
        IsWriteRequest(context),
        context,
        BIND(&TYPathServiceBase::GuardedInvoke, TIntrusivePtr<TYPathServiceBase>(this)));
}

void TObjectProxyBase::DoInvoke(IServiceContextPtr context)
{
    DISPATCH_YPATH_SERVICE_METHOD(GetId);
    DISPATCH_YPATH_SERVICE_METHOD(Get);
    DISPATCH_YPATH_SERVICE_METHOD(List);
    DISPATCH_YPATH_SERVICE_METHOD(Set);
    DISPATCH_YPATH_SERVICE_METHOD(Remove);
    TYPathServiceBase::DoInvoke(context);
}

bool TObjectProxyBase::IsWriteRequest(NRpc::IServiceContextPtr context) const
{
    DECLARE_YPATH_SERVICE_WRITE_METHOD(Set);
    DECLARE_YPATH_SERVICE_WRITE_METHOD(Remove);
    return TYPathServiceBase::IsWriteRequest(context);
}

IAttributeDictionary* TObjectProxyBase::GetUserAttributes()
{
    if (!UserAttributes.Get()) {
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
    return new TUserAttributeDictionary(
        Bootstrap->GetObjectManager(),
        Id);
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
            .Scalar(Bootstrap->GetObjectManager()->GetObjectRefCounter(Id));
        return true;
    }

    return false;
}

bool TObjectProxyBase::SetSystemAttribute(const Stroka& key, const TYsonString& value)
{
    UNUSED(key);
    UNUSED(value);

    return false;
}

TVersionedObjectId TObjectProxyBase::GetVersionedId() const
{
    return Id;
}

////////////////////////////////////////////////////////////////////////////////

TObjectProxyBase::TUserAttributeDictionary::TUserAttributeDictionary(
    TObjectManagerPtr objectManager,
    const TObjectId& objectId)
    : ObjectManager(MoveRV(objectManager))
    , ObjectId(objectId)
{ }

yhash_set<Stroka> TObjectProxyBase::TUserAttributeDictionary::List() const
{
    yhash_set<Stroka> attributes;
    const auto* attributeSet = ObjectManager->FindAttributes(ObjectId);
    if (attributeSet) {
        FOREACH (const auto& pair, attributeSet->Attributes()) {
            // Attribute cannot be empty (i.e. deleted) in null transaction.
            YASSERT(pair.second);
            attributes.insert(pair.first);
        }
    }
    return attributes;
}

TNullable<TYsonString> TObjectProxyBase::TUserAttributeDictionary::FindYson(const Stroka& key) const
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
    YASSERT(it->second);
    return it->second;
}

void TObjectProxyBase::TUserAttributeDictionary::SetYson(
    const Stroka& key,
    const NYTree::TYsonString& value)
{
    YASSERT(value.Data() != "");
    auto* attributeSet = ObjectManager->FindAttributes(ObjectId);
    if (!attributeSet) {
        attributeSet = ObjectManager->CreateAttributes(ObjectId);
    }
    attributeSet->Attributes()[key] = value;
}

bool TObjectProxyBase::TUserAttributeDictionary::Remove(const Stroka& key)
{
    auto* attributeSet = ObjectManager->FindAttributes(ObjectId);
    if (!attributeSet) {
        return false;
    }
    auto it = attributeSet->Attributes().find(key);
    if (it == attributeSet->Attributes().end()) {
        return false;
    }
    // Attribute cannot be empty (i.e. deleted) in null transaction.
    YASSERT(it->second);
    attributeSet->Attributes().erase(it);
    if (attributeSet->Attributes().empty()) {
        ObjectManager->RemoveAttributes(ObjectId);
    }
    return true;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT

