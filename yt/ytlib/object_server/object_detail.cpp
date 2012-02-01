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

IYPathService::TResolveResult TObjectProxyBase::ResolveAttributes(
    const TYPath& path,
    const Stroka& verb)
{
    UNUSED(path);
    UNUSED(verb);
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

DEFINE_RPC_SERVICE_METHOD(TObjectProxyBase, GetId)
{
    UNUSED(request);

    response->set_object_id(Id.ToProto());
    context->Reply();
}

void TObjectProxyBase::GetAttribute(const TYPath& path, TReqGet* request, TRspGet* response, TCtxGet* context)
{
    if (IsFinalYPath(path)) {
        TStringStream stream;
        TYsonWriter writer(&stream, EYsonFormat::Binary);
        
        writer.OnBeginMap();

        yvector<TAttributeInfo> systemAttributes;
        GetSystemAttributes(&systemAttributes);
        FOREACH (const auto& attribute, systemAttributes) {
            if (attribute.IsPresent) {
                writer.OnMapItem(attribute.Name);
                if (attribute.IsOpaque) {
                    writer.OnEntity();
                } else {
                    YVERIFY(GetSystemAttribute(attribute.Name, &writer));
                }
            }
        }

        const auto& userAttributes = ListUserAttributes();
        FOREACH (const auto& name, userAttributes) {
            writer.OnMapItem(name);
            writer.OnRaw(GetUserAttribute(name));
        }
        
        writer.OnEndMap();

        response->set_value(stream.Str());
    } else {
        Stroka token;
        TYPath suffixPath;
        ChopYPathToken(path, &token, &suffixPath);

        const auto& yson = DoGetAttribute(token);

        if (IsFinalYPath(suffixPath)) {
            response->set_value(yson);
        } else {
            auto wholeValue = DeserializeFromYson(yson);
            auto value = SyncYPathGet(~wholeValue, RootMarker + suffixPath);
            response->set_value(value);
        }
    }

    context->Reply();
}

void TObjectProxyBase::ListAttribute(const TYPath& path, TReqList* request, TRspList* response, TCtxList* context)
{
    yvector<Stroka> keys;

    if (IsFinalYPath(path)) {
        yvector<TAttributeInfo> systemAttributes;
        GetSystemAttributes(&systemAttributes);
        FOREACH (const auto& attribute, systemAttributes) {
            if (attribute.IsPresent) {
                keys.push_back(attribute.Name);
            }
        }
        
        const auto& userAttributes = ListUserAttributes();
        // If we used vector instead of yvector, we could start with user
        // attributes instead of copying them.
        keys.insert(keys.end(), userAttributes.begin(), userAttributes.end());
    } else {
        Stroka token;
        TYPath suffixPath;
        ChopYPathToken(path, &token, &suffixPath);

        auto wholeValue = DeserializeFromYson(DoGetAttribute(token));
        keys = SyncYPathList(~wholeValue, RootMarker + suffixPath);
    }

    ToProto(*response->mutable_keys(), keys);
    context->Reply();
}

void TObjectProxyBase::SetAttribute(const TYPath& path, TReqSet* request, TRspSet* response, TCtxSet* context)
{
    if (IsFinalYPath(path)) {
        auto value = DeserializeFromYson(request->value());
        if (value->GetType() != ENodeType::Map) {
            ythrow yexception() << "Map value expected";
        }

        const auto& userAttributes = ListUserAttributes();
        FOREACH (const auto& name, userAttributes) {
            YVERIFY(RemoveUserAttribute(name));
        }

        // TODO(babenko): handle system attributes
        auto mapValue = value->AsMap();
        FOREACH (const auto& pair, mapValue->GetChildren()) {
            auto key = pair.first;
            YASSERT(!key.empty());
            auto value = SerializeToYson(~pair.second);
            SetUserAttribute(key, value);
        }
    } else {
        Stroka token;
        TYPath suffixPath;
        ChopYPathToken(path, &token, &suffixPath);

        if (IsFinalYPath(suffixPath)) {
            if (token.empty()) {
                ythrow yexception() << "Attribute key cannot be empty";
            }

            if (!SetSystemAttribute(token, ~ProducerFromYson(request->value()))) {
            	// Check for system attributes
	            yvector<TAttributeInfo> systemAttributes;
    	        GetSystemAttributes(&systemAttributes);
    	        
            	FOREACH (const auto& attribute, systemAttributes) {
	                if (attribute.Name == token) {
    	                ythrow yexception() << Sprintf("System attribute %s cannot be set", ~token.Quote());
                	}
            	}

                SetUserAttribute(token, request->value());
            }
        } else {
            Stroka token;
            TYPath suffixPath;
            ChopYPathToken(path, &token, &suffixPath);

            bool isSystem;
            auto yson = DoGetAttribute(token, &isSystem);
            auto wholeValue = DeserializeFromYson(yson);
            SyncYPathSet(~wholeValue, RootMarker + suffixPath, request->value());
            DoSetAttribute(token, ~wholeValue, isSystem);
        }
    }

    context->Reply();
}

void TObjectProxyBase::RemoveAttribute(const TYPath& path, TReqRemove* request, TRspRemove* response, TCtxRemove* context)
{
    if (IsFinalYPath(path)) {
        const auto& userAttributes = ListUserAttributes();
        FOREACH (const auto& name, userAttributes) {
            YVERIFY(RemoveUserAttribute(name));
        }
    } else {
        Stroka token;
        TYPath suffixPath;
        ChopYPathToken(path, &token, &suffixPath);

        if (IsFinalYPath(suffixPath)) {
            if (!RemoveUserAttribute(token)) {
                ythrow yexception() << Sprintf("User attribute %s is not found", ~token.Quote());
            }
        } else {
            Stroka token;
            TYPath suffixPath;
            ChopYPathToken(path, &token, &suffixPath);

            bool isSystem;
            auto yson = DoGetAttribute(token, &isSystem);
            auto wholeValue = DeserializeFromYson(yson);
            SyncYPathRemove(~wholeValue, RootMarker + suffixPath);
            DoSetAttribute(token, ~wholeValue, isSystem);
        }
    }

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

TYson TObjectProxyBase::DoGetAttribute(const Stroka& name, bool* isSystem)
{
    TStringStream stream;
    TYsonWriter writer(&stream, EYsonFormat::Binary);
    if (GetSystemAttribute(name, &writer)) {
        if (isSystem) {
            *isSystem = true;
        }
        return stream.Str();
    }

    auto yson = GetUserAttribute(name);
    if (!yson.empty()) {
        if (isSystem) {
            *isSystem = false;
        }
        return yson;
    }
    
    ythrow yexception() << Sprintf("Attribute %s is not found", ~name.Quote());
}

void TObjectProxyBase::DoSetAttribute(const Stroka name, INode* value, bool isSystem)
{
    if (isSystem) {
        if (!SetSystemAttribute(name, ~ProducerFromNode(value))) {
            ythrow yexception() << Sprintf("System attribute %s cannot be set", ~name.Quote());
        }
    } else {
        SetUserAttribute(name, SerializeToYson(value));
    }
}

yhash_set<Stroka> TObjectProxyBase::ListUserAttributes()
{
    yhash_set<Stroka> attributes;
    const auto* attributeSet = ObjectManager->FindAttributes(Id);
    if (attributeSet) {
        FOREACH (const auto& pair, attributeSet->Attributes()) {
            // Attribute cannot be empty (i.e. deleted) in null transaction.
            YASSERT(!pair.second.empty());
            attributes.insert(pair.first);
        }
    }
    return attributes;
}

TYson TObjectProxyBase::GetUserAttribute(const Stroka& name)
{
    const auto* attributeSet = ObjectManager->FindAttributes(Id);
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

void TObjectProxyBase::SetUserAttribute(const Stroka& name, const TYson& value)
{
    auto* attributeSet = ObjectManager->FindAttributesForUpdate(Id);
    if (!attributeSet) {
        attributeSet = ObjectManager->CreateAttributes(Id);
    }
    attributeSet->Attributes()[name] = value;
}

bool TObjectProxyBase::RemoveUserAttribute(const Stroka& name)
{
    auto* attributeSet = ObjectManager->FindAttributesForUpdate(Id);
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
        ObjectManager->RemoveAttributes(Id);
    }
    return true;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT

