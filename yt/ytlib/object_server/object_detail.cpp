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

void TObjectProxyBase::GetSystemAttributeNames(yvector<Stroka>* names)
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

bool TObjectProxyBase::SetSystemAttribute(const Stroka& name, NYTree::TYsonProducer* producer)
{
    UNUSED(producer);

    if (name == "id" || name == "type" || name == "ref_counter") {
        throw yexception() << Sprintf("The %s system attribute cannot be set", ~name.Quote());
    }

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
        yvector<Stroka> names;
        GetSystemAttributeNames(&names);

        TStringStream stream;
        TYsonWriter writer(&stream, EFormat::Binary);
        
        writer.OnBeginMap();

        FOREACH (const auto& name, names) {
            writer.OnMapItem(name);
            YVERIFY(GetSystemAttribute(name, &writer));
        }

        const auto* userAttributes = FindAttributes();
        if (userAttributes) {
            FOREACH (const auto& pair, userAttributes->Attributes()) {
                writer.OnMapItem(pair.First());
                writer.OnRaw(pair.Second());
            }
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
        GetSystemAttributeNames(&keys);
        
        const auto* userAttributes = FindAttributes();
        if (userAttributes) {
            keys.reserve(keys.size() + userAttributes->Attributes().size());
            FOREACH (const auto& pair, userAttributes->Attributes()) {
                keys.push_back(pair.First());
            }
        }
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

        // TODO(babenko): handle system attributes
        auto mapValue = value->AsMap();
        if (mapValue->GetChildCount() == 0) {
            RemoveAttributes();
        } else {
            auto* userAttributes = GetAttributesForUpdate();
            userAttributes->Attributes().clear();
            FOREACH (const auto& pair, mapValue->GetChildren()) {
                auto key = pair.first;
                auto value = SerializeToYson(~pair.second);
                userAttributes->Attributes()[key] = value;
            }
        }
    } else {
        Stroka token;
        TYPath suffixPath;
        ChopYPathToken(path, &token, &suffixPath);

        if (IsFinalYPath(suffixPath)) {
            if (!SetSystemAttribute(token, ~ProducerFromYson(request->value()))) {
                auto* userAttributes = GetAttributesForUpdate();
                userAttributes->Attributes()[token] = request->value();
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
        RemoveAttributes();
    } else {
        Stroka token;
        TYPath suffixPath;
        ChopYPathToken(path, &token, &suffixPath);

        if (IsFinalYPath(suffixPath)) {
            auto* userAttributes = FindAttributesForUpdate();
            if (!userAttributes) {
                ythrow yexception() << Sprintf("User attribute %s is not found", ~token.Quote());
            }
            auto it = userAttributes->Attributes().find(token);
            if (it == userAttributes->Attributes().end()) {
                ythrow yexception() << Sprintf("User attribute %s is not found", ~token.Quote());
            }
            userAttributes->Attributes().erase(it);
            if (userAttributes->Attributes().size() == 0) {
                RemoveAttributes();
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

Stroka TObjectProxyBase::DoGetAttribute(const Stroka& name, bool* isSystem)
{
    TStringStream stream;
    TYsonWriter writer(&stream, EFormat::Binary);
    if (GetSystemAttribute(name, &writer)) {
        if (isSystem) {
            *isSystem = true;
        }
        return stream.Str();
    }

    const auto* userAttributes = FindAttributes();
    if (userAttributes) {
        auto it = userAttributes->Attributes().find(name);
        if (it != userAttributes->Attributes().end()) {
            if (isSystem) {
                *isSystem = false;
            }
            return it->Second();
        }
    }

    ythrow yexception() << Sprintf("Attribute %s is not found", ~name.Quote());
}

void TObjectProxyBase::DoSetAttribute(const Stroka name, NYTree::INode* value, bool isSystem)
{
    if (isSystem) {
        YVERIFY(SetSystemAttribute(name, ~ProducerFromNode(value)));
    } else {
        auto* userAttributes = GetAttributesForUpdate();
        userAttributes->Attributes().find(name)->Second() = SerializeToYson(value);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT

