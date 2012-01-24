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

TUntypedObjectProxyBase::TUntypedObjectProxyBase(
    TObjectManager* objectManager,
    const TObjectId& id,
    const Stroka& loggingCategory)
    : TYPathServiceBase(loggingCategory)
    , ObjectManager(objectManager)
    , Id(id)
{ }

TObjectId TUntypedObjectProxyBase::GetId() const
{
    return Id;
}

TTransactionId TUntypedObjectProxyBase::GetTransactionId() const
{
    return NullTransactionId;
}

IYPathService::TResolveResult TUntypedObjectProxyBase::ResolveAttributes(
    const TYPath& path,
    const Stroka& verb)
{
    bool supported;
    auto attributePath = ChopYPathAttributeMarker(path);
    if (IsFinalYPath(attributePath)) {
        supported =
            verb == "Get" ||
            verb == "List";
    } else {
        supported =
            verb == "Get" ||
            verb == "Set" ||
            verb == "List" ||
            verb == "Remove";
    }

    if (!supported) {
        ythrow TServiceException(EErrorCode::NoSuchVerb) << "Verb is not supported for attributes";
    }

    return TResolveResult::Here(path);
}

void TUntypedObjectProxyBase::GetSystemAttributeNames(yvector<Stroka>* names)
{
    names->push_back("id");
    names->push_back("type");
    names->push_back("ref_counter");
}

bool TUntypedObjectProxyBase::GetSystemAttribute(const Stroka& name, IYsonConsumer* consumer)
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

bool TUntypedObjectProxyBase::SetSystemAttribute(const Stroka& name, NYTree::TYsonProducer* producer)
{
    UNUSED(producer);

    if (name == "id" || name == "type" || name == "ref_counter") {
        throw yexception() << Sprintf("The %s system attribute cannot be set", ~name.Quote());
    }

    return false;
}

void TUntypedObjectProxyBase::DoInvoke(NRpc::IServiceContext* context)
{
    DISPATCH_YPATH_SERVICE_METHOD(GetId);
    DISPATCH_YPATH_SERVICE_METHOD(Get);
    DISPATCH_YPATH_SERVICE_METHOD(List);
    DISPATCH_YPATH_SERVICE_METHOD(Set);
    DISPATCH_YPATH_SERVICE_METHOD(Remove);
    NYTree::TYPathServiceBase::DoInvoke(context);
}

DEFINE_RPC_SERVICE_METHOD(TUntypedObjectProxyBase, GetId)
{
    UNUSED(request);

    response->set_object_id(Id.ToProto());
    context->Reply();
}

DEFINE_RPC_SERVICE_METHOD(TUntypedObjectProxyBase, Get)
{
    auto path = context->GetPath();
    if (IsFinalYPath(path)) {
        GetSelf(request, response, ~context);
    } else if (IsAttributeYPath(path)) {
        auto attributePath = ChopYPathAttributeMarker(path);
        GetAttribute(attributePath, request, response, ~context);
    } else {
        GetRecursive(path, request, response, ~context);
    }
}

void TUntypedObjectProxyBase::GetSelf(TReqGet* request, TRspGet* response, TCtxGet* context)
{
    UNUSED(request);

    response->set_value(NYTree::BuildYsonFluently().Entity());
    context->Reply();
}

void TUntypedObjectProxyBase::GetRecursive(const TYPath& path, TReqGet* request, TRspGet* response, TCtxGet* context)
{
    UNUSED(path);
    UNUSED(request);
    UNUSED(response);
    UNUSED(context);

    ythrow yexception() << "Path must be final";
}

void TUntypedObjectProxyBase::GetAttribute(const TYPath& path, TReqGet* request, TRspGet* response, TCtxGet* context)
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

        // TODO(roizner): NullTransactionId???
        TVersionedObjectId versionedId(Id, GetTransactionId());

        const auto* objectAttributes = ObjectManager->FindAttributes(versionedId);
        if (objectAttributes) {
            FOREACH (const auto& pair, objectAttributes->Attributes()) {
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

DEFINE_RPC_SERVICE_METHOD(TUntypedObjectProxyBase, List)
{
    auto path = context->GetPath();
    if (IsFinalYPath(path)) {
        ListSelf(request, response, ~context);
    } else if (IsAttributeYPath(path)) {
        auto attributePath = ChopYPathAttributeMarker(path);
        ListAttribute(attributePath, request, response, ~context);
    } else {
        ListRecursive(path, request, response, ~context);
    }
}

void TUntypedObjectProxyBase::ListSelf(TReqList* request, TRspList* response, TCtxList* context)
{
    UNUSED(request);
    UNUSED(response);
    UNUSED(context);

    ythrow TServiceException(EErrorCode::NoSuchVerb) << "Verb is not supported";
}

void TUntypedObjectProxyBase::ListRecursive(const NYTree::TYPath& path, TReqList* request, TRspList* response, TCtxList* context)
{
    UNUSED(path);
    UNUSED(request);
    UNUSED(response);
    UNUSED(context);

    ythrow yexception() << "Path must be final";
}

void TUntypedObjectProxyBase::ListAttribute(const TYPath& path, TReqList* request, TRspList* response, TCtxList* context)
{
    yvector<Stroka> keys;

    if (IsFinalYPath(path)) {
        GetSystemAttributeNames(&keys);
        
        TVersionedObjectId versionedId(Id, GetTransactionId());
        const auto* objectAttributes = ObjectManager->FindAttributes(versionedId);
        if (objectAttributes) {
            keys.reserve(keys.size() + objectAttributes->Attributes().size());
            FOREACH (const auto& pair, objectAttributes->Attributes()) {
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

DEFINE_RPC_SERVICE_METHOD(TUntypedObjectProxyBase, Set)
{
    auto path = context->GetPath();
    if (IsFinalYPath(path)) {
        SetSelf(request, response, ~context);
    } else if (IsAttributeYPath(path)) {
        auto attributePath = ChopYPathAttributeMarker(path);
        SetAttribute(attributePath, request, response, ~context);
    } else {
        SetRecursive(path, request, response, ~context);
    }
}

void TUntypedObjectProxyBase::SetSelf(TReqSet* request, TRspSet* response, TCtxSet* context)
{
    UNUSED(request);
    UNUSED(response);
    UNUSED(context);

    ythrow TServiceException(EErrorCode::NoSuchVerb) << "Verb is not supported";
}

void TUntypedObjectProxyBase::SetRecursive(const NYTree::TYPath& path, TReqSet* request, TRspSet* response, TCtxSet* context)
{
    UNUSED(path);
    UNUSED(request);
    UNUSED(response);
    UNUSED(context);

    ythrow yexception() << "Path must be final";
}

void TUntypedObjectProxyBase::SetAttribute(const TYPath& path, TReqSet* request, TRspSet* response, TCtxSet* context)
{
    yvector<Stroka> keys;

    if (IsFinalYPath(path)) {
        ythrow yexception() << "Verb is not supported";
    }

    Stroka token;
    TYPath suffixPath;
    ChopYPathToken(path, &token, &suffixPath);

    if (IsFinalYPath(suffixPath)) {
        if (!SetSystemAttribute(token, ~ProducerFromYson(request->value()))) {
            TVersionedObjectId versionedId(Id, GetTransactionId());
            auto* objectAttributes = ObjectManager->FindAttributesForUpdate(versionedId);
            if (!objectAttributes) {
                objectAttributes = ObjectManager->CreateAttributes(versionedId);
            }
            objectAttributes->Attributes()[token] = request->value();
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

    context->Reply();
}

DEFINE_RPC_SERVICE_METHOD(TUntypedObjectProxyBase, Remove)
{
    auto path = context->GetPath();
    if (IsFinalYPath(path)) {
        RemoveSelf(request, response, ~context);
    } else if (IsAttributeYPath(path)) {
        auto attributePath = ChopYPathAttributeMarker(path);
        RemoveAttribute(attributePath, request, response, ~context);
    } else {
        RemoveRecursive(path, request, response, ~context);
    }
}

void TUntypedObjectProxyBase::RemoveSelf(TReqRemove* request, TRspRemove* response, TCtxRemove* context)
{
    UNUSED(request);
    UNUSED(response);
    UNUSED(context);

    ythrow TServiceException(EErrorCode::NoSuchVerb) << "Verb is not supported";
}

void TUntypedObjectProxyBase::RemoveRecursive(const NYTree::TYPath& path, TReqRemove* request, TRspRemove* response, TCtxRemove* context)
{
    UNUSED(path);
    UNUSED(request);
    UNUSED(response);
    UNUSED(context);

    ythrow yexception() << "Path must be final";
}

void TUntypedObjectProxyBase::RemoveAttribute(const TYPath& path, TReqRemove* request, TRspRemove* response, TCtxRemove* context)
{
    yvector<Stroka> keys;

    if (IsFinalYPath(path)) {
        ythrow yexception() << "Verb is not supported";
    }

    Stroka token;
    TYPath suffixPath;
    ChopYPathToken(path, &token, &suffixPath);

    if (IsFinalYPath(suffixPath)) {
        TVersionedObjectId versionedId(Id, GetTransactionId());
        auto* objectAttributes = ObjectManager->FindAttributesForUpdate(versionedId);
        if (!objectAttributes) {
            ythrow yexception() << Sprintf("User attribute %s is not found", ~token.Quote());
        }
        auto it = objectAttributes->Attributes().find(token);
        if (it == objectAttributes->Attributes().end()) {
            ythrow yexception() << Sprintf("User attribute %s is not found", ~token.Quote());
        }
        objectAttributes->Attributes().erase(it);
        if (objectAttributes->Attributes().size() == 0) {
            ObjectManager->RemoveAttributes(versionedId);
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

    context->Reply();
}

Stroka TUntypedObjectProxyBase::DoGetAttribute(const Stroka& name, bool* isSystem)
{
    TStringStream stream;
    TYsonWriter writer(&stream, EFormat::Binary);
    if (GetSystemAttribute(name, &writer)) {
        if (isSystem)
            *isSystem = true;
        return stream.Str();
    }

    TVersionedObjectId versionedId(Id, GetTransactionId());

    const auto* objectAttributes = ObjectManager->FindAttributes(versionedId);
    if (objectAttributes) {
        auto it = objectAttributes->Attributes().find(name);
        if (it != objectAttributes->Attributes().end()) {
            if (isSystem)
                *isSystem = false;
            return it->Second();
        }
    }

    ythrow yexception() << Sprintf("Attribute %s is not found", ~name.Quote());
}

void TUntypedObjectProxyBase::DoSetAttribute(const Stroka name, NYTree::INode* value, bool isSystem)
{
    if (isSystem) {
        YVERIFY(SetSystemAttribute(name, ~ProducerFromNode(value)));
    } else {
        TVersionedObjectId versionedId(Id, GetTransactionId());
        ObjectManager->GetAttributesForUpdate(versionedId)
             .Attributes().find(name)->Second() = SerializeToYson(value);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT

