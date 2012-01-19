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
{
    RegisterSystemAttribute("id");
    RegisterSystemAttribute("type");
    RegisterSystemAttribute("ref_counter");
}

TObjectId TUntypedObjectProxyBase::GetId() const
{
    return Id;
}

IYPathService::TResolveResult TUntypedObjectProxyBase::ResolveAttributes(
    const TYPath& path,
    const Stroka& verb )
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
        ythrow TServiceException(EErrorCode::NoSuchVerb) <<
            Sprintf("Verb is not supported for attributes");
    }

    return TResolveResult::Here(path);
}

void TUntypedObjectProxyBase::RegisterSystemAttribute(const Stroka& name)
{
    YVERIFY(SystemAttributes.insert(name).second);
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

IYPathService::TPtr TUntypedObjectProxyBase::GetSystemAttribute(const Stroka& name)
{
    UNUSED(name);
    return NULL;
}

bool TUntypedObjectProxyBase::IsLogged(IServiceContext* context) const
{
    UNUSED(context);
    return false;
}

void TUntypedObjectProxyBase::DoInvoke( NRpc::IServiceContext* context )
{
    DISPATCH_YPATH_SERVICE_METHOD(GetId);
    DISPATCH_YPATH_SERVICE_METHOD(Get);
    NYTree::TYPathServiceBase::DoInvoke(context);
}

DEFINE_RPC_SERVICE_METHOD(TUntypedObjectProxyBase, GetId)
{
    UNUSED(request);

    response->set_id(Id.ToProto());
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
        TStringStream stream;
        TYsonWriter writer(&stream, TYsonWriter::EFormat::Binary);
        
        writer.OnBeginMap();
        
        FOREACH (const auto& name, SystemAttributes) {
            writer.OnMapItem(name);
            if (!GetSystemAttribute(name, &writer)) {
                auto service = GetSystemAttribute(name);
                YASSERT(service);
                auto value = SyncYPathGet(~service, RootMarker);
                writer.OnRaw(value);
            }
        }

        // TODO: user attributes
        writer.OnEndMap();

        response->set_value(stream.Str());
    } else {
        Stroka token;
        TYPath suffixPath;
        ChopYPathToken(path, &token, &suffixPath);

        if (SystemAttributes.find(token) == SystemAttributes.end()) {
            // TODO: user attributes
            ythrow yexception() << Sprintf("Attribute %s is not found", ~token.Quote());
        } else {
            TStringStream stream;
            TYsonWriter writer(&stream, TYsonWriter::EFormat::Binary);
            if (GetSystemAttribute(token, &writer)) {
                if (IsFinalYPath(suffixPath)) {
                    response->set_value(stream.Str());
                } else {
                    auto wholeValue = DeserializeFromYson(stream.Str());
                    auto value = SyncYPathGet(~wholeValue, RootMarker + suffixPath);
                    response->set_value(value);
                }
            } else {
                auto service = GetSystemAttribute(token);
                if (!service) {
                    ythrow yexception() << Sprintf("Attribute %s is not found", ~token.Quote());
                }

                auto value = SyncYPathGet(~service, RootMarker + suffixPath);
                response->set_value(value);
            }
        }
    }

    context->Reply();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT

