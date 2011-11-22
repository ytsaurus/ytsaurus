#include "stdafx.h"
#include "node_detail.h"
#include "ypath_detail.h"
#include "ypath_service.h"
#include "tree_visitor.h"
#include "tree_builder.h"
#include "yson_writer.h"
#include "ypath_client.h"

namespace NYT {
namespace NYTree {

using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

IYPathService::TResolveResult TNodeBase::ResolveAttributes(TYPath path, const Stroka& verb)
{
    Stroka attributePath = ChopYPathAttributeMarker(path);
    if (IsFinalYPath(attributePath) &&
        verb != "Get" &&
        verb != "List" &&
        verb != "Remove")
    {
        ythrow TServiceException(EYPathErrorCode::NoSuchVerb) <<
            "Verb is not supported for attributes";
    }
    return TResolveResult::Here(path);
}

void TNodeBase::DoInvoke(IServiceContext* context)
{
    Stroka verb = context->GetVerb();
    // TODO: use method table
    if (verb == "Get") {
        GetThunk(context);
    } else if (verb == "Set") {
        SetThunk(context);
    } else if (verb == "Remove") {
        RemoveThunk(context);
    } else {
        TYPathServiceBase::DoInvoke(context);
    }
}

////////////////////////////////////////////////////////////////////////////////

RPC_SERVICE_METHOD_IMPL(TNodeBase, Get)
{
    TYPath path = context->GetPath();
    if (IsFinalYPath(path)) {
        GetSelf(request, response, context);
    } else if (IsAttributeYPath(path)) {
        auto attributePath = ChopYPathAttributeMarker(path);
        if (IsFinalYPath(attributePath)) {
            TStringStream stream;
            TYsonWriter writer(&stream, TYsonWriter::EFormat::Binary);

            writer.OnBeginMap();

            FOREACH (const auto& attributeName, GetVirtualAttributeNames()) {
                auto attributeService = GetVirtualAttributeService(attributeName);
                auto attributeValue = SyncExecuteYPathGet(~attributeService, "/");
                writer.OnMapItem(attributeName);
                writer.OnRaw(attributeValue);
            }

            auto attributes = GetAttributes();
            if (~attributes != NULL) {
                FOREACH (const auto& pair, attributes->GetChildren()) {
                    writer.OnMapItem(pair.first);
                    TTreeVisitor visitor(&writer);
                    visitor.Visit(pair.second);
                }
            }

            writer.OnEndMap();

            response->SetValue(stream.Str());
            context->Reply();
        } else {
            Stroka prefix;
            TYPath suffixPath;
            ChopYPathToken(attributePath, &prefix, &suffixPath);

            auto service = GetVirtualAttributeService(prefix);
            if (~service != NULL) {
                response->SetValue(SyncExecuteYPathGet(~service, "/" + suffixPath));
                context->Reply();
                return;
            }

            auto attributes = GetAttributes();
            if (~attributes == NULL) {
                ythrow yexception() << "Node has no attributes";
            }

            response->SetValue(SyncExecuteYPathGet(~IYPathService::FromNode(~attributes), "/" + attributePath));
            context->Reply();
        }
    } else {
        GetRecursive(path, request, response, context);
    }
}

void TNodeBase::GetSelf(TReqGet* request, TRspGet* response, TCtxGet::TPtr context)
{
    UNUSED(request);

    TStringStream stream;
    TYsonWriter writer(&stream, TYsonWriter::EFormat::Binary);
    TTreeVisitor visitor(&writer, false);
    visitor.Visit(this);

    response->SetValue(stream.Str());
    context->Reply();
}

void TNodeBase::GetRecursive(TYPath path, TReqGet* request, TRspGet* response, TCtxGet::TPtr context)
{
    UNUSED(path);
    UNUSED(request);
    UNUSED(response);
    UNUSED(context);

    ythrow yexception() << "Path must be final";
}

RPC_SERVICE_METHOD_IMPL(TNodeBase, Set)
{
    TYPath path = context->GetPath();
    if (IsFinalYPath(path)) {
        SetSelf(request, response, context);
    } else if (IsAttributeYPath(path)) {
        auto attributePath = ChopYPathAttributeMarker(path);
        if (IsFinalYPath(attributePath)) {
            ythrow yexception() << "Resolution error: cannot set the whole attribute list";    
        }

        auto value = request->GetValue();

        Stroka prefix;
        TYPath suffixPath;
        ChopYPathToken(attributePath, &prefix, &suffixPath);

        auto service = GetVirtualAttributeService(prefix);
        if (~service != NULL) {
            SyncExecuteYPathSet(~service, "/" + suffixPath, value);
            context->Reply();
            return;
        }

        auto attributes = GetAttributes();
        if (~attributes == NULL) {
            attributes = GetFactory()->CreateMap();
            SetAttributes(attributes);
        }

        SyncExecuteYPathSet(
            ~IYPathService::FromNode(~attributes),
            "/" + attributePath,
            value);
        context->Reply();
    } else {
        SetRecursive(path, request, response, context);
    }
}

void TNodeBase::SetSelf(TReqSet* request, TRspSet* response, TCtxSet::TPtr context)
{
    UNUSED(request);
    UNUSED(response);
    UNUSED(context);

    ythrow TServiceException(EYPathErrorCode::NoSuchVerb) <<
        "Verb is not supported";
}

void TNodeBase::SetRecursive(TYPath path, TReqSet* request, TRspSet* response, TCtxSet::TPtr context)
{
    UNUSED(path);
    UNUSED(request);
    UNUSED(response);
    UNUSED(context);

    ythrow yexception() << "Path must be final";
}

RPC_SERVICE_METHOD_IMPL(TNodeBase, Remove)
{
    Stroka path = context->GetPath();
    if (IsFinalYPath(path)) {
        RemoveSelf(request, response, context);
    } else if (IsAttributeYPath(path)) {
        auto attributePath = ChopYPathAttributeMarker(path);
        if (IsFinalYPath(attributePath)) {
            SetAttributes(NULL);
        } else {
            Stroka prefix;
            TYPath suffixPath;
            ChopYPathToken(attributePath, &prefix, &suffixPath);

            auto attributes = GetAttributes();
            if (~attributes == NULL) {
                ythrow yexception() << "Node has no attributes";
            }

            SyncExecuteYPathRemove(~IYPathService::FromNode(~attributes), "/" + attributePath);

            if (attributes->GetChildCount() == 0) {
                SetAttributes(NULL);
            }
        }
        context->Reply();
    } else {    
        RemoveRecursive(path, request, response, context);
    }
}

void TNodeBase::RemoveSelf(TReqRemove* request, TRspRemove* response, TCtxRemove::TPtr context)
{
    UNUSED(request);
    UNUSED(response);

    auto parent = GetParent();

    if (~parent == NULL) {
        ythrow yexception() << "Cannot remove the root";
    }

    parent->AsComposite()->RemoveChild(this);
    context->Reply();
}

void TNodeBase::RemoveRecursive(TYPath path, TReqRemove* request, TRspRemove* response, TCtxRemove::TPtr context)
{
    UNUSED(path);
    UNUSED(request);
    UNUSED(response);
    UNUSED(context);

    ythrow yexception() << "Path must be final";
}

yvector<Stroka> TNodeBase::GetVirtualAttributeNames()
{
    return yvector<Stroka>();
}

IYPathService::TPtr TNodeBase::GetVirtualAttributeService(const Stroka& name)
{
    UNUSED(name);
    return NULL;
}

////////////////////////////////////////////////////////////////////////////////

bool TMapNodeMixin::DoInvoke(IServiceContext* context)
{
    Stroka verb = context->GetVerb();
    if (verb == "List") {
        ListThunk(context);
        return true;
    }
    return false;
}

RPC_SERVICE_METHOD_IMPL(TMapNodeMixin, List)
{
    UNUSED(request);

    FOREACH (const auto& pair, GetChildren()) {
        response->AddKeys(pair.first);
    }

    context->Reply();
}

IYPathService::TResolveResult TMapNodeMixin::ResolveRecursive(TYPath path, const Stroka& verb)
{
    Stroka prefix;
    TYPath suffixPath;
    ChopYPathToken(path, &prefix, &suffixPath);

    auto child = FindChild(prefix);
    if (~child != NULL) {
        return IYPathService::TResolveResult::There(~IYPathService::FromNode(~child), suffixPath);
    }

    if (verb == "Set" || verb == "Create") {
        return IYPathService::TResolveResult::Here(path);
    }

    ythrow yexception() << Sprintf("Key %s is not found", ~prefix.Quote());
}

void TMapNodeMixin::SetRecursive(TYPath path, NProto::TReqSet* request)
{
    auto builder = CreateBuilderFromFactory(GetFactory());
    builder->BeginTree();
    TStringInput input(request->GetValue());
    TYsonReader reader(~builder);
    reader.Read(&input);
    auto value = builder->EndTree();

    TMapNodeMixin::SetRecursive(path, ~value);
}

void TMapNodeMixin::SetRecursive(TYPath path, INode* value)
{
    IMapNode::TPtr currentNode = this;
    TYPath currentPath = path;

    while (true) {
        Stroka prefix;
        TYPath suffixPath;
        ChopYPathToken(currentPath, &prefix, &suffixPath);

        if (suffixPath.empty()) {
            if (!currentNode->AddChild(value, prefix)) {
                ythrow yexception() << Sprintf("Key %s already exists", ~prefix.Quote());
            }
            break;
        }

        auto intermediateNode = GetFactory()->CreateMap();
        if (!currentNode->AddChild(intermediateNode, prefix)) {
            ythrow yexception() << Sprintf("Key %s already exists", ~prefix.Quote());
        }

        currentNode = intermediateNode;
        currentPath = suffixPath;
    }
}

////////////////////////////////////////////////////////////////////////////////

IYPathService::TResolveResult TListNodeMixin::ResolveRecursive(TYPath path, const Stroka& verb)
{
    Stroka prefix;
    TYPath suffixPath;
    ChopYPathToken(path, &prefix, &suffixPath);

    if (prefix.empty()) {
        ythrow yexception() << "Child index is empty";
    }

    char lastPrefixCh = prefix[prefix.length() - 1];
    if ((verb == "Set" || verb == "Create") &&
        (lastPrefixCh != '+' || lastPrefixCh != '-'))
    {
        return IYPathService::TResolveResult::Here(path);
    } else {
        int index = ParseChildIndex(prefix);
        auto child = FindChild(index);
        YASSERT(~child != NULL);
        return IYPathService::TResolveResult::There(~IYPathService::FromNode(~child), suffixPath);
    }
}

void TListNodeMixin::SetRecursive(TYPath path, NProto::TReqSet* request)
{
    auto builder = CreateBuilderFromFactory(GetFactory());
    builder->BeginTree();
    TStringInput input(request->GetValue());
    TYsonReader reader(~builder);
    reader.Read(&input);
    auto value = builder->EndTree();

    SetRecursive(path, ~value);
}

void TListNodeMixin::SetRecursive(TYPath path, INode* value)
{
    INode::TPtr currentNode = this;
    TYPath currentPath = path;

    Stroka prefix;
    TYPath suffixPath;
    ChopYPathToken(currentPath, &prefix, &suffixPath);

    if (prefix.empty()) {
        ythrow yexception() << "Resolution error: child index is empty";
    }

    if (prefix == "+") {
        return CreateChild(GetChildCount(), suffixPath, value);
    } else if (prefix == "-") {
        return CreateChild(0, suffixPath, value);
    }

    char lastPrefixCh = prefix[prefix.length() - 1];
    if (lastPrefixCh != '+' && lastPrefixCh != '-') {
        ythrow yexception() << "Resolution error: insertion point expected";
    }

    int index = ParseChildIndex(TStringBuf(prefix.begin(), prefix.end() - 1));
    switch (lastPrefixCh) {
        case '+':
            CreateChild(index + 1, suffixPath, value);
            break;
        case '-':
            CreateChild(index, suffixPath, value);
            break;

        default:
            YUNREACHABLE();
    }
}

void TListNodeMixin::CreateChild(int beforeIndex, TYPath path, INode* value)
{
    if (IsFinalYPath(path)) {
        AddChild(value, beforeIndex);
    } else {
        auto currentNode = GetFactory()->CreateMap();
        auto currentPath = path;
        AddChild(currentNode, beforeIndex);

        while (true) {
            Stroka prefix;
            TYPath suffixPath;
            ChopYPathToken(currentPath, &prefix, &suffixPath);

            if (IsFinalYPath(suffixPath)) {
                currentNode->AddChild(value, prefix);
                break;
            }

            auto intermediateNode = GetFactory()->CreateMap();
            currentNode->AddChild(intermediateNode, prefix);

            currentNode = intermediateNode;
            currentPath = suffixPath;
        }
    }
}

int TListNodeMixin::ParseChildIndex(TStringBuf str)
{
    int index;
    try {
        index = FromString<int>(str);
    } catch (...) {
        ythrow yexception() << Sprintf("Failed to parse index %s\n%s",
            ~Stroka(str).Quote(),
            ~CurrentExceptionMessage());
    }

    int count = GetChildCount();
    if (count == 0) {
        ythrow yexception() << Sprintf("Invalid index %d: list is empty",
            index);
    }

    if (index < 0 || index >= count) {
        ythrow yexception() << Sprintf("Invalid index %d: expected value in range %d..%d",
            index,
            0,
            count - 1);
    }

    return index;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

