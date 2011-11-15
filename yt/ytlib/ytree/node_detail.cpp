#include "stdafx.h"
#include "node_detail.h"
#include "ypath_detail.h"
#include "ypath_service.h"
#include "tree_visitor.h"
#include "tree_builder.h"
#include "yson_writer.h"

namespace NYT {
namespace NYTree {

using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

IYPathService::TResolveResult TNodeBase::Resolve(TYPath path, bool mustExist)
{
    if (IsFinalYPath(path)) {
        return ResolveSelf(path, mustExist);
    } else if (path[0] == '@') {
        auto attributes = GetAttributes();
        if (~attributes == NULL) {
            ythrow yexception() << "Node has no custom attributes";
        }

        // TODO: virtual attributes

        return TResolveResult::There(
            ~IYPathService::FromNode(~attributes),
            path.substr(1));
    } else {
        return ResolveRecursive(path, mustExist);
    }
}

IYPathService::TResolveResult TNodeBase::ResolveSelf(TYPath path, bool mustExist)
{
    UNUSED(mustExist);
    return TResolveResult::Here(path);
}

IYPathService::TResolveResult TNodeBase::ResolveRecursive(TYPath path, bool mustExist)
{
    UNUSED(path);
    UNUSED(mustExist);
    ythrow yexception() << "Further navigation is not supported";
}

void TNodeBase::Invoke(IServiceContext* context)
{
    try {
        DoInvoke(context);
    } catch (...) {
        ythrow TTypedServiceException<EYPathErrorCode>(EYPathErrorCode::GenericError) <<
            CurrentExceptionMessage();
    }
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
        ythrow TTypedServiceException<EYPathErrorCode>(EYPathErrorCode::NoSuchVerb) <<
            Sprintf("Unknown verb %s", ~verb.Quote());
    }
}

void TNodeBase::ThrowNonEmptySuffixPath(TYPath path)
{
    ythrow yexception() << Sprintf("Suffix path %s cannot be resolved",
        ~path.Quote());
}

RPC_SERVICE_METHOD_IMPL(TNodeBase, Get)
{
    TYPath path = context->GetPath();
    if (IsFinalYPath(path)) {
        GetSelf(request, response, context);
    } else {
        GetRecursive(path, request, response, context);
    }

    // TODO: attributes
//    if (path[0] == '@') {
//        auto attributes = GetAttributes();
//
//        if (path == "@") {
//            // TODO: use fluent API
//
//            consumer->OnBeginMap();
//            auto names = GetVirtualAttributeNames();
//            FOREACH (const auto& name, names) {
//                consumer->OnMapItem(name);
//                YVERIFY(GetVirtualAttribute(name, consumer));
//            }
//            
//            if (~attributes != NULL) {
//                auto children = attributes->GetChildren();
//                FOREACH (const auto& pair, children) {
//                    consumer->OnMapItem(pair.First());
//                    TTreeVisitor visitor(consumer);
//                    visitor.Visit(pair.Second());
//                }
//            }
//
//            consumer->OnEndMap(false);
//
//            return TGetResult::CreateDone();
//        } else {
//            Stroka prefix;
//            TYPath suffixPath;
//            ChopYPathPrefix(TYPath(path.begin() + 1, path.end()), &prefix, &suffixPath);
//
//            if (GetVirtualAttribute(prefix, consumer))
//                return TGetResult::CreateDone();
//
//            if (~attributes == NULL) {
//                ythrow yexception() << "Node has no custom attributes";
//            }
//
//            auto child = attributes->FindChild(prefix);
//            if (~child == NULL) {
//                ythrow yexception() << Sprintf("Attribute %s is not found",
//                    ~prefix.Quote());
//            }
//
//            TTreeVisitor visitor(consumer);
//            visitor.Visit(child);
//            return TGetResult::CreateDone();
//        }
//    } else {
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

    ThrowNonEmptySuffixPath(path);
}

RPC_SERVICE_METHOD_IMPL(TNodeBase, Set)
{
    TYPath path = context->GetPath();
    if (IsFinalYPath(path)) {
        SetSelf(request, response, context);
    } else {
        SetRecursive(path, request, response, context);
    }
}

void TNodeBase::SetSelf(TReqSet* request, TRspSet* response, TCtxSet::TPtr context)
{
    UNUSED(request);
    UNUSED(response);
    UNUSED(context);

    ythrow yexception() << "Cannot modify the node";
}

void TNodeBase::SetRecursive(TYPath path, TReqSet* request, TRspSet* response, TCtxSet::TPtr context)
{
    UNUSED(path);
    UNUSED(request);
    UNUSED(response);
    UNUSED(context);

    ThrowNonEmptySuffixPath(path);

    // TODO: attributes
//    if (path[0] == '@') {
//        auto attributes = GetAttributes();
//        if (~attributes == NULL) {
//            attributes = ~GetFactory()->CreateMap();
//            SetAttributes(attributes);
//        }
//
//        // TODO: should not be able to override a virtual attribute
//
//        return IYPathService::TSetResult::CreateRecurse(
//            IYPathService::FromNode(~attributes),
//            TYPath(path.begin() + 1, path.end()));
//    } else {
}

RPC_SERVICE_METHOD_IMPL(TNodeBase, Remove)
{
    Stroka path = context->GetPath();
    if (IsFinalYPath(path)) {
        RemoveSelf(request, response, context);
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

    // TODO: attributes
//    if (path[0] == '@') {
//        auto attributes = GetAttributes();
//        if (~attributes == NULL) {
//            ythrow yexception() << "Node has no custom attributes";
//        }
//
//        return IYPathService::TRemoveResult::CreateRecurse(
//            IYPathService::FromNode(~attributes),
//            TYPath(path.begin() + 1, path.end()));

    ThrowNonEmptySuffixPath(path);
}

yvector<Stroka> TNodeBase::GetVirtualAttributeNames()
{
    return yvector<Stroka>();
}

bool TNodeBase::GetVirtualAttribute(const Stroka& name, IYsonConsumer* consumer)
{
    UNUSED(name);
    UNUSED(consumer);
    return false;
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

IYPathService::TResolveResult TMapNodeMixin::ResolveRecursive(TYPath path, bool mustExist)
{
    try {
        return GetYPathChild(path);
    } catch (...) {
        if (mustExist)
            throw;
        return IYPathService::TResolveResult::Here(path);
    }
}

IYPathService::TResolveResult TMapNodeMixin::GetYPathChild(TYPath path) const
{
    Stroka prefix;
    TYPath suffixPath;
    ChopYPathPrefix(path, &prefix, &suffixPath);
    auto child = FindChild(prefix);
    if (~child == NULL) {
        ythrow yexception() << Sprintf("Key %s is not found", ~prefix.Quote());
    }

    return IYPathService::TResolveResult::There(~IYPathService::FromNode(~child), suffixPath);
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
        ChopYPathPrefix(currentPath, &prefix, &suffixPath);

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

void TMapNodeMixin::ThrowNonEmptySuffixPath(TYPath path)
{
    // This should throw.
    GetYPathChild(path);
    YUNREACHABLE();
}

////////////////////////////////////////////////////////////////////////////////

IYPathService::TResolveResult TListNodeMixin::ResolveRecursive(TYPath path, bool mustExist)
{
    try {
        return GetYPathChild(path);
    } catch (...) {
        if (mustExist)
            throw;
        return IYPathService::TResolveResult::Here(path);
    }
}

IYPathService::TResolveResult TListNodeMixin::GetYPathChild(TYPath path) const
{
    Stroka prefix;
    TYPath suffixPath;
    ChopYPathPrefix(path, &prefix, &suffixPath);
    int index = FromString<int>(prefix);
    return GetYPathChild(index, suffixPath);
}

IYPathService::TResolveResult TListNodeMixin::GetYPathChild(
    int index,
    TYPath suffixPath) const
{
    int count = GetChildCount();
    if (count == 0) {
        ythrow yexception() << "List is empty";
    }

    if (index < 0 || index >= count) {
        ythrow yexception() << Sprintf("Invalid child index %d, expecting value in range 0..%d",
            index,
            count - 1);
    }

    auto child = FindChild(index);
    return IYPathService::TResolveResult::There(~IYPathService::FromNode(~child), suffixPath);
}

void TListNodeMixin::ThrowNonEmptySuffixPath(TYPath path)
{
    // This should throw.
    GetYPathChild(path);
    YUNREACHABLE();
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
    ChopYPathPrefix(currentPath, &prefix, &suffixPath);

    if (prefix.empty()) {
        ythrow yexception() << "Child index is empty";
    }

    if (prefix == "+") {
        return CreateYPathChild(GetChildCount(), suffixPath, value);
    } else if (prefix == "-") {
        return CreateYPathChild(0, suffixPath, value);
    }

    char lastPrefixCh = prefix[prefix.length() - 1];
    TStringBuf indexString =
        lastPrefixCh == '+' || lastPrefixCh == '-'
        ? TStringBuf(prefix.begin() + 1, prefix.end())
        : prefix;

    int index;
    try {
        index = FromString<int>(indexString);
    } catch (...) {
        ythrow yexception() << Sprintf("Failed to parse child index %s\n%s",
            ~Stroka(indexString).Quote(),
            ~CurrentExceptionMessage());
    }

    if (lastPrefixCh == '+') {
        CreateYPathChild(index + 1, suffixPath, value);
    } else if (lastPrefixCh == '-') {
        CreateYPathChild(index, suffixPath, value);
    } else {
        // Looks like an out-of-range child index.
        // This should throw.
        GetYPathChild(index, suffixPath);
        YUNREACHABLE();
    }
}

void TListNodeMixin::CreateYPathChild(int beforeIndex, TYPath path, INode* value)
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
            ChopYPathPrefix(currentPath, &prefix, &suffixPath);

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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

