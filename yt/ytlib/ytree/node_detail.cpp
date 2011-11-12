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

IYPathService::TNavigateResult TNodeBase::Navigate(TYPath path, bool mustExist)
{
    if (path.empty()) {
        return TNavigateResult::Here("");
    }

    if (path[0] == '@') {
        auto attributes = GetAttributes();
        if (~attributes == NULL) {
            throw yexception() << "Node has no custom attributes";
        }

        // TODO: virtual attributes

        return TNavigateResult::There(
            ~IYPathService::FromNode(~attributes),
            path.substr(1));
    }

    return NavigateRecursive(path, mustExist);
}

IYPathService::TNavigateResult TNodeBase::NavigateRecursive(TYPath path, bool mustExist)
{
    UNUSED(path);
    UNUSED(mustExist);
    throw yexception() << "Further navigation is not supported";
}

void TNodeBase::Invoke(NRpc::IServiceContext* context)
{
    try {
        DoInvoke(context);
    } catch (...) {
        ythrow TTypedServiceException<EYPathErrorCode>(EYPathErrorCode::GenericError) << "\n" <<
            CurrentExceptionMessage();
    }
}

void TNodeBase::DoInvoke(NRpc::IServiceContext* context)
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
        context->Reply(TError(EErrorCode::NoSuchMethod));
    }
}

void TNodeBase::ThrowNonEmptySuffixPath(TYPath path)
{
    ythrow yexception() << Sprintf("Suffix path %s cannot be resolved", path.Quote());
}

RPC_SERVICE_METHOD_IMPL(TNodeBase, Get)
{
    Stroka path = context->GetPath();
    if (path.empty()) {
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
//            TYPath tailPath;
//            ChopYPathPrefix(TYPath(path.begin() + 1, path.end()), &prefix, &tailPath);
//
//            if (GetVirtualAttribute(prefix, consumer))
//                return TGetResult::CreateDone();
//
//            if (~attributes == NULL) {
//                throw yexception() << "Node has no custom attributes";
//            }
//
//            auto child = attributes->FindChild(prefix);
//            if (~child == NULL) {
//                throw yexception() << Sprintf("Attribute %s is not found",
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
    Stroka path = context->GetPath();
    if (path.empty()) {
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
    if (path.empty()) {
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
        throw yexception() << "Cannot remove the root";
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
//            throw yexception() << "Node has no custom attributes";
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

IYPathService::TNavigateResult TMapNodeMixin::NavigateRecursive(TYPath path, bool mustExist)
{
    try {
        return GetYPathChild(path);
    } catch (...) {
        if (mustExist)
            throw;
        return IYPathService::TNavigateResult::Here(path);
    }
}

IYPathService::TNavigateResult TMapNodeMixin::GetYPathChild(TYPath path) const
{
    Stroka prefix;
    TYPath tailPath;
    ChopYPathPrefix(path, &prefix, &tailPath);
    auto child = FindChild(prefix);
    if (~child == NULL) {
        ythrow yexception() << Sprintf("Key %s is not found", ~prefix.Quote());
    }

    return IYPathService::TNavigateResult::There(~IYPathService::FromNode(~child), tailPath);
}

void TMapNodeMixin::SetRecursive(TYPath path, const TYson& value, ITreeBuilder* builder)
{
    IMapNode::TPtr currentNode = this;
    TYPath currentPath = path;

    while (true) {
        Stroka prefix;
        TYPath tailPath;
        ChopYPathPrefix(currentPath, &prefix, &tailPath);

        if (tailPath.empty()) {
            builder->BeginTree();
            TStringInput input(value);
            TYsonReader reader(builder);
            reader.Read(&input);
            auto newChild = builder->EndTree();
            currentNode->AddChild(newChild, prefix);
            break;
        }

        auto newChild = GetFactory()->CreateMap();
        currentNode->AddChild(newChild, prefix);
        currentNode = newChild;
        currentPath = tailPath;
    }
}

void TMapNodeMixin::ThrowNonEmptySuffixPath(TYPath path)
{
    // This should throw.
    GetYPathChild(path);
    YUNREACHABLE();
}

////////////////////////////////////////////////////////////////////////////////

IYPathService::TNavigateResult TListNodeMixin::NavigateRecursive(TYPath path, bool mustExist)
{
    try {
        return GetYPathChild(path);
    } catch (...) {
        if (mustExist)
            throw;
        return IYPathService::TNavigateResult::Here(path);
    }
}

IYPathService::TNavigateResult TListNodeMixin::GetYPathChild(TYPath path) const
{
    Stroka prefix;
    TYPath tailPath;
    ChopYPathPrefix(path, &prefix, &tailPath);
    int index = FromString<int>(prefix);
    return GetYPathChild(index, tailPath);
}

IYPathService::TNavigateResult TListNodeMixin::GetYPathChild(
    int index,
    TYPath tailPath) const
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
    return IYPathService::TNavigateResult::There(~IYPathService::FromNode(~child), tailPath);
}

void TListNodeMixin::ThrowNonEmptySuffixPath(TYPath path)
{
    // This should throw.
    GetYPathChild(path);
    YUNREACHABLE();
}

void TListNodeMixin::SetRecursive(
    TYPath path,
    const TYson& value,
    ITreeBuilder* builder)
{
    INode::TPtr currentNode = this;
    TYPath currentPath = path;

    Stroka prefix;
    TYPath tailPath;
    ChopYPathPrefix(currentPath, &prefix, &tailPath);

    if (prefix.empty()) {
        throw yexception() << "Child index is empty";
    }

    if (prefix == "+") {
        return CreateYPathChild(GetChildCount(), tailPath, value, builder);
    } else if (prefix == "-") {
        return CreateYPathChild(0, tailPath, value, builder);
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
        throw yexception() << Sprintf("Failed to parse child index %s\n%s",
            ~Stroka(indexString).Quote(),
            ~CurrentExceptionMessage());
    }

    if (lastPrefixCh == '+') {
        CreateYPathChild(index + 1, tailPath, value, builder);
    } else if (lastPrefixCh == '-') {
        CreateYPathChild(index, tailPath, value, builder);
    } else {
        // Looks like an out-of-range child index.
        // This should throw.
        GetYPathChild(index, tailPath);
        YUNREACHABLE();
    }
}

void TListNodeMixin::CreateYPathChild(
    int beforeIndex,
    TYPath tailPath,
    const TYson& value,
    ITreeBuilder* builder)
{
    builder->BeginTree();
    TYsonReader reader(builder);
    TStringInput input(value);
    reader.Read(&input);
    auto newChild = builder->EndTree();

    if (tailPath.empty()) {
        AddChild(newChild, beforeIndex);
    } else {
        auto currentNode = GetFactory()->CreateMap();
        AddChild(currentNode, beforeIndex);

        while (true) {
            Stroka prefix;
            ChopYPathPrefix(tailPath, &prefix, &tailPath);

            if (tailPath.empty()) {
                currentNode->AddChild(newChild, prefix);
                break;
            }

            auto intermediateNode = GetFactory()->CreateMap();
            currentNode->AddChild(intermediateNode, prefix);
            currentNode = intermediateNode;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

