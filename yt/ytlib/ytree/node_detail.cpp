#include "stdafx.h"
#include "node_detail.h"
#include "ypath_detail.h"
#include "ypath_service.h"
#include "tree_visitor.h"
#include "tree_builder.h"
#include "yson_writer.h"
#include "ypath_client.h"
#include "serialize.h"

namespace NYT {
namespace NYTree {

using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

bool TNodeBase::IsWriteRequest(IServiceContext* context) const
{
    DECLARE_YPATH_SERVICE_WRITE_METHOD(Set);
    DECLARE_YPATH_SERVICE_WRITE_METHOD(SetNode);
    DECLARE_YPATH_SERVICE_WRITE_METHOD(Remove);
    return TYPathServiceBase::IsWriteRequest(context);
}

IYPathService::TResolveResult TNodeBase::ResolveAttributes(const TYPath& path, const Stroka& verb)
{
    auto attributePath = ChopYPathAttributeMarker(path);
    if (IsFinalYPath(attributePath) &&
        verb != "Get" &&
        verb != "List" &&
        verb != "Remove")
    {
        ythrow TServiceException(EErrorCode::NoSuchVerb) <<
            Sprintf("Verb is not supported for attributes");
    } else {
        return TResolveResult::Here(path);
    }
}

void TNodeBase::DoInvoke(IServiceContext* context)
{
    Stroka verb = context->GetVerb();
    // TODO: use method table
    if (verb == "Get") {
        GetThunk(context);
    } else if (verb == "GetNode") {
        GetNodeThunk(context);
    } else if (verb == "Set") {
        SetThunk(context);
    } else if (verb == "SetNode") {
        SetNodeThunk(context);
    } else if (verb == "Remove") {
        RemoveThunk(context);
    } else {
        TYPathServiceBase::DoInvoke(context);
    }
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_RPC_SERVICE_METHOD(TNodeBase, Get)
{
    TYPath path = context->GetPath();
    if (IsFinalYPath(path)) {
        GetSelf(request, response, ~context);
    } else if (IsAttributeYPath(path)) {
        auto attributePath = ChopYPathAttributeMarker(path);
        if (IsFinalYPath(attributePath)) {
            TStringStream stream;
            TYsonWriter writer(&stream, EFormat::Binary);

            writer.OnBeginMap();

            auto virtualNames = GetVirtualAttributeNames();
            std::sort(virtualNames.begin(), virtualNames.end());
            FOREACH (const auto& attributeName, virtualNames) {
                auto attributeService = GetVirtualAttributeService(attributeName);
                auto attributeValue = SyncYPathGet(~attributeService, NYTree::RootMarker);
                writer.OnMapItem(attributeName);
                writer.OnRaw(attributeValue);
            }

            auto attributes = GetAttributes();
            if (attributes) {
                auto children = attributes->GetChildren();
                std::sort(children.begin(), children.end());
                FOREACH (const auto& pair, children) {
                    writer.OnMapItem(pair.first);
                    TTreeVisitor visitor(&writer);
                    visitor.Visit(~pair.second);
                }
            }

            writer.OnEndMap();

            response->set_value(stream.Str());
            context->Reply();
        } else {
            Stroka token;
            TYPath suffixPath;
            ChopYPathToken(attributePath, &token, &suffixPath);

            auto service = GetVirtualAttributeService(token);
            if (service) {
                response->set_value(SyncYPathGet(~service, "/" + suffixPath));
                context->Reply();
                return;
            }

            auto attributes = GetAttributes();
            if (!attributes) {
                ythrow yexception() << "Node has no attributes";
            }

            response->set_value(SyncYPathGet(~attributes, "/" + attributePath));
            context->Reply();
        }
    } else {
        GetRecursive(path, request, response, ~context);
    }
}

void TNodeBase::GetSelf(TReqGet* request, TRspGet* response, TCtxGet* context)
{
    UNUSED(request);
    
    TStringStream stream;
    TYsonWriter writer(&stream, EFormat::Binary);
    TTreeVisitor visitor(&writer, false);
    visitor.Visit(this);

    response->set_value(stream.Str());
    context->Reply();
}

void TNodeBase::GetRecursive(const TYPath& path, TReqGet* request, TRspGet* response, TCtxGet* context)
{
    UNUSED(path);
    UNUSED(request);
    UNUSED(response);
    UNUSED(context);

    ythrow yexception() << "Path must be final";
}


DEFINE_RPC_SERVICE_METHOD(TNodeBase, GetNode)
{
    TYPath path = context->GetPath();
    if (IsFinalYPath(path)) {
        GetNodeSelf(request, response, ~context);
    } else if (IsAttributeYPath(path)) {
        auto attributes = GetAttributes();
        if (!attributes) {
            ythrow yexception() << "Node has no attributes";
        }

        auto attributePath = ChopYPathAttributeMarker(path);
        auto value = SyncYPathGetNode(
            ~attributes,
            "/" + attributePath);
        response->set_value(reinterpret_cast<i64>(static_cast<INode*>(~value)));
        context->Reply();
    } else {
        GetNodeRecursive(path, request, response, ~context);
    }
}

void TNodeBase::GetNodeSelf(TReqGetNode* request, TRspGetNode* response, TCtxGetNode* context)
{
    UNUSED(request);

    response->set_value(reinterpret_cast<i64>(static_cast<INode*>(this)));
    context->Reply();
}

void TNodeBase::GetNodeRecursive(const TYPath& path, TReqGetNode* request, TRspGetNode* response, TCtxGetNode* context)
{
    UNUSED(path);
    UNUSED(request);
    UNUSED(response);
    UNUSED(context);

    ythrow yexception() << "Path must be final";
}


DEFINE_RPC_SERVICE_METHOD(TNodeBase, Set)
{
    TYPath path = context->GetPath();
    if (IsFinalYPath(path)) {
        SetSelf(request, response, ~context);
    } else if (IsAttributeYPath(path)) {
        auto attributePath = ChopYPathAttributeMarker(path);
        if (IsFinalYPath(attributePath)) {
            // TODO: fixme
            ythrow yexception() << "Cannot set the whole attribute list";    
        }

        auto value = request->value();

        Stroka token;
        TYPath suffixPath;
        ChopYPathToken(attributePath, &token, &suffixPath);

        auto service = GetVirtualAttributeService(token);
        if (service) {
            SyncYPathSet(~service, "/" + suffixPath, value);
            context->Reply();
            return;
        }

        auto attributes = EnsureAttributes();
        SyncYPathSet(
            ~attributes,
            "/" + attributePath,
            value);
        context->Reply();
    } else {
        SetRecursive(path, request, response, ~context);
    }
}

void TNodeBase::SetSelf(TReqSet* request, TRspSet* response, TCtxSet* context)
{
    UNUSED(request);
    UNUSED(response);
    UNUSED(context);

    ythrow TServiceException(EErrorCode::NoSuchVerb) <<
        "Verb is not supported";
}

void TNodeBase::SetRecursive(const TYPath& path, TReqSet* request, TRspSet* response, TCtxSet* context)
{
    UNUSED(path);
    UNUSED(request);
    UNUSED(response);
    UNUSED(context);

    ythrow yexception() << "Path must be final";
}


DEFINE_RPC_SERVICE_METHOD(TNodeBase, SetNode)
{
    TYPath path = context->GetPath();
    if (IsFinalYPath(path)) {
        SetNodeSelf(request, response, ~context);
    } else if (IsAttributeYPath(path)) {
        auto attributes = EnsureAttributes();
        auto value = reinterpret_cast<INode*>(request->value());
        auto attributePath = ChopYPathAttributeMarker(path);
        SyncYPathSetNode(
            ~attributes,
            "/" + attributePath,
            value);
        context->Reply();
    } else {
        SetNodeRecursive(path, request, response, ~context);
    }
}

void TNodeBase::SetNodeSelf(TReqSetNode* request, TRspSetNode* response, TCtxSetNode* context)
{
    UNUSED(request);
    UNUSED(response);
    UNUSED(context);

    auto parent = GetParent();
    if (!parent) {
        ythrow yexception() << "Cannot set the root";
    }

    auto value = reinterpret_cast<INode*>(request->value());
    parent->ReplaceChild(this, value);
    context->Reply();
}

void TNodeBase::SetNodeRecursive(const TYPath& path, TReqSetNode* request, TRspSetNode* response, TCtxSetNode* context)
{
    UNUSED(path);
    UNUSED(request);
    UNUSED(response);
    UNUSED(context);

    ythrow yexception() << "Path must be final";
}


DEFINE_RPC_SERVICE_METHOD(TNodeBase, Remove)
{
    TYPath path = context->GetPath();
    if (IsFinalYPath(path)) {
        RemoveSelf(request, response, ~context);
    } else if (IsAttributeYPath(path)) {
        auto attributePath = ChopYPathAttributeMarker(path);
        if (IsFinalYPath(attributePath)) {
            SetAttributes(NULL);
        } else {
            Stroka token;
            TYPath suffixPath;
            ChopYPathToken(attributePath, &token, &suffixPath);

            auto attributes = GetAttributes();
            if (!attributes) {
                ythrow yexception() << "Node has no attributes";
            }

            SyncYPathRemove(~attributes, "/" + attributePath);

            if (attributes->GetChildCount() == 0) {
                SetAttributes(NULL);
            }
        }
        context->Reply();
    } else {    
        RemoveRecursive(path, request, response, ~context);
    }
}

void TNodeBase::RemoveSelf(TReqRemove* request, TRspRemove* response, TCtxRemove* context)
{
    UNUSED(request);
    UNUSED(response);

    auto parent = GetParent();

    if (!parent) {
        ythrow yexception() << "Cannot remove the root";
    }

    parent->AsComposite()->RemoveChild(this);
    context->Reply();
}

void TNodeBase::RemoveRecursive(const TYPath& path, TReqRemove* request, TRspRemove* response, TCtxRemove* context)
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

IMapNode::TPtr TNodeBase::EnsureAttributes()
{
    auto attributes = GetAttributes();
    if (attributes) {
        return attributes;
    }

    auto factory = CreateFactory();
    attributes = factory->CreateMap();
    SetAttributes(~attributes);
    return attributes;
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

DEFINE_RPC_SERVICE_METHOD(TMapNodeMixin, List)
{
    UNUSED(request);

    FOREACH (const auto& pair, GetChildren()) {
        response->add_keys(pair.first);
    }

    context->Reply();
}

IYPathService::TResolveResult TMapNodeMixin::ResolveRecursive(
    const TYPath& path,
    const Stroka& verb)
{
    Stroka token;
    TYPath suffixPath;
    ChopYPathToken(path, &token, &suffixPath);

    auto child = FindChild(token);
    if (child) {
        return IYPathService::TResolveResult::There(~child, suffixPath);
    }

    if (verb == "Set" || verb == "SetNode" || verb == "Create") {
        return IYPathService::TResolveResult::Here(path);
    }

    ythrow yexception() << Sprintf("Key %s is not found", ~token.Quote());
}

void TMapNodeMixin::SetRecursive(
    INodeFactory* factory,
    const TYPath& path,
    NProto::TReqSet* request)
{
    auto value = DeserializeFromYson(request->value(), factory);
    TMapNodeMixin::SetRecursive(factory, path, ~value);
}

void TMapNodeMixin::SetRecursive(
    INodeFactory* factory,
    const TYPath& path,
    INode* value)
{
    IMapNode::TPtr currentNode = this;
    TYPath currentPath = path;

    while (true) {
        Stroka token;
        TYPath suffixPath;
        ChopYPathToken(currentPath, &token, &suffixPath);

        if (suffixPath.empty()) {
            if (!currentNode->AddChild(value, token)) {
                ythrow yexception() << Sprintf("Key %s already exists", ~token.Quote());
            }
            break;
        }

        auto intermediateNode = factory->CreateMap();
        if (!currentNode->AddChild(~intermediateNode, token)) {
            ythrow yexception() << Sprintf("Key %s already exists", ~token.Quote());
        }

        currentNode = intermediateNode;
        currentPath = suffixPath;
    }
}

////////////////////////////////////////////////////////////////////////////////

IYPathService::TResolveResult TListNodeMixin::ResolveRecursive(
    const TYPath& path,
    const Stroka& verb)
{
    Stroka token;
    TYPath suffixPath;
    ChopYPathToken(path, &token, &suffixPath);

    if (token.empty()) {
        ythrow yexception() << "Child index is empty";
    }

    char lastPrefixCh = token[token.length() - 1];
    if ((verb == "Set" || verb == "SetNode" || verb == "Create") &&
        (lastPrefixCh == '+' || lastPrefixCh == '-'))
    {
        return IYPathService::TResolveResult::Here(path);
    } else {
        int index = ParseChildIndex(token);
        auto child = FindChild(index);
        YASSERT(child);
        return IYPathService::TResolveResult::There(~child, suffixPath);
    }
}

void TListNodeMixin::SetRecursive(
    INodeFactory* factory,
    const TYPath& path,
    NProto::TReqSet* request)
{
    auto value = DeserializeFromYson(request->value(), factory);
    SetRecursive(factory, path, ~value);
}

void TListNodeMixin::SetRecursive(
    INodeFactory* factory,
    const TYPath& path,
    INode* value)
{
    INode::TPtr currentNode = this;
    TYPath currentPath = path;

    Stroka token;
    TYPath suffixPath;
    ChopYPathToken(currentPath, &token, &suffixPath);

    if (token.empty()) {
        ythrow yexception() << "Child index is empty";
    }

    if (token == "+") {
        return CreateChild(factory, GetChildCount(), suffixPath, value);
    } else if (token == "-") {
        return CreateChild(factory, 0, suffixPath, value);
    }

    char lastPrefixCh = token[token.length() - 1];
    if (lastPrefixCh != '+' && lastPrefixCh != '-') {
        ythrow yexception() << "Insertion point expected";
    }

    int index = ParseChildIndex(TStringBuf(token.begin(), token.length() - 1));
    switch (lastPrefixCh) {
        case '+':
            CreateChild(factory, index + 1, suffixPath, value);
            break;
        case '-':
            CreateChild(factory, index, suffixPath, value);
            break;

        default:
            YUNREACHABLE();
    }
}

void TListNodeMixin::CreateChild(
    INodeFactory* factory,
    int beforeIndex,
    const TYPath& path,
    INode* value)
{
    if (IsFinalYPath(path)) {
        AddChild(value, beforeIndex);
    } else {
        auto currentNode = factory->CreateMap();
        auto currentPath = path;
        AddChild(~currentNode, beforeIndex);

        while (true) {
            Stroka token;
            TYPath suffixPath;
            ChopYPathToken(currentPath, &token, &suffixPath);

            if (IsFinalYPath(suffixPath)) {
                YVERIFY(currentNode->AddChild(value, token));
                break;
            }

            auto intermediateNode = factory->CreateMap();
            YVERIFY(currentNode->AddChild(~intermediateNode, token));

            currentNode = intermediateNode;
            currentPath = suffixPath;
        }
    }
}

int TListNodeMixin::ParseChildIndex(const TStringBuf& str)
{
    int index;
    try {
        index = FromString<int>(str);
    } catch (const std::exception& ex) {
        ythrow yexception() << Sprintf("Failed to parse index %s\n%s",
            ~Stroka(str).Quote(),
            ex.what());
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

