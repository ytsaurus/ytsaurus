#include "stdafx.h"
#include "node_detail.h"
#include "ypath_detail.h"
#include "ypath_service.h"
#include "tree_visitor.h"
#include "tree_builder.h"
#include "yson_writer.h"
#include "ypath_client.h"
#include "serialize.h"
#include "lexer.h"

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

void TNodeBase::DoInvoke(IServiceContext* context)
{
    DISPATCH_YPATH_SERVICE_METHOD(Get);
    DISPATCH_YPATH_SERVICE_METHOD(Set);
    DISPATCH_YPATH_SERVICE_METHOD(Remove);
    DISPATCH_YPATH_SERVICE_METHOD(GetNode);
    DISPATCH_YPATH_SERVICE_METHOD(SetNode);
    DISPATCH_YPATH_SERVICE_METHOD(List);
    TYPathServiceBase::DoInvoke(context);
}

void TNodeBase::GetSelf(TReqGet* request, TRspGet* response, TCtxGet* context)
{
    UNUSED(request);
    
    TStringStream stream;
    TYsonWriter writer(&stream);
    VisitTree(this, &writer, false);

    response->set_value(stream.Str());
    context->Reply();
}

void TNodeBase::GetNodeSelf(TReqGetNode* request, TRspGetNode* response, TCtxGetNode* context)
{
    UNUSED(request);

    response->set_value_ptr(reinterpret_cast<i64>(static_cast<INode*>(this)));
    context->Reply();
}

void TNodeBase::SetNodeSelf(TReqSetNode* request, TRspSetNode* response, TCtxSetNode* context)
{
    UNUSED(response);

    auto parent = GetParent();
    if (!parent) {
        ythrow yexception() << "Cannot replace the root";
    }

    auto value = reinterpret_cast<INode*>(request->value_ptr());
    parent->ReplaceChild(this, value);
    context->Reply();
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

////////////////////////////////////////////////////////////////////////////////

void TMapNodeMixin::ListSelf(TReqList* request, TRspList* response, TCtxList* context)
{
    UNUSED(request);

    NYT::ToProto(response->mutable_keys(), GetKeys());
    context->Reply();
}

IYPathService::TResolveResult TMapNodeMixin::ResolveRecursive(
    const TYPath& path,
    const Stroka& verb)
{
    TYPath suffixPath;
    auto token = ChopStringToken(path, &suffixPath);

    auto child = FindChild(token);
    if (child) {
        return IYPathService::TResolveResult::There(~child, suffixPath);
    }

    if (verb == "Set" || verb == "SetNode" || verb == "Create") {
        return IYPathService::TResolveResult::Here("/" + path);
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
    // Split path into tokens.
    // Check that no attribute markers are present.
    std::vector<Stroka> tokens;
    TYPath currentPath = path;

    while (true) {
        auto token = ChopStringToken(currentPath, &currentPath);
        tokens.push_back(token);

        auto separator = ChopToken(currentPath, &currentPath);
        if (separator.IsEmpty()) {
            break;
        } else if (separator.GetType() != ETokenType::Slash) {
            ythrow yexception() << Sprintf("Unexpected token %s of type %s",
                ~separator.ToString().Quote(),
                ~separator.GetType().ToString());
        }
    }

    // Check that the first token gives a unique key.
    auto firstToken = tokens.front();
    if (FindChild(firstToken)) {
        ythrow yexception() << Sprintf("Key %s already exists", ~firstToken.Quote());
    }

    // Make the actual changes.
    IMapNodePtr currentNode = this;
    for (auto it = tokens.begin(); it != tokens.end(); ++it) {
        auto token = *it;
        if (it == tokens.end() - 1) {
            // Final step: append the given value.
            YVERIFY(currentNode->AddChild(value, token));
        } else {
            // Intermediate step: create and append a map.
            auto intermediateNode = factory->CreateMap();
            YVERIFY(currentNode->AddChild(~intermediateNode, token));
            currentNode = intermediateNode;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

IYPathService::TResolveResult TListNodeMixin::ResolveRecursive(
    const TYPath& path,
    const Stroka& verb)
{
    TYPath suffixPath;

    auto token = ChopToken(path, &suffixPath);
    switch (token.GetType()) {
        case ETokenType::Plus: {
    		auto nextToken = ChopToken(suffixPath);
    		if (!nextToken.IsEmpty()) {
    			ythrow yexception() << Sprintf("Unexpected token %s of type %s",
    				~nextToken.ToString().Quote(),
    				~nextToken.GetType().ToString());
    		}
    		return IYPathService::TResolveResult::Here("/" + path);
        }
        case ETokenType::Int64: {
    		auto index = token.GetInt64Value();
    		auto count = GetChildCount();
    		if (index < 0) {
    			index += count;
    		}
    		if (index < 0 || index >= count) {
				ythrow yexception() << Sprintf("Index out of range (Index: %" PRId64 ", ChildCount: %" PRId64 ")",
					token.GetInt64Value(),
					count);
    		}
    		auto child = FindChild(index);
			YASSERT(child);
			return IYPathService::TResolveResult::There(~child, suffixPath);
        }
    	default:
            ythrow yexception() << Sprintf("Unexpected token %s of type %s",
                ~token.ToString().Quote(),
                ~token.GetType().ToString());
    }
}

void TListNodeMixin::SetRecursive(
    INodeFactory* factory,
    const TYPath& path,
    NProto::TReqSet* request)
{
    auto value = DeserializeFromYson(request->value(), factory);
    TListNodeMixin::SetRecursive(path, ~value);
}

void TListNodeMixin::SetRecursive(
    const TYPath& path,
    INode* value)
{
    TYPath suffixPath;
    auto token = ChopToken(path, &suffixPath);

    if (token.GetType() == ETokenType::Plus) {
        YASSERT(IsEmpty(suffixPath));
        AddChild(value);
    } else {
        // TODO(roizner): support syntaxis "list/^n" and "list/n^"
        ythrow yexception() << Sprintf("Unexpected token %s of type %s",
            ~token.ToString().Quote(),
            ~token.GetType().ToString());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

