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
    
    auto withAttributes = request->Attributes().Get<bool>("with_attributes", false);

    TStringStream stream;
    TYsonWriter writer(&stream);
    VisitTree(this, &writer, withAttributes);

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

    if (token.empty()) {
        ythrow yexception() << Sprintf("Child name cannot be empty");
    }

    auto child = FindChild(token);
    if (child) {
        return IYPathService::TResolveResult::There(~child, suffixPath);
    }

    if (verb == "Set" || verb == "SetNode" || verb == "Create") {
        if (IsEmpty(suffixPath)) {
            return IYPathService::TResolveResult::Here("/" + path);
        }
    }

    ythrow yexception() << Sprintf("Key %s is not found", ~token.Quote());
}

void TMapNodeMixin::SetRecursive(
    INodeFactory* factory,
    const TYPath& path,
    NProto::TReqSet* request)
{
    auto value = DeserializeFromYson(request->value(), factory);
    TMapNodeMixin::SetRecursive(path, ~value);
}

void TMapNodeMixin::SetRecursive(
    const TYPath& path,
    INode* value)
{
    TYPath suffixPath;
    auto token = ChopToken(path, &suffixPath);

    if (token.GetType() != ETokenType::String) {
        ythrow yexception() << Sprintf("Unexpected token %s of type %s",
            ~token.ToString().Quote(),
            ~token.GetType().ToString());
    }

    YASSERT(IsEmpty(suffixPath));
    const auto& childName = token.GetStringValue();
    YASSERT(!childName.empty());
    YASSERT(!FindChild(childName));
    AddChild(value, childName);
}

////////////////////////////////////////////////////////////////////////////////

IYPathService::TResolveResult TListNodeMixin::ResolveRecursive(
    const TYPath& path,
    const Stroka& verb)
{
    TYPath suffixPath1, suffixPath2;
    auto token1 = ChopToken(path, &suffixPath1);
    auto token2 = ChopToken(suffixPath1, &suffixPath2);
    if (token1.GetType() == ETokenType::Integer && token2.GetType() == ETokenType::Caret) {
        std::swap(token1, token2);
    }

    switch (token1.GetType()) {
        case ETokenType::Plus:
            if (!token2.IsEmpty()) {
                ythrow yexception() << Sprintf("Unexpected token %s of type %s",
                    ~token2.ToString().Quote(),
                    ~token2.GetType().ToString());
            }
            return IYPathService::TResolveResult::Here("/" + path);

        case ETokenType::Integer: {
            YASSERT(token2.GetType() != ETokenType::Caret);

            auto index = NormalizeAndCheckIndex(token1.GetIntegerValue());
            auto child = FindChild(index);
            YASSERT(child);
            return IYPathService::TResolveResult::There(~child, suffixPath1);
        }
        case ETokenType::Caret: {
            NormalizeAndCheckIndex(token2.GetIntegerValue());
            auto token3 = ChopToken(suffixPath2);
            if (!token3.IsEmpty()) {
                ythrow yexception() << Sprintf("Unexpected token %s of type %s",
                    ~token3.ToString().Quote(),
                    ~token3.GetType().ToString());
            }
            return IYPathService::TResolveResult::Here("/" + path);
        }
        default:
            ythrow yexception() << Sprintf("Unexpected token %s of type %s",
                ~token1.ToString().Quote(),
                ~token1.GetType().ToString());
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
    auto token1 = ChopToken(path, &suffixPath);
    auto token2 = ChopToken(suffixPath, &suffixPath);

    int beforeIndex = -1;

    switch (token1.GetType()) {
        case ETokenType::Plus:
            YASSERT(token2.IsEmpty());
            break;

        case ETokenType::Caret:
            YASSERT(token2.GetType() == ETokenType::Integer);
            YASSERT(IsEmpty(suffixPath));
            beforeIndex = NormalizeAndCheckIndex(token2.GetIntegerValue());
            break;

        case ETokenType::Integer:
            YASSERT(token2.GetType() == ETokenType::Caret);
            YASSERT(IsEmpty(suffixPath));
            beforeIndex = NormalizeAndCheckIndex(token1.GetIntegerValue()) + 1;
            if (beforeIndex == GetChildCount()) {
                beforeIndex = -1;
            }
            break;

        default:
            YUNREACHABLE();
    }

    AddChild(value, beforeIndex);
}

i64 TListNodeMixin::NormalizeAndCheckIndex(i64 index) const
{
    auto result = index;
    auto count = GetChildCount();
    if (result < 0) {
        result += count;
    }
    if (result < 0 || result >= count) {
        ythrow yexception() << Sprintf("Index out of range (Index: %" PRId64 ", ChildCount: %" PRId32 ")",
            index,
            count);
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

