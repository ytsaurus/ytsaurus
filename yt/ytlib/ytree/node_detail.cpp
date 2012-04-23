#include "stdafx.h"
#include "node_detail.h"
#include "ypath_detail.h"
#include "ypath_service.h"
#include "tree_visitor.h"
#include "tree_builder.h"
#include "yson_writer.h"
#include "ypath_client.h"
#include "serialize.h"
#include "tokenizer.h"

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
    TTokenizer tokens(path);
    auto name = tokens[0].GetStringValue();

    if (name.Empty()) {
        ythrow yexception() << Sprintf("Child name cannot be empty");
    }

    auto child = FindChild(name);
    if (child) {
        return IYPathService::TResolveResult::There(~child, TYPath(tokens.GetSuffix(0)));
    }

    if (verb == "Set" || verb == "SetNode" || verb == "Create") {
        if (tokens[1].IsEmpty()) {
            return IYPathService::TResolveResult::Here("/" + path);
        }
    }

    ythrow yexception() << Sprintf("Key %s is not found", ~Stroka(name).Quote());
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
    TTokenizer tokens(path);
    auto childName = tokens[0].GetStringValue();
    YASSERT(tokens[1].IsEmpty());
    YASSERT(!childName.empty());
    YASSERT(!FindChild(childName));
    AddChild(value, childName);
}

////////////////////////////////////////////////////////////////////////////////

IYPathService::TResolveResult TListNodeMixin::ResolveRecursive(
    const TYPath& path,
    const Stroka& verb)
{
    TTokenizer tokens(path);
    switch (tokens[0].GetType()) {
        case ETokenType::Plus:
            tokens[1].CheckType(ETokenType::None);
            return IYPathService::TResolveResult::Here("/" + path);

        case ETokenType::Integer:
            if (tokens[1].GetType() == ETokenType::Caret) {
                NormalizeAndCheckIndex(tokens[0].GetIntegerValue());
                tokens[2].CheckType(ETokenType::None);
                return IYPathService::TResolveResult::Here("/" + path);
            } else {
                auto index = NormalizeAndCheckIndex(tokens[0].GetIntegerValue());
                auto child = FindChild(index);
                YASSERT(child);
                return IYPathService::TResolveResult::There(~child, TYPath(tokens.GetSuffix(0)));
            }

        case ETokenType::Caret:
            NormalizeAndCheckIndex(tokens[1].GetIntegerValue());
            tokens[2].CheckType(ETokenType::None);
            return IYPathService::TResolveResult::Here("/" + path);

        default:
            ThrowUnexpectedToken(tokens[0]);
            YUNREACHABLE();
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
    int beforeIndex = -1;

    TTokenizer tokens(path);
    switch (tokens[0].GetType()) {
        case ETokenType::Plus:
            YASSERT(tokens[1].IsEmpty());
            break;

        case ETokenType::Caret:
            beforeIndex = NormalizeAndCheckIndex(tokens[1].GetIntegerValue());
            YASSERT(tokens[2].IsEmpty());
            break;

        case ETokenType::Integer:
            YASSERT(tokens[1].GetType() == ETokenType::Caret);
            YASSERT(tokens[2].IsEmpty());
            beforeIndex = NormalizeAndCheckIndex(tokens[0].GetIntegerValue()) + 1;
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

