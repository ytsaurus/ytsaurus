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
    TTokenizer tokenizer(path);
    tokenizer.ParseNext();
    Stroka name(tokenizer.Current().GetStringValue());

    if (name.Empty()) {
        ythrow yexception() << Sprintf("Child name cannot be empty");
    }

    auto child = FindChild(name);
    if (child) {
        return IYPathService::TResolveResult::There(~child, TYPath(tokenizer.GetCurrentSuffix()));
    }

    if (verb == "Set" || verb == "SetNode" || verb == "Create") {
        if (!tokenizer.ParseNext()) {
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
    TTokenizer tokenizer(path);
    tokenizer.ParseNext();
    Stroka childName(tokenizer.Current().GetStringValue());
    YASSERT(!tokenizer.ParseNext());
    YASSERT(!childName.empty());
    YASSERT(!FindChild(childName));
    AddChild(value, childName);
}

////////////////////////////////////////////////////////////////////////////////

IYPathService::TResolveResult TListNodeMixin::ResolveRecursive(
    const TYPath& path,
    const Stroka& verb)
{
    TTokenizer tokenizer(path);
    tokenizer.ParseNext();
    switch (tokenizer.GetCurrentType()) {
        case ETokenType::Plus:
            tokenizer.ParseNext();
            tokenizer.Current().CheckType(ETokenType::EndOfStream);
            return IYPathService::TResolveResult::Here("/" + path);

        case ETokenType::Integer: {
            int index = tokenizer.Current().GetIntegerValue();
            index = NormalizeAndCheckIndex(index);
            tokenizer.ParseNext();
            if (tokenizer.GetCurrentType() == ETokenType::Caret) {
                tokenizer.ParseNext();
                tokenizer.Current().CheckType(ETokenType::EndOfStream);
                return IYPathService::TResolveResult::Here("/" + path);
            } else {
                auto child = FindChild(index);
                YASSERT(child);
                return IYPathService::TResolveResult::There(~child, TYPath(tokenizer.GetCurrentInput()));
            }
        }

        case ETokenType::Caret:
            tokenizer.ParseNext();
            NormalizeAndCheckIndex(tokenizer.Current().GetIntegerValue());
            tokenizer.ParseNext();
            tokenizer.Current().CheckType(ETokenType::EndOfStream);
            return IYPathService::TResolveResult::Here("/" + path);

        default:
            ThrowUnexpectedToken(tokenizer.Current());
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

    TTokenizer tokenizer(path);
    tokenizer.ParseNext();
    switch (tokenizer.GetCurrentType()) {
        case ETokenType::Plus:
            YASSERT(!tokenizer.ParseNext());
            break;

        case ETokenType::Caret:
            tokenizer.ParseNext();
            beforeIndex = NormalizeAndCheckIndex(tokenizer.Current().GetIntegerValue());
            YASSERT(!tokenizer.ParseNext());
            break;

        case ETokenType::Integer:
            beforeIndex = NormalizeAndCheckIndex(tokenizer.Current().GetIntegerValue()) + 1;
            tokenizer.ParseNext();
            YASSERT(tokenizer.GetCurrentType() == ETokenType::Caret);
            YASSERT(!tokenizer.ParseNext());
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

