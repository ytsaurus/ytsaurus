#include "stdafx.h"
#include "node_detail.h"

#include "convert.h"
#include "ypath_detail.h"
#include "ypath_service.h"
#include "tree_visitor.h"
#include "tree_builder.h"
#include "yson_writer.h"
#include "ypath_client.h"
#include "tokenizer.h"
#include "ypath_format.h"

namespace NYT {
namespace NYTree {

using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

bool TNodeBase::IsWriteRequest(IServiceContextPtr context) const
{
    DECLARE_YPATH_SERVICE_WRITE_METHOD(Set);
    DECLARE_YPATH_SERVICE_WRITE_METHOD(Remove);
    return TYPathServiceBase::IsWriteRequest(context);
}

void TNodeBase::DoInvoke(IServiceContextPtr context)
{
    DISPATCH_YPATH_SERVICE_METHOD(Get);
    DISPATCH_YPATH_SERVICE_METHOD(Set);
    DISPATCH_YPATH_SERVICE_METHOD(Remove);
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

void TCompositeNodeMixin::SetRecursive(
    const TYPath& path,
    TReqSet* request,
    TRspSet* response,
    TCtxSet* context)
{
    UNUSED(response);

    auto factory = CreateFactory();
    auto value = ConvertToNode(TYsonString(request->value()), ~factory);
    SetRecursive(path, value);
    context->Reply();
}

void TCompositeNodeMixin::RemoveRecursive(
    const TYPath& path,
    TSupportsRemove::TReqRemove* request,
    TSupportsRemove::TRspRemove* response,
    TSupportsRemove::TCtxRemove* context)
{
    UNUSED(request);
    UNUSED(response);

    TTokenizer tokenizer(path);
    tokenizer.ParseNext();
    switch (tokenizer.CurrentToken().GetType()) {
        case RemoveAllToken:
            YASSERT(!tokenizer.ParseNext());
            Clear();
            break;

        default:
            ThrowUnexpectedToken(tokenizer.CurrentToken());
            YUNREACHABLE();
    }

    context->Reply();
}

////////////////////////////////////////////////////////////////////////////////

IYPathService::TResolveResult TMapNodeMixin::ResolveRecursive(
    const TYPath& path,
    const Stroka& verb)
{
    TTokenizer tokenizer(path);
    tokenizer.ParseNext();
    switch (tokenizer.GetCurrentType()) {
        case RemoveAllToken:
            tokenizer.ParseNext();
            tokenizer.CurrentToken().CheckType(ETokenType::EndOfStream);
            return IYPathService::TResolveResult::Here(
                TokenTypeToString(PathSeparatorToken) + path);

        case ETokenType::String: {
            Stroka name(tokenizer.CurrentToken().GetStringValue());

            if (name.Empty()) {
                ythrow yexception() << Sprintf("Child name cannot be empty");
            }

            auto child = FindChild(name);
            if (child) {
                return IYPathService::TResolveResult::There(
                    child, TYPath(tokenizer.GetCurrentSuffix()));
            }

            if (verb == "Set" || verb == "Create") {
                if (!tokenizer.ParseNext()) {
                    return IYPathService::TResolveResult::Here(
                        TokenTypeToString(PathSeparatorToken) + path);
                }
            }

            ythrow yexception() << Sprintf("Key %s is not found",
                ~Stroka(name).Quote());
        }

        default:
            ThrowUnexpectedToken(tokenizer.CurrentToken());
            YUNREACHABLE();
    }
}

void TMapNodeMixin::ListSelf(TReqList* request, TRspList* response, TCtxList* context)
{
    UNUSED(request);

    NYT::ToProto(response->mutable_keys(), GetKeys());
    context->Reply();
}

void TMapNodeMixin::SetRecursive(const TYPath& path, INodePtr value)
{
    TTokenizer tokenizer(path);
    tokenizer.ParseNext();
    Stroka childName(tokenizer.CurrentToken().GetStringValue());
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
        case ListAppendToken:
        case RemoveAllToken:
            tokenizer.ParseNext();
            tokenizer.CurrentToken().CheckType(ETokenType::EndOfStream);
            return IYPathService::TResolveResult::Here(
                TokenTypeToString(PathSeparatorToken) + path);

        case ETokenType::Integer: {
            int index = NormalizeAndCheckIndex(tokenizer.CurrentToken().GetIntegerValue());
            tokenizer.ParseNext();
            if (tokenizer.GetCurrentType() == ListInsertToken) {
                tokenizer.ParseNext();
                tokenizer.CurrentToken().CheckType(ETokenType::EndOfStream);
                return IYPathService::TResolveResult::Here(
                    TokenTypeToString(PathSeparatorToken) + path);
            } else {
                auto child = FindChild(index);
                YASSERT(child);
                return IYPathService::TResolveResult::There(child, TYPath(tokenizer.CurrentInput()));
            }
        }

        case ListInsertToken:
            tokenizer.ParseNext();
            NormalizeAndCheckIndex(tokenizer.CurrentToken().GetIntegerValue());
            tokenizer.ParseNext();
            tokenizer.CurrentToken().CheckType(ETokenType::EndOfStream);
            return IYPathService::TResolveResult::Here(
                TokenTypeToString(PathSeparatorToken) + path);

        default:
            ThrowUnexpectedToken(tokenizer.CurrentToken());
            YUNREACHABLE();
    }
}

void TListNodeMixin::SetRecursive(
    const TYPath& path,
    INodePtr value)
{
    int beforeIndex = -1;

    TTokenizer tokenizer(path);
    tokenizer.ParseNext();
    switch (tokenizer.GetCurrentType()) {
        case ListAppendToken:
            YASSERT(!tokenizer.ParseNext());
            break;

        case ListInsertToken:
            tokenizer.ParseNext();
            beforeIndex = NormalizeAndCheckIndex(tokenizer.CurrentToken().GetIntegerValue());
            YASSERT(!tokenizer.ParseNext());
            break;

        case ETokenType::Integer:
            beforeIndex = NormalizeAndCheckIndex(tokenizer.CurrentToken().GetIntegerValue()) + 1;
            tokenizer.ParseNext();
            YASSERT(tokenizer.GetCurrentType() == ListInsertToken);
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
        ythrow yexception() << Sprintf("Index %" PRId64 " is out of range", index);
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

