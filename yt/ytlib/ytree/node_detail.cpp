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

void ThrowInvalidNodeType(IConstNodePtr node, ENodeType expectedType, ENodeType actualType)
{
    ythrow yexception() << Sprintf("Node %s has invalid type: expected %s, actual %s",
        ~node->GetPath(),
        ~expectedType.ToString(),
        ~actualType.ToString());
}

void ThrowNoSuchChildKey(IConstNodePtr node, const Stroka& key)
{
    ythrow yexception() << Sprintf("Node %s has no child with key %s",
        ~node->GetPath(),
        ~YsonizeString(key, EYsonFormat::Text));
}

void ThrowNoSuchChildIndex(IConstNodePtr node, int index)
{
    ythrow yexception() << Sprintf("Node %s has no child with index %d",
        ~node->GetPath(),
        index);
}

void ThrowVerbNotSuppored(IConstNodePtr node, const Stroka& verb)
{
    ythrow TServiceException(TError(
        NRpc::EErrorCode::NoSuchVerb,
        "Node %s does not support verb %s",
        ~node->GetPath(),
        ~verb));
}

void ThrowNoChildren(IConstNodePtr node)
{
    ythrow yexception() << Sprintf("Node %s cannot have children",
        ~node->GetPath());
}

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

IYPathService::TResolveResult TNodeBase::ResolveRecursive(const NYTree::TYPath& path, const Stroka& verb)
{
    ThrowNoChildren(this);
    YUNREACHABLE();
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
        case WildcardToken:
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
        case WildcardToken:
            if (verb != "Remove") {
                ythrow yexception() << "Wildcard is only allowed for Remove verb";
            }
            tokenizer.ParseNext();
            tokenizer.CurrentToken().CheckType(ETokenType::EndOfStream);
            return IYPathService::TResolveResult::Here(
                TokenTypeToString(PathSeparatorToken) + path);

        case ETokenType::String: {
            Stroka key(tokenizer.CurrentToken().GetStringValue());
            if (key.Empty()) {
                ythrow yexception() << Sprintf("Child key cannot be empty");
            }

            auto child = FindChild(key);
            if (child) {
                return IYPathService::TResolveResult::There(
                    child, TYPath(tokenizer.GetCurrentSuffix()));
            }

            if (verb == "Set" ||
                verb == "Create" ||
                verb == "Copy")
            {
                if (!tokenizer.ParseNext()) {
                    return IYPathService::TResolveResult::Here(
                        TokenTypeToString(PathSeparatorToken) + path);
                }
            }

            ThrowNoSuchChildKey(this, key);
        }

        default:
            ThrowUnexpectedToken(tokenizer.CurrentToken());
            YUNREACHABLE();
    }
}

void TMapNodeMixin::ListSelf(TReqList* request, TRspList* response, TCtxList* context)
{
    UNUSED(request);

    auto keys = GetKeys();
    response->set_keys(ConvertToYsonString(keys).Data());
    context->Reply();
}

void TMapNodeMixin::SetRecursive(const TYPath& path, INodePtr value)
{
    TTokenizer tokenizer(path);
    tokenizer.ParseNext();
    Stroka key(tokenizer.CurrentToken().GetStringValue());
    YASSERT(!tokenizer.ParseNext());
    YASSERT(!key.empty());
    YASSERT(!FindChild(key));
    AddChild(value, key);
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
        case WildcardToken:
            tokenizer.ParseNext();
            tokenizer.CurrentToken().CheckType(ETokenType::EndOfStream);
            return IYPathService::TResolveResult::Here(TokenTypeToString(PathSeparatorToken) + path);

        case ETokenType::Integer: {
            int index = tokenizer.CurrentToken().GetIntegerValue();
            auto child = GetChild(index);
            tokenizer.ParseNext();
            if (tokenizer.GetCurrentType() == ListInsertToken) {
                tokenizer.ParseNext();
                tokenizer.CurrentToken().CheckType(ETokenType::EndOfStream);
                return IYPathService::TResolveResult::Here(TokenTypeToString(PathSeparatorToken) + path);
            } else {
                return IYPathService::TResolveResult::There(child, TYPath(tokenizer.CurrentInput()));
            }
        }

        case ListInsertToken: {
            tokenizer.ParseNext();
            int index = tokenizer.CurrentToken().GetIntegerValue();
            GetChild(index); // just a check
            tokenizer.ParseNext();
            tokenizer.CurrentToken().CheckType(ETokenType::EndOfStream);
            return IYPathService::TResolveResult::Here(TokenTypeToString(PathSeparatorToken) + path);
        }

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
            beforeIndex = tokenizer.CurrentToken().GetIntegerValue();
            GetChild(beforeIndex); // just a check
            YASSERT(!tokenizer.ParseNext());
            break;

        case ETokenType::Integer:
            beforeIndex = tokenizer.CurrentToken().GetIntegerValue();
            GetChild(beforeIndex); // just a check
            ++beforeIndex;
            if (beforeIndex == GetChildCount()) {
                beforeIndex = -1;
            }
            tokenizer.ParseNext();
            YASSERT(tokenizer.GetCurrentType() == ListInsertToken);
            YASSERT(!tokenizer.ParseNext());
            break;

        default:
            YUNREACHABLE();
    }

    AddChild(value, beforeIndex);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

