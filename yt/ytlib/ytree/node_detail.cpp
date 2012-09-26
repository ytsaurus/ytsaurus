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

#include <ytlib/misc/protobuf_helpers.h>

namespace NYT {
namespace NYTree {

using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

namespace {

Stroka GetNodePathHelper(IConstNodePtr node)
{
    auto path = node->GetPath();
    return path.empty() ? "Node" : Sprintf("Node %s", ~path);
}

} // namespace

void ThrowInvalidNodeType(IConstNodePtr node, ENodeType expectedType, ENodeType actualType)
{
    THROW_ERROR_EXCEPTION("%s has invalid type: expected %s, actual %s",
        ~GetNodePathHelper(node),
        ~expectedType.ToString(),
        ~actualType.ToString());
}

void ThrowNoSuchChildKey(IConstNodePtr node, const Stroka& key)
{
    THROW_ERROR_EXCEPTION("%s has no child with key %s",
        ~GetNodePathHelper(node),
        ~YsonizeString(key, EYsonFormat::Text));
}

void ThrowNoSuchChildIndex(IConstNodePtr node, int index)
{
    THROW_ERROR_EXCEPTION("%s has no child with index %d",
        ~GetNodePathHelper(node),
        index);
}

void ThrowVerbNotSuppored(IConstNodePtr node, const Stroka& verb)
{
    THROW_ERROR_EXCEPTION(
        NRpc::EErrorCode::NoSuchVerb,
        "%s does not support verb %s",
        ~GetNodePathHelper(node),
        ~verb);
}

void ThrowCannotHaveChildren(IConstNodePtr node)
{
    THROW_ERROR_EXCEPTION("%s cannot have children",
        ~GetNodePathHelper(node));
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
    DISPATCH_YPATH_SERVICE_METHOD(Exists);
    TYPathServiceBase::DoInvoke(context);
}

void TNodeBase::GetSelf(TReqGet* request, TRspGet* response, TCtxGet* context)
{
    TNullable< std::vector<Stroka> > attributesToVisit;

    YCHECK(!(request->attributes_size() > 0 && request->all_attributes()));

    if (request->attributes_size() > 0) {
        attributesToVisit = NYT::FromProto<Stroka>(request->attributes());
        std::sort(attributesToVisit->begin(), attributesToVisit->end());
        attributesToVisit->erase(
            std::unique(attributesToVisit->begin(), attributesToVisit->end()),
            attributesToVisit->end());
    }

    TStringStream stream;
    TYsonWriter writer(&stream);

    VisitTree(this,
        &writer,
        attributesToVisit || request->all_attributes(),
        attributesToVisit ? attributesToVisit.GetPtr() : NULL);

    response->set_value(stream.Str());
    context->Reply();
}

void TNodeBase::RemoveSelf(TReqRemove* request, TRspRemove* response, TCtxRemove* context)
{
    UNUSED(request);
    UNUSED(response);

    auto parent = GetParent();
    if (!parent) {
        THROW_ERROR_EXCEPTION("Cannot remove the root");
    }

    parent->AsComposite()->RemoveChild(this);
    context->Reply();
}

IYPathService::TResolveResult TNodeBase::ResolveRecursive(
    const NYTree::TYPath& path,
    IServiceContextPtr context)
{
    UNUSED(context);

    if (context->GetVerb() == "Exists") {
        return TResolveResult::Here(path);
    }
    ThrowCannotHaveChildren(this);
    YUNREACHABLE();
}

void TNodeBase::ExistsSelf(TReqExists* request, TRspExists* response, TCtxExists* context)
{
    UNUSED(request);
    YCHECK(!TTokenizer(context->GetPath()).ParseNext());
    response->set_value(true);
    context->Reply();
}

void TNodeBase::ExistsRecursive(const NYTree::TYPath& path, TReqExists* request, TRspExists* response, TCtxExists* context)
{
    UNUSED(request);
    UNUSED(response);
    response->set_value(false);
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
    IServiceContextPtr context)
{
    const auto& verb = context->GetVerb();
    TTokenizer tokenizer(path);
    tokenizer.ParseNext();
    switch (tokenizer.GetCurrentType()) {
        case WildcardToken:
            if (verb != "Remove") {
                THROW_ERROR_EXCEPTION("Wildcard is only allowed for Remove verb");
            }
            tokenizer.ParseNext();
            tokenizer.CurrentToken().CheckType(ETokenType::EndOfStream);
            return IYPathService::TResolveResult::Here(
                TokenTypeToString(PathSeparatorToken) + path);

        case ETokenType::String: {
            Stroka key(tokenizer.CurrentToken().GetStringValue());
            if (key.Empty()) {
                THROW_ERROR_EXCEPTION("Child key cannot be empty");
            }

            auto child = FindChild(key);
            if (child) {
                return IYPathService::TResolveResult::There(
                    child, TYPath(tokenizer.GetCurrentSuffix()));
            }

            if (verb == "Set" ||
                verb == "Create" ||
                verb == "Copy" ||
                verb == "Exists")
            {
                // In case of Exists return false anywhere
                if (!tokenizer.ParseNext() || verb == "Exists") {
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
    if (request->attributes_size() == 0) {
        // Fast path.
        response->set_keys(ConvertToYsonString(GetKeys()).Data());
        context->Reply();
        return;
    }

    auto attributesToVisit = NYT::FromProto<Stroka>(request->attributes());

    std::sort(attributesToVisit.begin(), attributesToVisit.end());
    attributesToVisit.erase(
        std::unique(attributesToVisit.begin(), attributesToVisit.end()),
        attributesToVisit.end());

    TStringStream stream;
    TYsonWriter writer(&stream);

    writer.OnBeginList();
    FOREACH (const auto& pair, GetChildren()) {
        const auto& key = pair.First();
        const auto& node = pair.Second();
        const auto& attributes = node->Attributes();

        writer.OnListItem();
        writer.OnBeginAttributes();

        FOREACH (const auto& attributeKey, attributesToVisit) {
            auto value = attributes.FindYson(attributeKey);
            if  (value) {
                writer.OnKeyedItem(attributeKey);
                Consume(*value, &writer);
            }
        }

        writer.OnEndAttributes();
        writer.OnStringScalar(key);
    }
    writer.OnEndList();

    response->set_keys(stream.Str());
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
    IServiceContextPtr context)
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
            const auto& verb = context->GetVerb();
            
            int index = AdjustAndValidateChildIndex(tokenizer.CurrentToken().GetIntegerValue());
            if (!FindChild(index) && verb == "Exists") {
                return IYPathService::TResolveResult::Here(TokenTypeToString(PathSeparatorToken) + path);
            }
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
            int index = AdjustAndValidateChildIndex(tokenizer.CurrentToken().GetIntegerValue());
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
            beforeIndex = AdjustAndValidateChildIndex(tokenizer.CurrentToken().GetIntegerValue());
            YASSERT(!tokenizer.ParseNext());
            break;

        case ETokenType::Integer:
            beforeIndex = AdjustAndValidateChildIndex(tokenizer.CurrentToken().GetIntegerValue());
            ++beforeIndex;
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

