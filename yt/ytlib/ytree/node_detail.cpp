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

#include <ytlib/ypath/token.h>
#include <ytlib/ypath/tokenizer.h>

#include <ytlib/misc/protobuf_helpers.h>

namespace NYT {
namespace NYTree {

using namespace NRpc;
using namespace NYPath;

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
    THROW_ERROR_EXCEPTION("%s has no child with key: %s",
        ~GetNodePathHelper(node),
        ~ToYPathLiteral(key));
}

void ThrowNoSuchChildIndex(IConstNodePtr node, int index)
{
    THROW_ERROR_EXCEPTION("%s has no child with index: %d",
        ~GetNodePathHelper(node),
        index);
}

void ThrowVerbNotSuppored(IConstNodePtr node, const Stroka& verb)
{
    THROW_ERROR_EXCEPTION(
        NRpc::EErrorCode::NoSuchVerb,
        "%s does not support verb: %s",
        ~GetNodePathHelper(node),
        ~verb);
}

void ThrowVerbNotSuppored(const Stroka& verb)
{
    THROW_ERROR_EXCEPTION(
        NRpc::EErrorCode::NoSuchVerb,
        "Verb is not supported: %s",
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

void TNodeBase::GetSelf(TReqGet* request, TRspGet* response, TCtxGetPtr context)
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

void TNodeBase::RemoveSelf(TReqRemove* request, TRspRemove* response, TCtxRemovePtr context)
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
    const NYPath::TYPath& path,
    IServiceContextPtr context)
{
    UNUSED(context);

    if (context->GetVerb() == "Exists") {
        return TResolveResult::Here(path);
    }
    ThrowCannotHaveChildren(this);
    YUNREACHABLE();
}

void TNodeBase::ExistsSelf(TReqExists* request, TRspExists* response, TCtxExistsPtr context)
{
    UNUSED(request);

    response->set_value(true);
    context->Reply();
}

void TNodeBase::ExistsRecursive(const NYTree::TYPath& path, TReqExists* request, TRspExists* response, TCtxExistsPtr context)
{
    UNUSED(request);

    response->set_value(false);
    context->Reply();
}

////////////////////////////////////////////////////////////////////////////////

void TCompositeNodeMixin::SetRecursive(
    const TYPath& path,
    TReqSet* request,
    TRspSet* response,
    TCtxSetPtr context)
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
    TSupportsRemove::TCtxRemovePtr context)
{
    UNUSED(request);
    UNUSED(response);

    NYPath::TTokenizer tokenizer(path);
    tokenizer.Advance();
    if (tokenizer.GetType() != NYPath::ETokenType::Literal || tokenizer.GetToken() != WildcardToken) {
        tokenizer.ThrowUnexpected();
    }

    tokenizer.Advance();
    tokenizer.Expect(NYPath::ETokenType::EndOfStream);

    Clear();

    context->Reply();
}

////////////////////////////////////////////////////////////////////////////////

IYPathService::TResolveResult TMapNodeMixin::ResolveRecursive(
    const TYPath& path,
    IServiceContextPtr context)
{
    const auto& verb = context->GetVerb();

    NYPath::TTokenizer tokenizer(path);
    tokenizer.Advance();
    tokenizer.Expect(NYPath::ETokenType::Literal);

    if (tokenizer.GetToken() == WildcardToken) {
        if (verb != "Remove") {
            THROW_ERROR_EXCEPTION("\"%s\" is only allowed for Remove verb",
                WildcardToken);
        }

        tokenizer.Advance();
        tokenizer.Expect(NYPath::ETokenType::EndOfStream);

        return IYPathService::TResolveResult::Here("/" + path);
    } else {
        auto key = tokenizer.GetLiteralValue();
        if (key.Empty()) {
            THROW_ERROR_EXCEPTION("Child key cannot be empty");
        }

        auto child = FindChild(key);
        if (!child) {
            if ((verb == "Set" || verb == "Create" || verb == "Copy") &&
                tokenizer.Advance() == NYPath::ETokenType::EndOfStream ||
                verb == "Exists")
            {
                return IYPathService::TResolveResult::Here("/" + path);
            } else {
                ThrowNoSuchChildKey(this, key);
            }
        }

        return IYPathService::TResolveResult::There(child, tokenizer.GetSuffix());
    }
}

void TMapNodeMixin::ListSelf(TReqList* request, TRspList* response, TCtxListPtr context)
{
    if (request->attributes_size() == 0) {
        // Fast path.
        response->set_keys(ConvertToYsonString(GetKeys()).Data());
    } else {
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
    }

    context->Reply();
}

void TMapNodeMixin::SetRecursive(const TYPath& path, INodePtr value)
{
    NYPath::TTokenizer tokenizer(path);
    tokenizer.Advance();
    tokenizer.Expect(NYPath::ETokenType::Literal);
    auto key = tokenizer.GetLiteralValue();
    
    tokenizer.Advance();
    tokenizer.Expect(NYPath::ETokenType::EndOfStream);

    AddChild(value, key);
}

////////////////////////////////////////////////////////////////////////////////

IYPathService::TResolveResult TListNodeMixin::ResolveRecursive(
    const TYPath& path,
    IServiceContextPtr context)
{
    NYPath::TTokenizer tokenizer(path);
    tokenizer.Advance();
    tokenizer.Expect(NYPath::ETokenType::Literal);

    const auto& token = tokenizer.GetToken();
    if (token == WildcardToken ||
        token == ListBeginToken ||
        token == ListEndToken)
    {
        tokenizer.Advance();
        tokenizer.Expect(NYPath::ETokenType::EndOfStream);

        return IYPathService::TResolveResult::Here("/" + path);
    } else if (token.has_prefix(ListBeforeToken) ||
               token.has_prefix(ListAfterToken))
    {
        auto indexToken = ExtractListIndex(token);
        int index = ParseListIndex(indexToken);
        AdjustChildIndex(index);
        
        tokenizer.Advance();
        tokenizer.Expect(NYPath::ETokenType::EndOfStream);

        return IYPathService::TResolveResult::Here("/" + path);
    } else {
        int index = ParseListIndex(token);
        int adjustedIndex = AdjustChildIndex(index);
        auto child = FindChild(adjustedIndex);
        const auto& verb = context->GetVerb();
        if (!child && verb == "Exists") {
        	return IYPathService::TResolveResult::Here("/" + path);
        }
        return IYPathService::TResolveResult::There(child, tokenizer.GetSuffix());
    }
}

void TListNodeMixin::SetRecursive(
    const TYPath& path,
    INodePtr value)
{
    int beforeIndex;

    NYPath::TTokenizer tokenizer(path);
    tokenizer.Advance();
    const auto& token = tokenizer.GetToken();
    if (token.has_prefix(ListBeginToken)) {
        beforeIndex = 0;
    } else if (token.has_prefix(ListEndToken)) {
        beforeIndex = GetChildCount();
    } else if (token.has_prefix(ListBeforeToken) ||
               token.has_prefix(ListAfterToken))
    {
        auto indexToken = ExtractListIndex(token);
        int index = ParseListIndex(indexToken);
        beforeIndex = AdjustChildIndex(index);
        if (token.has_prefix(ListAfterToken)) {
            ++beforeIndex;
        }
    } else {
        tokenizer.ThrowUnexpected();
    }

    tokenizer.Advance();
    tokenizer.Expect(NYPath::ETokenType::EndOfStream);

    AddChild(value, beforeIndex);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

