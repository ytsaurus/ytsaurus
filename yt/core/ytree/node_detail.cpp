#include "stdafx.h"
#include "node_detail.h"

#include "convert.h"
#include "ypath_detail.h"
#include "ypath_service.h"
#include "tree_visitor.h"
#include "tree_builder.h"
#include "ypath_client.h"

#include <core/misc/protobuf_helpers.h>

#include <core/yson/writer.h>
#include <core/yson/tokenizer.h>

#include <core/ypath/token.h>
#include <core/ypath/tokenizer.h>

#include <core/misc/protobuf_helpers.h>

namespace NYT {
namespace NYTree {

using namespace NRpc;
using namespace NYPath;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

bool TNodeBase::DoInvoke(IServiceContextPtr context)
{
    DISPATCH_YPATH_SERVICE_METHOD(GetKey);
    DISPATCH_YPATH_SERVICE_METHOD(Get);
    DISPATCH_YPATH_SERVICE_METHOD(Set);
    DISPATCH_YPATH_SERVICE_METHOD(Remove);
    DISPATCH_YPATH_SERVICE_METHOD(List);
    DISPATCH_YPATH_SERVICE_METHOD(Exists);
    return TYPathServiceBase::DoInvoke(context);
}

void TNodeBase::GetSelf(TReqGet* request, TRspGet* response, TCtxGetPtr context)
{
    auto attributeFilter =
        request->has_attribute_filter()
        ? NYT::FromProto<TAttributeFilter>(request->attribute_filter())
        : TAttributeFilter::None;

    bool ignoreOpaque = request->ignore_opaque();

    context->SetRequestInfo("AttributeFilterMode: %v, IgnoreOpaque: %v",
        attributeFilter.Mode,
        ignoreOpaque);

    ValidatePermission(EPermissionCheckScope::This, EPermission::Read);

    TStringStream stream;
    TYsonWriter writer(&stream);

    VisitTree(
        this,
        &writer,
        attributeFilter,
        false,
        ignoreOpaque);

    response->set_value(stream.Str());

    context->Reply();
}

void TNodeBase::GetKeySelf(TReqGetKey* request, TRspGetKey* response, TCtxGetKeyPtr context)
{
    UNUSED(request);

    context->SetRequestInfo();

    ValidatePermission(EPermissionCheckScope::This, EPermission::Read);

    auto parent = GetParent();
    if (!parent) {
        THROW_ERROR_EXCEPTION("Node has no parent");
    }

    Stroka key;
    switch (parent->GetType()) {
        case ENodeType::Map:
            key = parent->AsMap()->GetChildKey(this);
            break;

        case ENodeType::List:
            key = ToString(parent->AsList()->GetChildIndex(this));
            break;

        default:
            YUNREACHABLE();
    }

    context->SetResponseInfo("Key: %v", ~key);
    response->set_value(key);

    context->Reply();
}

void TNodeBase::RemoveSelf(TReqRemove* request, TRspRemove* response, TCtxRemovePtr context)
{
    UNUSED(response);

    context->SetRequestInfo();

    auto parent = GetParent();
    if (!parent) {
        ThrowCannotRemoveRoot();
    }

    ValidatePermission(EPermissionCheckScope::This, EPermission::Write);
    ValidatePermission(EPermissionCheckScope::Descendants, EPermission::Write);
    ValidatePermission(EPermissionCheckScope::Parent, EPermission::Write);

    bool isComposite = (GetType() == ENodeType::Map || GetType() == ENodeType::List);
    if (!request->recursive() && isComposite && AsComposite()->GetChildCount() > 0) {
        THROW_ERROR_EXCEPTION("Cannot remove non-empty composite node");
    }

    parent->AsComposite()->RemoveChild(this);

    context->Reply();
}

IYPathService::TResolveResult TNodeBase::ResolveRecursive(
    const NYPath::TYPath& path,
    IServiceContextPtr context)
{
    if (context->GetMethod() == "Exists") {
        return TResolveResult::Here(path);
    }

    ThrowCannotHaveChildren(this);
    YUNREACHABLE();
}

////////////////////////////////////////////////////////////////////////////////

void TCompositeNodeMixin::SetRecursive(
    const TYPath& path,
    TReqSet* request,
    TRspSet* response,
    TCtxSetPtr context)
{
    UNUSED(response);

    context->SetRequestInfo();

    ValidatePermission(EPermissionCheckScope::This, EPermission::Write);

    auto factory = CreateFactory();
    auto value = ConvertToNode(TYsonString(request->value()), factory.Get());
    SetChild(factory, "/" + path, value, false);
    factory->Commit();

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

    context->SetRequestInfo();

    NYPath::TTokenizer tokenizer(path);
    tokenizer.Advance();
    if (tokenizer.GetToken() == WildcardToken) {
        tokenizer.Advance();
        tokenizer.Expect(NYPath::ETokenType::EndOfStream);

        ValidatePermission(EPermissionCheckScope::This, EPermission::Write);
        ValidatePermission(EPermissionCheckScope::Descendants, EPermission::Write);
        Clear();

        context->Reply();
    } else if (request->force()) {
        // There is no child node under the given path, so there is nothing to remove.
        context->Reply();
    } else {
        ThrowNoSuchChildKey(this, tokenizer.GetLiteralValue());
    }
}

////////////////////////////////////////////////////////////////////////////////

IYPathService::TResolveResult TMapNodeMixin::ResolveRecursive(
    const TYPath& path,
    IServiceContextPtr context)
{
    const auto& method = context->GetMethod();

    NYPath::TTokenizer tokenizer(path);
    tokenizer.Advance();
    tokenizer.Expect(NYPath::ETokenType::Literal);

    if (tokenizer.GetToken() == WildcardToken) {
        if (method != "Remove") {
            THROW_ERROR_EXCEPTION("%Qv is only allowed for Remove method",
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
            if (method == "Exists" || method == "Create" || method == "Copy" || method == "Remove" ||
                method == "Set" && tokenizer.Advance() == NYPath::ETokenType::EndOfStream)
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
    context->SetRequestInfo();

    ValidatePermission(EPermissionCheckScope::This, EPermission::Read);

    auto attributeFilter =
        request->has_attribute_filter()
        ? NYT::FromProto<TAttributeFilter>(request->attribute_filter())
        : TAttributeFilter::None;

    int maxSize = request->has_max_size() ? request->max_size() : std::numeric_limits<int>::max();

    TStringStream stream;
    TYsonWriter writer(&stream);

    auto children = GetChildren();
    if (children.size() > maxSize) {
        writer.OnBeginAttributes();
        writer.OnKeyedItem("incomplete");
        writer.OnStringScalar("true");
        writer.OnEndAttributes();
    }

    size_t counter = 0;

    writer.OnBeginList();
    for (const auto& pair : children) {
        const auto& key = pair.first;
        const auto& node = pair.second;
        writer.OnListItem();
        node->SerializeAttributes(&writer, attributeFilter, false);
        writer.OnStringScalar(key);

        counter += 1;
        if (counter == maxSize) {
            break;
        }
    }
    writer.OnEndList();

    response->set_keys(stream.Str());

    context->Reply();
}

void TMapNodeMixin::SetChild(
    INodeFactoryPtr factory,
    const TYPath& path,
    INodePtr value,
    bool recursive)
{
    NYPath::TTokenizer tokenizer(path);
    tokenizer.Advance();
    if (tokenizer.GetType() == NYPath::ETokenType::EndOfStream) {
        tokenizer.ThrowUnexpected();
    }

    auto node = AsMap();
    while (tokenizer.GetType() != NYPath::ETokenType::EndOfStream) {
        tokenizer.Expect(NYPath::ETokenType::Slash);

        tokenizer.Advance();
        tokenizer.Expect(NYPath::ETokenType::Literal);
        auto key = tokenizer.GetLiteralValue();

        tokenizer.Advance();

        bool lastStep = (tokenizer.GetType() == NYPath::ETokenType::EndOfStream);
        if (!recursive && !lastStep) {
            THROW_ERROR_EXCEPTION("Cannot create intermediate nodes");
        }

        auto newValue = lastStep ? value : factory->CreateMap();
        YCHECK(node->AddChild(newValue, key));

        if (!lastStep) {
            node = newValue->AsMap();
        }
    }
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
        const auto& method = context->GetMethod();
        if (!child && method == "Exists") {
            return IYPathService::TResolveResult::Here("/" + path);
        }
        return IYPathService::TResolveResult::There(child, tokenizer.GetSuffix());
    }
}

void TListNodeMixin::SetChild(
    INodeFactoryPtr /*factory*/,
    const TYPath& path,
    INodePtr value,
    bool recursive)
{
    if (recursive) {
        THROW_ERROR_EXCEPTION("Cannot create intermediate nodes in a list");
    }

    int beforeIndex = -1;

    NYPath::TTokenizer tokenizer(path);

    tokenizer.Advance();
    tokenizer.Expect(NYPath::ETokenType::Slash);

    tokenizer.Advance();
    tokenizer.Expect(NYPath::ETokenType::Literal);

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

