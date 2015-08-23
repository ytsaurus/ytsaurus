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
#include <core/yson/async_writer.h>
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
    auto attributeFilter = request->has_attribute_filter()
        ? NYT::FromProto<TAttributeFilter>(request->attribute_filter())
        : TAttributeFilter::None;

    bool ignoreOpaque = request->ignore_opaque();

    context->SetRequestInfo("AttributeFilterMode: %v, IgnoreOpaque: %v",
        attributeFilter.Mode,
        ignoreOpaque);

    ValidatePermission(EPermissionCheckScope::This, EPermission::Read);

    TAsyncYsonWriter writer;

    VisitTree(
        this,
        &writer,
        attributeFilter,
        false,
        ignoreOpaque);

    writer.Finish().Subscribe(BIND([=] (const TErrorOr<TYsonString>& resultOrError) {
        if (resultOrError.IsOK()) {
            response->set_value(resultOrError.Value().Data());
            context->Reply();
        } else {
            context->Reply(resultOrError);
        }
    }));
}

void TNodeBase::GetKeySelf(TReqGetKey* /*request*/, TRspGetKey* response, TCtxGetKeyPtr context)
{
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

    context->SetResponseInfo("Key: %v", key);
    response->set_value(key);

    context->Reply();
}

void TNodeBase::RemoveSelf(TReqRemove* request, TRspRemove* /*response*/, TCtxRemovePtr context)
{
    context->SetRequestInfo();

    auto parent = GetParent();
    if (!parent) {
        ThrowCannotRemoveRoot();
    }

    ValidatePermission(
        EPermissionCheckScope::This | EPermissionCheckScope::Descendants,
        EPermission::Remove);
    ValidatePermission(
        EPermissionCheckScope::Parent,
        EPermission::Write);

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
    TRspSet* /*response*/,
    TCtxSetPtr context)
{
    context->SetRequestInfo();

    ValidatePermission(EPermissionCheckScope::This, EPermission::Write);

    auto factory = CreateFactory();
    auto child = ConvertToNode(TYsonString(request->value()), factory.Get());
    SetChild(factory, "/" + path, child, false);
    factory->Commit();

    context->Reply();
}

void TCompositeNodeMixin::RemoveRecursive(
    const TYPath& path,
    TSupportsRemove::TReqRemove* request,
    TSupportsRemove::TRspRemove* /*response*/,
    TSupportsRemove::TCtxRemovePtr context)
{
    context->SetRequestInfo();

    NYPath::TTokenizer tokenizer(path);
    if (tokenizer.Advance() == NYPath::ETokenType::Asterisk) {
        tokenizer.Advance();
        tokenizer.Expect(NYPath::ETokenType::EndOfStream);

        ValidatePermission(EPermissionCheckScope::This, EPermission::Write);
        ValidatePermission(EPermissionCheckScope::Descendants, EPermission::Remove);
        Clear();

        context->Reply();
    } else if (request->force()) {
        // There is no child node under the given path, so there is nothing to remove.
        context->Reply();
    } else {
        ThrowNoSuchChildKey(this, tokenizer.GetLiteralValue());
    }
}

int TCompositeNodeMixin::GetMaxChildCount() const
{
    return std::numeric_limits<int>::max();
}

////////////////////////////////////////////////////////////////////////////////

IYPathService::TResolveResult TMapNodeMixin::ResolveRecursive(
    const TYPath& path,
    IServiceContextPtr context)
{
    const auto& method = context->GetMethod();

    NYPath::TTokenizer tokenizer(path);
    switch (tokenizer.Advance()) {
        case NYPath::ETokenType::Asterisk: {
            if (method != "Remove") {
                THROW_ERROR_EXCEPTION("\"*\" is only allowed for Remove method");
            }

            tokenizer.Advance();
            tokenizer.Expect(NYPath::ETokenType::EndOfStream);

            return IYPathService::TResolveResult::Here("/" + path);
        }

        case NYPath::ETokenType::Literal: {
            auto key = tokenizer.GetLiteralValue();
            if (key.Empty()) {
                THROW_ERROR_EXCEPTION("Child key cannot be empty");
            }

            auto suffix = tokenizer.GetSuffix();
            bool lastToken =  tokenizer.Advance() == NYPath::ETokenType::EndOfStream;

            auto child = FindChild(key);
            if (!child) {
                if (method == "Exists" ||
                    method == "Create" ||
                    method == "Copy" ||
                    method == "Remove" ||
                    method == "Set" && lastToken)
                {
                    return IYPathService::TResolveResult::Here("/" + path);
                } else {
                    ThrowNoSuchChildKey(this, key);
                }
            }

            return IYPathService::TResolveResult::There(child, suffix);
        }

        default:
            tokenizer.ThrowUnexpected();
            YUNREACHABLE();
    }
}

void TMapNodeMixin::ListSelf(TReqList* request, TRspList* response, TCtxListPtr context)
{
    context->SetRequestInfo();

    ValidatePermission(EPermissionCheckScope::This, EPermission::Read);

    auto attributeFilter = request->has_attribute_filter()
        ? NYT::FromProto<TAttributeFilter>(request->attribute_filter())
        : TAttributeFilter::None;

    i64 limit = request->limit();

    TAsyncYsonWriter writer;

    auto children = GetChildren();
    if (children.size() > limit) {
        writer.OnBeginAttributes();
        writer.OnKeyedItem("incomplete");
        writer.OnBooleanScalar(true);
        writer.OnEndAttributes();
    }

    i64 counter = 0;

    writer.OnBeginList();
    for (const auto& pair : children) {
        const auto& key = pair.first;
        const auto& node = pair.second;
        writer.OnListItem();
        node->WriteAttributes(&writer, attributeFilter, false);
        writer.OnStringScalar(key);

        counter += 1;
        if (counter == limit) {
            break;
        }
    }
    writer.OnEndList();

    writer.Finish().Subscribe(BIND([=] (const TErrorOr<TYsonString>& resultOrError) {
        if (resultOrError.IsOK()) {
            response->set_value(resultOrError.Value().Data());
            context->Reply();
        } else {
            context->Reply(resultOrError);
        }
    }));
}

void TMapNodeMixin::SetChild(
    INodeFactoryPtr factory,
    const TYPath& path,
    INodePtr child,
    bool recursive)
{
    NYPath::TTokenizer tokenizer(path);
    if (tokenizer.Advance() == NYPath::ETokenType::EndOfStream) {
        tokenizer.ThrowUnexpected();
    }

    auto currentNode = AsMap();
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

        int maxChildCount = GetMaxChildCount();
        if (currentNode->GetChildCount() >= maxChildCount) {
            THROW_ERROR_EXCEPTION("Too many children in map node")
                << TErrorAttribute("limit", maxChildCount);
        }

        auto newChild = lastStep ? child : factory->CreateMap();
        YCHECK(currentNode->AddChild(newChild, key));

        if (!lastStep) {
            currentNode = newChild->AsMap();
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

IYPathService::TResolveResult TListNodeMixin::ResolveRecursive(
    const TYPath& path,
    IServiceContextPtr context)
{
    NYPath::TTokenizer tokenizer(path);
    switch (tokenizer.Advance()) {
        case NYPath::ETokenType::Asterisk: {
            tokenizer.Advance();
            tokenizer.Expect(NYPath::ETokenType::EndOfStream);

            return IYPathService::TResolveResult::Here("/" + path);
        }

        case NYPath::ETokenType::Literal: {
            const auto& token = tokenizer.GetToken();
            if (token == ListBeginToken ||
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

        default:
            tokenizer.ThrowUnexpected();
            YUNREACHABLE();
    }
}

void TListNodeMixin::SetChild(
    INodeFactoryPtr /*factory*/,
    const TYPath& path,
    INodePtr child,
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
    } else if (token.has_prefix(ListBeforeToken) || token.has_prefix(ListAfterToken)) {
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

    int maxChildCount = GetMaxChildCount();
    if (GetChildCount() >= maxChildCount) {
        THROW_ERROR_EXCEPTION("Too many children in list node")
            << TErrorAttribute("limit", maxChildCount);
    }

    AddChild(child, beforeIndex);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

