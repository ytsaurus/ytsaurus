#include "stdafx.h"
#include "virtual.h"

#include <core/misc/singleton.h>

#include <core/ypath/tokenizer.h>
#include <core/ypath/token.h>

#include <core/ytree/convert.h>
#include <core/ytree/ypath_proxy.h>

#include <core/rpc/dispatcher.h>

#include <ytlib/hive/cell_directory.h>

#include <ytlib/object_client/object_service_proxy.h>
#include <ytlib/object_client/helpers.h>

#include <ytlib/cypress_client/cypress_ypath_proxy.h>

#include <server/hydra/hydra_manager.h>

#include <server/cypress_server/node_detail.h>
#include <server/cypress_server/node_proxy_detail.h>

#include <server/cell_master/bootstrap.h>
#include <server/cell_master/hydra_facade.h>
#include <server/cell_master/multicell_manager.h>
#include <server/cell_master/config.h>

namespace NYT {
namespace NCypressServer {

using namespace NRpc;
using namespace NYTree;
using namespace NYTree;
using namespace NYson;
using namespace NYPath;
using namespace NCellMaster;
using namespace NTransactionServer;
using namespace NObjectClient;
using namespace NObjectServer;
using namespace NCypressClient;

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

TVirtualMulticellMapBase::TVirtualMulticellMapBase(NCellMaster::TBootstrap* bootstrap)
    : Bootstrap_(bootstrap)
{ }

bool TVirtualMulticellMapBase::DoInvoke(IServiceContextPtr context)
{
    DISPATCH_YPATH_SERVICE_METHOD(Get);
    DISPATCH_YPATH_SERVICE_METHOD(List);
    DISPATCH_YPATH_SERVICE_METHOD(Exists);
    DISPATCH_YPATH_SERVICE_METHOD(Enumerate);
    return TSupportsAttributes::DoInvoke(context);
}

IYPathService::TResolveResult TVirtualMulticellMapBase::ResolveRecursive(
    const TYPath& path,
    IServiceContextPtr context)
{
    NYPath::TTokenizer tokenizer(path);
    tokenizer.Advance();
    tokenizer.Expect(NYPath::ETokenType::Literal);

    TObjectId objectId;
    const auto& objectIdString = tokenizer.GetLiteralValue();
    if (!TObjectId::FromString(objectIdString, &objectId)) {
        THROW_ERROR_EXCEPTION("Error parsing object id %v",
            objectIdString);
    }

    auto objectManager = Bootstrap_->GetObjectManager();
    IYPathServicePtr proxy;
    if (Bootstrap_->IsPrimaryMaster() && CellTagFromId(objectId) != Bootstrap_->GetCellTag()) {
        proxy = objectManager->CreateRemoteProxy(objectId);
    } else {
        auto* object = objectManager->FindObject(objectId);
        if (IsObjectAlive(object) && IsValid(object)) {
            proxy = objectManager->GetProxy(object, nullptr);
        }
    }

    if (!proxy) {
        if (context->GetMethod() == "Exists") {
            return TResolveResult::Here(path);
        }
        THROW_ERROR_EXCEPTION(
            NYTree::EErrorCode::ResolveError,
            "No such child %Qv",
            objectId);
    }

    return TResolveResult::There(proxy, tokenizer.GetSuffix());
}

void TVirtualMulticellMapBase::GetSelf(TReqGet* request, TRspGet* response, TCtxGetPtr context)
{
    YASSERT(!NYson::TTokenizer(GetRequestYPath(context)).ParseNext());

    auto attributeFilter = request->has_attribute_filter()
        ? FromProto<TAttributeFilter>(request->attribute_filter())
        : TAttributeFilter::None;

    i64 limit = request->limit();

    FetchItems(limit, attributeFilter).Subscribe(BIND([=] (const TErrorOr<TFetchItemsSessionPtr>& sessionOrError) {
        if (!sessionOrError.IsOK()) {
            context->Reply(TError(sessionOrError));
            return;
        }

        const auto& session = sessionOrError.Value();

        TStringStream stream;
        TYsonWriter writer(&stream, EYsonFormat::Binary);

        if (session->Incomplete) {
            writer.OnBeginAttributes();
            writer.OnKeyedItem("incomplete");
            writer.OnBooleanScalar(true);
            writer.OnEndAttributes();
        }

        writer.OnBeginMap();
        for (const auto& item : session->Items) {
            writer.OnKeyedItem(item.Key);
            if (item.Attributes) {
                writer.OnBeginAttributes();
                writer.OnRaw(item.Attributes->Data(), item.Attributes->GetType());
                writer.OnEndAttributes();
            }
            writer.OnEntity();
        }
        writer.OnEndMap();

        const auto& str = stream.Str();
        response->set_value(str);

        context->SetRequestInfo("Count: %v, Limit: %v, ByteSize: %v",
            session->Items.size(),
            limit,
            str.length());
        context->Reply();
    }).Via(NRpc::TDispatcher::Get()->GetInvoker()));
}

void TVirtualMulticellMapBase::ListSelf(TReqList* request, TRspList* response, TCtxListPtr context)
{
    auto attributeFilter = request->has_attribute_filter()
        ? FromProto<TAttributeFilter>(request->attribute_filter())
        : TAttributeFilter::None;

    i64 limit = request->limit();

    FetchItems(limit, attributeFilter).Subscribe(BIND([=] (const TErrorOr<TFetchItemsSessionPtr>& sessionOrError) {
        if (!sessionOrError.IsOK()) {
            context->Reply(TError(sessionOrError));
            return;
        }

        const auto& session = sessionOrError.Value();

        TStringStream stream;
        TYsonWriter writer(&stream, EYsonFormat::Binary);

        if (session->Incomplete) {
            writer.OnBeginAttributes();
            writer.OnKeyedItem("incomplete");
            writer.OnBooleanScalar(true);
            writer.OnEndAttributes();
        }

        writer.OnBeginList();
        for (const auto& item : session->Items) {
            writer.OnListItem();
            if (item.Attributes) {
                writer.OnBeginAttributes();
                writer.OnRaw(item.Attributes->Data(), item.Attributes->GetType());
                writer.OnEndAttributes();
            }
            writer.OnStringScalar(item.Key);
        }
        writer.OnEndList();

        const auto& str = stream.Str();
        response->set_value(str);

        context->SetRequestInfo("Count: %v, Limit: %v, ByteSize: %v",
            session->Items.size(),
            limit,
            str.length());
        context->Reply();
    }).Via(NRpc::TDispatcher::Get()->GetInvoker()));
}

void TVirtualMulticellMapBase::ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors)
{
    descriptors->push_back(TAttributeDescriptor("count")
        .SetOpaque(true));
    descriptors->push_back(TAttributeDescriptor("multicell_count")
        .SetOpaque(true));
}

bool TVirtualMulticellMapBase::GetBuiltinAttribute(const Stroka& /*key*/, IYsonConsumer* /*consumer*/)
{
    return false;
}

TFuture<TYsonString> TVirtualMulticellMapBase::GetBuiltinAttributeAsync(const Stroka& key)
{
    if (key == "count") {
        return FetchSizes().Apply(BIND([] (const std::vector<std::pair<TCellTag, i64>>& multicellSizes) {
            i64 result = 0;
            for (const auto& pair : multicellSizes) {
                result += pair.second;
            }
            return ConvertToYsonString(result);
        }));
    }

    if (key == "multicell_count") {
        return FetchSizes().Apply(BIND([] (const std::vector<std::pair<TCellTag, i64>>& multicellSizes) {
            return BuildYsonStringFluently().DoMapFor(multicellSizes, [] (TFluentMap fluent, const std::pair<TCellTag, i64>& pair) {
                fluent.Item(ToString(pair.first)).Value(pair.second);
            });
        }));
    }

    return Null;
}

ISystemAttributeProvider* TVirtualMulticellMapBase::GetBuiltinAttributeProvider()
{
    return this;
}

bool TVirtualMulticellMapBase::SetBuiltinAttribute(const Stroka& /*key*/, const TYsonString& /*value*/)
{
    return false;
}

bool TVirtualMulticellMapBase::RemoveBuiltinAttribute(const Stroka& /*key*/)
{
    return false;
}

TFuture<std::vector<std::pair<TCellTag, i64>>> TVirtualMulticellMapBase::FetchSizes()
{
    std::vector<TFuture<std::pair<TCellTag, i64>>> asyncResults{
        MakeFuture(std::make_pair(Bootstrap_->GetCellTag(), GetSize()))
    };

    if (Bootstrap_->IsPrimaryMaster()) {
        auto multicellManager = Bootstrap_->GetMulticellManager();
        auto cellDirectory = Bootstrap_->GetCellDirectory();
        for (auto cellTag : multicellManager->GetRegisteredSecondaryMasterCellTags()) {
            auto cellId = ReplaceCellTagInId(Bootstrap_->GetCellId(), cellTag);
            auto channel = cellDirectory->FindChannel(cellId);
            if (channel) {
                TObjectServiceProxy proxy(channel);
                proxy.SetDefaultTimeout(Bootstrap_->GetConfig()->ObjectManager->ForwardingRpcTimeout);

                auto path = GetWellKnownPath();
                auto req = TYPathProxy::Get(path + "/@count");
                auto asyncResult = proxy.Execute(req).Apply(BIND([=, this_ = MakeStrong(this)] (const TYPathProxy::TErrorOrRspGetPtr& rspOrError) {
                    if (!rspOrError.IsOK()) {
                        THROW_ERROR_EXCEPTION("Error fetching size of virtual map %v from cell %v",
                            path,
                            cellTag);
                    }
                    const auto& rsp = rspOrError.Value();
                    return std::make_pair(cellTag, ConvertTo<i64>(TYsonString(rsp->value())));
                }));

                asyncResults.push_back(asyncResult);
            }
        }
    }

    return Combine(asyncResults);
}

TFuture<TVirtualMulticellMapBase::TFetchItemsSessionPtr> TVirtualMulticellMapBase::FetchItems(
    i64 limit,
    const TAttributeFilter& attributeFilter)
{
    auto multicellManager = Bootstrap_->GetMulticellManager();

    auto session = New<TFetchItemsSession>();
    session->Limit = limit;
    session->AttributeFilter = attributeFilter;
    session->CellTags = multicellManager->GetRegisteredSecondaryMasterCellTags();

    auto promise = NewPromise<TFetchItemsSessionPtr>();
    FetchItemsFromLocal(session, promise);
    if (Bootstrap_->IsPrimaryMaster()) {
        FetchItemsFromRemote(session, promise);
    } else {
        promise.Set(session);
    }

    return promise.ToFuture();
}

void TVirtualMulticellMapBase::FetchItemsFromLocal(
    TVirtualMulticellMapBase::TFetchItemsSessionPtr session,
    TPromise<TFetchItemsSessionPtr> promise)
{
    YCHECK(session->Items.empty());
    auto keys = GetKeys(session->Limit);
    session->Incomplete |= (keys.size() == session->Limit);

    auto objectManager = Bootstrap_->GetObjectManager();

    for (const auto& key : keys) {
        auto* object = objectManager->FindObject(key);
        if (IsObjectAlive(object)) {
            TFetchItem item;
            item.Key = ToString(key);
            if (session->AttributeFilter.Mode != EAttributeFilterMode::None) {
                TStringStream stream;
                TYsonWriter writer(&stream, EYsonFormat::Binary);
                auto proxy = objectManager->GetProxy(object, nullptr);
                proxy->WriteAttributesFragment(&writer, session->AttributeFilter, false);
                const auto& str = stream.Str();
                if (!str.empty()) {
                    item.Attributes = TYsonString(str, EYsonType::MapFragment);
                }
            }
            session->Items.push_back(item);
        }
    }

    if (session->Items.size() >= session->Limit) {
        promise.Set(session);
    }
}

void TVirtualMulticellMapBase::FetchItemsFromRemote(
    TVirtualMulticellMapBase::TFetchItemsSessionPtr session,
    TPromise<TFetchItemsSessionPtr> promise)
{
    if (promise.IsSet())
        return;

    if (session->CellTagIndex >= session->CellTags.size() ||
        session->Items.size() >= session->Limit)
    {
        promise.Set(session);
        return;
    }

    auto cellDirectory = Bootstrap_->GetCellDirectory();
    auto cellTag = session->CellTags[session->CellTagIndex++];
    auto cellId = ReplaceCellTagInId(Bootstrap_->GetCellId(), cellTag);
    auto channel = cellDirectory->FindChannel(cellId);
    if (!channel) {
        FetchItemsFromRemote(session, promise);
        return;
    }

    TObjectServiceProxy proxy(channel);
    proxy.SetDefaultTimeout(Bootstrap_->GetConfig()->ObjectManager->ForwardingRpcTimeout);

    auto path = GetWellKnownPath();
    auto req = TCypressYPathProxy::Enumerate(path);
    req->set_limit(session->Limit - session->Items.size());
    ToProto(req->mutable_attribute_filter(), session->AttributeFilter);

    proxy.Execute(req).Subscribe(BIND([=, this_ = MakeStrong(this)] (const TCypressYPathProxy::TErrorOrRspEnumeratePtr& rspOrError) mutable {
        if (!rspOrError.IsOK()) {
            auto error = TError("Error fetching content of virtual map %v from cell %v",
                path,
                cellTag)
                << rspOrError;
            promise.Set(error);
            return;
        }

        const auto& rsp = rspOrError.Value();

        session->Incomplete |= rsp->incomplete();
        for (const auto& protoItem : rsp->items()) {
            TFetchItem item;
            item.Key = protoItem.key();
            if (protoItem.has_attributes()) {
                item.Attributes = TYsonString(protoItem.attributes(), EYsonType::MapFragment);
            }
            session->Items.push_back(item);
        }

        FetchItemsFromRemote(session, promise);
    }).Via(NRpc::TDispatcher::Get()->GetInvoker()));
}

DEFINE_YPATH_SERVICE_METHOD(TVirtualMulticellMapBase, Enumerate)
{
    i64 limit = request->limit();
    auto attributeFilter = request->has_attribute_filter()
        ? FromProto<TAttributeFilter>(request->attribute_filter())
        : TAttributeFilter::None;

    context->SetRequestInfo("Limit: %v", limit);

    auto keys = GetKeys(limit);

    auto objectManager = Bootstrap_->GetObjectManager();

    for (const auto& key : keys) {
        auto* object = objectManager->FindObject(key);
        if (IsObjectAlive(object)) {
            auto* protoItem = response->add_items();
            protoItem->set_key(ToString(key));
            if (attributeFilter.Mode != EAttributeFilterMode::None) {
                TStringStream stream;
                TYsonWriter writer(&stream, EYsonFormat::Binary);
                auto proxy = objectManager->GetProxy(object, nullptr);
                proxy->WriteAttributesFragment(&writer, attributeFilter, false);
                const auto& str = stream.Str();
                if (!str.empty()) {
                    protoItem->set_attributes(str);
                }
            }
        }
    }

    response->set_incomplete(response->items_size() == limit);
    context->SetResponseInfo("Count: %v, Incomplete: %v",
        response->items_size(),
        response->incomplete());
    context->Reply();
}

////////////////////////////////////////////////////////////////////////////////

class TVirtualNode
    : public TCypressNodeBase
{
public:
    explicit TVirtualNode(const TVersionedNodeId& id)
        : TCypressNodeBase(id)
    { }

};

////////////////////////////////////////////////////////////////////////////////

class TVirtualNodeProxy
    : public TCypressNodeProxyBase<TNontemplateCypressNodeProxyBase, IEntityNode, TVirtualNode>
{
public:
    TVirtualNodeProxy(
        INodeTypeHandlerPtr typeHandler,
        TBootstrap* bootstrap,
        TTransaction* transaction,
        TVirtualNode* trunkNode,
        IYPathServicePtr service,
        EVirtualNodeOptions options)
        : TBase(
            typeHandler,
            bootstrap,
            transaction,
            trunkNode)
        , Service_(service)
        , Options_(options)
    { }

private:
    typedef TCypressNodeProxyBase<TNontemplateCypressNodeProxyBase, IEntityNode, TVirtualNode> TBase;

    const IYPathServicePtr Service_;
    const EVirtualNodeOptions Options_;


    virtual TResolveResult ResolveSelf(const TYPath& path, IServiceContextPtr context) override
    {
        const auto& method = context->GetMethod();
        if ((Options_ & EVirtualNodeOptions::RedirectSelf) != EVirtualNodeOptions::None &&
            method != "Remove")
        {
            return TResolveResult::There(Service_, path);
        } else {
            return TBase::ResolveSelf(path, context);
        }
    }

    virtual TResolveResult ResolveRecursive(const TYPath& path, IServiceContextPtr context) override
    {
        NYPath::TTokenizer tokenizer(path);
        switch (tokenizer.Advance()) {
            case NYPath::ETokenType::EndOfStream:
            case NYPath::ETokenType::Slash:
                return TResolveResult::There(Service_, path);
            default:
                return TResolveResult::There(Service_, "/" + path);
        }
    }


    virtual void ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors) override
    {
        auto* provider = GetTargetBuiltinAttributeProvider();
        if (provider) {
            provider->ListSystemAttributes(descriptors);
        }

        TBase::ListSystemAttributes(descriptors);
    }

    virtual bool GetBuiltinAttribute(const Stroka& key, IYsonConsumer* consumer) override
    {
        auto* provider = GetTargetBuiltinAttributeProvider();
        if (provider && provider->GetBuiltinAttribute(key, consumer)) {
            return true;
        }

        return TBase::GetBuiltinAttribute(key, consumer);
    }

    virtual TFuture<TYsonString> GetBuiltinAttributeAsync(const Stroka& key) override
    {
        auto* provider = GetTargetBuiltinAttributeProvider();
        if (provider) {
            auto result = provider->GetBuiltinAttributeAsync(key);
            if (result) {
                return result;
            }
        }

        return TBase::GetBuiltinAttributeAsync(key);
    }

    virtual bool SetBuiltinAttribute(const Stroka& key, const TYsonString& value) override
    {
        auto* provider = GetTargetBuiltinAttributeProvider();
        if (provider && provider->SetBuiltinAttribute(key, value)) {
            return true;
        }

        return TBase::SetBuiltinAttribute(key, value);
    }

    virtual bool IsLeaderReadRequired() const override
    {
        return Any(Options_ & EVirtualNodeOptions::RequireLeader);
    }

    ISystemAttributeProvider* GetTargetBuiltinAttributeProvider()
    {
        return dynamic_cast<ISystemAttributeProvider*>(Service_.Get());
    }

};

////////////////////////////////////////////////////////////////////////////////

class TVirtualNodeTypeHandler
    : public TCypressNodeTypeHandlerBase<TVirtualNode>
{
public:
    TVirtualNodeTypeHandler(
        TBootstrap* bootstrap,
        TYPathServiceProducer producer,
        EObjectType objectType,
        EVirtualNodeOptions options)
        : TCypressNodeTypeHandlerBase<TVirtualNode>(bootstrap)
        , Producer_(producer)
        , ObjectType_(objectType)
        , Options_(options)
    { }

    virtual EObjectType GetObjectType() override
    {
        return ObjectType_;
    }

    virtual ENodeType GetNodeType() override
    {
        return ENodeType::Entity;
    }

private:
    const TYPathServiceProducer Producer_;
    const EObjectType ObjectType_;
    const EVirtualNodeOptions Options_;


    virtual ICypressNodeProxyPtr DoGetProxy(
        TVirtualNode* trunkNode,
        TTransaction* transaction) override
    {
        auto service = Producer_.Run(trunkNode, transaction);
        return New<TVirtualNodeProxy>(
            this,
            Bootstrap_,
            transaction,
            trunkNode,
            service,
            Options_);
    }

};

INodeTypeHandlerPtr CreateVirtualTypeHandler(
    TBootstrap* bootstrap,
    EObjectType objectType,
    TYPathServiceProducer producer,
    EVirtualNodeOptions options)
{
    return New<TVirtualNodeTypeHandler>(
        bootstrap,
        producer,
        objectType,
        options);
}

INodeTypeHandlerPtr CreateVirtualTypeHandler(
    TBootstrap* bootstrap,
    EObjectType objectType,
    IYPathServicePtr service,
    EVirtualNodeOptions options)
{
    return CreateVirtualTypeHandler(
        bootstrap,
        objectType,
        BIND([=] (TCypressNodeBase* trunkNode, TTransaction* transaction) -> IYPathServicePtr {
            UNUSED(trunkNode);
            UNUSED(transaction);
            return service;
        }),
        options);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressServer
} // namespace NYT
