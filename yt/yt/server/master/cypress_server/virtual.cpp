#include "virtual.h"
#include "node.h"
#include "node_detail.h"
#include "node_proxy_detail.h"
#include "config.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>

#include <yt/yt/server/lib/hydra/hydra_manager.h>

#include <yt/yt/server/lib/misc/interned_attributes.h>

#include <yt/yt/server/master/security_server/user.h>

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>
#include <yt/yt/server/master/cell_master/multicell_manager.h>
#include <yt/yt/server/master/cell_master/config.h>
#include <yt/yt/server/master/cell_master/config_manager.h>

#include <yt/yt/ytlib/hive/cell_directory.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>
#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/ytlib/cypress_client/cypress_ypath_proxy.h>

#include <yt/yt/core/ypath/tokenizer.h>
#include <yt/yt/core/ypath/token.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/ypath_proxy.h>
#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/ypath_service.h>

#include <yt/yt/core/yson/writer.h>
#include <yt/yt/core/yson/async_writer.h>
#include <yt/yt/core/yson/attribute_consumer.h>

#include <yt/yt/core/rpc/dispatcher.h>

#include <yt/yt/core/concurrency/scheduler.h>

namespace NYT::NCypressServer {

using namespace NYTree;
using namespace NYson;
using namespace NYPath;
using namespace NCellMaster;
using namespace NTransactionServer;
using namespace NObjectClient;
using namespace NObjectServer;
using namespace NSecurityServer;
using namespace NCypressClient;
using namespace NConcurrency;

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

TVirtualSinglecellMapBase::TVirtualSinglecellMapBase(
    NCellMaster::TBootstrap* bootstrap,
    NYTree::INodePtr owningNode)
    : TVirtualMapBase(std::move(owningNode))
    , Bootstrap_(bootstrap)
{ }

std::optional<TVirtualCompositeNodeReadOffloadParams> TVirtualSinglecellMapBase::GetReadOffloadParams() const
{
    const auto& hydraFacade = Bootstrap_->GetHydraFacade();
    if (!hydraFacade->IsAutomatonLocked()) {
        // This means that multithreaded reads are disabled.
        return std::nullopt;
    }
    const auto& configManager = Bootstrap_->GetConfigManager();
    const auto& config = configManager->GetConfig();
    if (!config->CypressManager->VirtualMapReadOffloadBatchSize) {
        return std::nullopt;
    }
    return TVirtualCompositeNodeReadOffloadParams{
        // NB: Must not release LocalRead thread.
        .WaitForStrategy = EWaitForStrategy::Get,
        .BatchSize = *config->CypressManager->VirtualMapReadOffloadBatchSize,
    };
}

////////////////////////////////////////////////////////////////////////////////

TVirtualMulticellMapBase::TVirtualMulticellMapBase(
    NCellMaster::TBootstrap* bootstrap,
    INodePtr owningNode,
    bool ignoreForeignObjects)
    : Bootstrap_(bootstrap)
    , OwningNode_(owningNode)
    , IgnoreForeignObjects_(ignoreForeignObjects)
{ }

bool TVirtualMulticellMapBase::DoInvoke(const IYPathServiceContextPtr& context)
{
    DISPATCH_YPATH_SERVICE_METHOD(Get);
    DISPATCH_YPATH_SERVICE_METHOD(List);
    DISPATCH_YPATH_SERVICE_METHOD(Exists);
    DISPATCH_YPATH_SERVICE_METHOD(Enumerate);
    return TSupportsAttributes::DoInvoke(context);
}

IYPathService::TResolveResult TVirtualMulticellMapBase::ResolveRecursive(
    const TYPath& path,
    const IYPathServiceContextPtr& context)
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

    const auto& objectManager = Bootstrap_->GetObjectManager();
    IYPathServicePtr proxy;

    // Cf. TPathResolver::ResolveRoot.
    const auto& multicellManager = Bootstrap_->GetMulticellManager();
    auto cellTag = CellTagFromId(objectId);
    if (cellTag == multicellManager->GetCellTag() ||
        multicellManager->IsRegisteredMasterCell(cellTag))
    {
        if (multicellManager->IsPrimaryMaster() && cellTag != multicellManager->GetCellTag()) {
            proxy = objectManager->CreateRemoteProxy(cellTag);
        } else {
            auto* object = objectManager->FindObject(objectId);
            if (IsObjectAlive(object) && IsValid(object)) {
                proxy = objectManager->GetProxy(object, nullptr);
            }
        }
    } // Else it's a garbage guid. Treat it as a non-existent object ID.

    if (!proxy) {
        if (context->GetMethod() == "Exists") {
            return TResolveResultHere{path};
        }
        THROW_ERROR_EXCEPTION(
            NYTree::EErrorCode::ResolveError,
            "No such child %v",
            objectId);
    }

    return TResolveResultThere{std::move(proxy), TYPath(tokenizer.GetSuffix())};
}

void TVirtualMulticellMapBase::GetSelf(
    TReqGet* request,
    TRspGet* response,
    const TCtxGetPtr& context)
{
    YT_ASSERT(!NYson::TTokenizer(GetRequestTargetYPath(context->RequestHeader())).ParseNext());

    auto attributeFilter = request->has_attributes()
        ? FromProto<TAttributeFilter>(request->attributes())
        : TAttributeFilter();

    i64 limit = request->has_limit()
        ? request->limit()
        : DefaultVirtualChildLimit;

    context->SetRequestInfo("Limit: %v, AttributeFilter: %v", limit, attributeFilter);

    // NB: Must deal with owning node's attributes here due to thread affinity issues.
    auto asyncOwningNodeAttributes = GetOwningNodeAttributes(attributeFilter);

    FetchItems(limit, attributeFilter)
        .Subscribe(BIND([=] (const TErrorOr<TFetchItemsSessionPtr>& sessionOrError) {
            if (!sessionOrError.IsOK()) {
                context->Reply(TError(sessionOrError));
                return;
            }

            auto owningNodeAttributesOrError = WaitFor(asyncOwningNodeAttributes);
            if (!owningNodeAttributesOrError.IsOK()) {
                context->Reply(owningNodeAttributesOrError);
                return;
            }

            const auto& owningNodeAttributes = owningNodeAttributesOrError.Value();
            const auto& session = sessionOrError.Value();

            TStringStream stream;
            TBufferedBinaryYsonWriter writer(&stream);

            {
                TAsyncYsonConsumerAdapter asyncAdapter(&writer);
                TAttributeFragmentConsumer attributesConsumer(&asyncAdapter);
                attributesConsumer.OnRaw(owningNodeAttributes);
                if (session->Incomplete) {
                    attributesConsumer.OnKeyedItem("incomplete");
                    attributesConsumer.OnBooleanScalar(true);
                }
                attributesConsumer.Finish();
            }

            writer.OnBeginMap();
            for (const auto& item : session->Items) {
                writer.OnKeyedItem(item.Key);
                if (item.Attributes) {
                    writer.OnBeginAttributes();
                    writer.OnRaw(item.Attributes);
                    writer.OnEndAttributes();
                }
                writer.OnEntity();
            }
            writer.OnEndMap();
            writer.Flush();

            const auto& str = stream.Str();
            response->set_value(str);

            context->SetResponseInfo("Count: %v, Limit: %v, ByteSize: %v",
                session->Items.size(),
                limit,
                str.length());
            context->Reply();
        }).Via(NRpc::TDispatcher::Get()->GetHeavyInvoker()));
}

void TVirtualMulticellMapBase::ListSelf(
    TReqList* request,
    TRspList* response,
    const TCtxListPtr& context)
{
    auto attributeFilter = request->has_attributes()
        ? FromProto<TAttributeFilter>(request->attributes())
        : TAttributeFilter();

    i64 limit = request->has_limit()
        ? request->limit()
        : DefaultVirtualChildLimit;

    context->SetRequestInfo("Limit: %v, AttributeFilter: %v", limit, attributeFilter);

    FetchItems(limit, attributeFilter)
        .Subscribe(BIND([=] (const TErrorOr<TFetchItemsSessionPtr>& sessionOrError) {
            if (!sessionOrError.IsOK()) {
                context->Reply(TError(sessionOrError));
                return;
            }

            const auto& session = sessionOrError.Value();

            TStringStream stream;
            TBufferedBinaryYsonWriter writer(&stream);

            {
                TAsyncYsonConsumerAdapter asyncAdapter(&writer);
                TAttributeFragmentConsumer attributesConsumer(&asyncAdapter);
                if (session->Incomplete) {
                    attributesConsumer.OnKeyedItem("incomplete");
                    attributesConsumer.OnBooleanScalar(true);
                }
                attributesConsumer.Finish();
            }

            writer.OnBeginList();
            for (const auto& item : session->Items) {
                writer.OnListItem();
                if (item.Attributes) {
                    writer.OnBeginAttributes();
                    writer.OnRaw(item.Attributes);
                    writer.OnEndAttributes();
                }
                writer.OnStringScalar(item.Key);
            }
            writer.OnEndList();
            writer.Flush();

            const auto& str = stream.Str();
            response->set_value(str);

            context->SetResponseInfo("Count: %v, Limit: %v, ByteSize: %v",
                session->Items.size(),
                limit,
                str.length());
            context->Reply();
        }).Via(NRpc::TDispatcher::Get()->GetHeavyInvoker()));
}

void TVirtualMulticellMapBase::ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors)
{
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Count)
        .SetOpaque(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::MulticellCount)
        .SetOpaque(true));
}

const THashSet<TInternedAttributeKey>& TVirtualMulticellMapBase::GetBuiltinAttributeKeys()
{
    return BuiltinAttributeKeysCache_.GetBuiltinAttributeKeys(this);
}

bool TVirtualMulticellMapBase::GetBuiltinAttribute(TInternedAttributeKey /*key*/, IYsonConsumer* /*consumer*/)
{
    return false;
}

TFuture<TYsonString> TVirtualMulticellMapBase::GetBuiltinAttributeAsync(TInternedAttributeKey key)
{
    switch (key) {
        case EInternedAttributeKey::Count:
            return FetchSizes().Apply(BIND([] (const std::vector<std::pair<TCellTag, i64>>& multicellSizes) {
                i64 result = 0;
                for (auto [cellId, size] : multicellSizes) {
                    result += size;
                }
                return ConvertToYsonString(result);
            }));

        case EInternedAttributeKey::MulticellCount:
            return FetchSizes().Apply(BIND([] (const std::vector<std::pair<TCellTag, i64>>& multicellSizes) {
                return BuildYsonStringFluently().DoMapFor(multicellSizes, [] (TFluentMap fluent, const std::pair<TCellTag, i64>& pair) {
                    fluent.Item(ToString(pair.first)).Value(pair.second);
                });
            }));

        default:
            break;
    }

    return std::nullopt;
}

ISystemAttributeProvider* TVirtualMulticellMapBase::GetBuiltinAttributeProvider()
{
    return this;
}

bool TVirtualMulticellMapBase::SetBuiltinAttribute(TInternedAttributeKey /*key*/, const TYsonString& /*value*/, bool /*force*/)
{
    return false;
}

bool TVirtualMulticellMapBase::RemoveBuiltinAttribute(TInternedAttributeKey /*key*/)
{
    return false;
}

TFuture<std::vector<std::pair<TCellTag, i64>>> TVirtualMulticellMapBase::FetchSizes()
{
    std::vector<TFuture<std::pair<TCellTag, i64>>> asyncResults{
        FetchSizeFromLocal()
    };

    const auto& multicellManager = Bootstrap_->GetMulticellManager();
    if (multicellManager->IsPrimaryMaster()) {
        for (auto cellTag : multicellManager->GetRegisteredMasterCellTags()) {
            auto asyncResult = FetchSizeFromRemote(cellTag);
            if (asyncResult) {
                asyncResults.push_back(asyncResult);
            }
        }
    }

    return AllSucceeded(asyncResults);
}

TFuture<std::pair<TCellTag, i64>> TVirtualMulticellMapBase::FetchSizeFromLocal()
{
    return GetSize()
        .Apply(BIND([=, this, this_ = MakeStrong(this)] (i64 size) {
            return std::pair(Bootstrap_->GetMulticellManager()->GetCellTag(), size);
        }));
}

TFuture<std::pair<TCellTag, i64>> TVirtualMulticellMapBase::FetchSizeFromRemote(TCellTag cellTag)
{
    TObjectServiceProxy proxy(
        Bootstrap_->GetClusterConnection(),
        NApi::EMasterChannelKind::Follower,
        cellTag,
        /*stickyGroupSizeCache*/ nullptr);
    auto batchReq = proxy.ExecuteBatch();
    batchReq->SetSuppressUpstreamSync(true);

    auto path = GetWellKnownPath();
    auto req = TYPathProxy::Get(path + "/@count");
    batchReq->AddRequest(req, "get_count");

    return batchReq->Invoke()
        .Apply(BIND([=] (const TObjectServiceProxy::TErrorOrRspExecuteBatchPtr& batchRspOrError) {
            auto cumulativeError = GetCumulativeError(batchRspOrError);
            if (!cumulativeError.IsOK()) {
                THROW_ERROR_EXCEPTION("Error fetching size of virtual map %v from cell %v",
                    path,
                    cellTag)
                    << cumulativeError;
            }

            const auto& batchRsp = batchRspOrError.Value();

            auto rspOrError = batchRsp->GetResponse<TYPathProxy::TRspGet>("get_count");
            const auto& rsp = rspOrError.Value();
            return std::pair(cellTag, ConvertTo<i64>(TYsonString(rsp->value())));
        }));
}

TFuture<TVirtualMulticellMapBase::TFetchItemsSessionPtr> TVirtualMulticellMapBase::FetchItems(
    i64 limit,
    const TAttributeFilter& attributeFilter)
{
    auto session = New<TFetchItemsSession>();
    session->Invoker = CreateSerializedInvoker(NRpc::TDispatcher::Get()->GetHeavyInvoker());
    session->Limit = limit;
    session->AttributeFilter = attributeFilter;

    std::vector<TFuture<void>> asyncResults{
        FetchItemsFromLocal(session)
    };

    const auto& multicellManager = Bootstrap_->GetMulticellManager();
    if (multicellManager->IsPrimaryMaster()) {
        for (auto cellTag : multicellManager->GetRegisteredMasterCellTags()) {
            asyncResults.push_back(FetchItemsFromRemote(session, cellTag));
        }
    }

    return AllSucceeded(asyncResults).Apply(BIND([=] {
        return session;
    }));
}

TFuture<void> TVirtualMulticellMapBase::FetchItemsFromLocal(const TFetchItemsSessionPtr& session)
{
    return GetKeys(session->Limit)
        .Apply(BIND([=, this, this_ = MakeStrong(this)] (const std::vector<TObjectId>& keys) {
            session->Incomplete |= (std::ssize(keys) == session->Limit);

            const auto& objectManager = Bootstrap_->GetObjectManager();

            std::vector<TFuture<TYsonString>> asyncAttributes;
            std::vector<TObjectId> aliveKeys;
            for (const auto& key : keys) {
                auto* object = objectManager->FindObject(key);
                if (!IsObjectAlive(object)) {
                    continue;
                }
                if (IgnoreForeignObjects_ && object->IsForeign()) {
                    continue;
                }

                aliveKeys.push_back(key);
                if (session->AttributeFilter && !session->AttributeFilter.IsEmpty()) {
                    TAsyncYsonWriter writer(EYsonType::MapFragment);
                    auto proxy = objectManager->GetProxy(object, nullptr);
                    proxy->WriteAttributesFragment(&writer, session->AttributeFilter, false);
                    asyncAttributes.emplace_back(writer.Finish());
                } else {
                    static const auto EmptyYson = MakeFuture(TYsonString());
                    asyncAttributes.push_back(EmptyYson);
                }
            }

            return AllSucceeded(asyncAttributes)
                .Apply(BIND([=, aliveKeys = std::move(aliveKeys), this_ = MakeStrong(this)] (const std::vector<TYsonString>& attributes) {
                    YT_VERIFY(aliveKeys.size() == attributes.size());
                    for (int index = 0; index < static_cast<int>(aliveKeys.size()); ++index) {
                        if (std::ssize(session->Items) >= session->Limit) {
                            break;
                        }
                        session->Items.push_back(TFetchItem{ToString(aliveKeys[index]), attributes[index]});
                    }
                }).AsyncVia(session->Invoker));
        }).AsyncVia(GetCurrentInvoker()));
}

TFuture<void> TVirtualMulticellMapBase::FetchItemsFromRemote(
    const TFetchItemsSessionPtr& session,
    TCellTag cellTag)
{
    const auto& securityManager = Bootstrap_->GetSecurityManager();
    const auto* user = securityManager->GetAuthenticatedUser();

    TObjectServiceProxy proxy(
        Bootstrap_->GetClusterConnection(),
        NApi::EMasterChannelKind::Follower,
        cellTag,
        /*stickyGroupSizeCache*/ nullptr);
    auto batchReq = proxy.ExecuteBatch();
    batchReq->SetUser(user->GetName());

    if (NeedSuppressUpstreamSync()) {
        batchReq->SetSuppressUpstreamSync(true);
    }
    if (NeedSuppressTransactionCoordinatorSync()) {
        batchReq->SetSuppressTransactionCoordinatorSync(true);
    }

    auto path = GetWellKnownPath();
    auto req = TCypressYPathProxy::Enumerate(path);
    req->set_limit(session->Limit);
    if (session->AttributeFilter) {
        ToProto(req->mutable_attributes(), session->AttributeFilter);
    }
    batchReq->AddRequest(req, "enumerate");

    return batchReq->Invoke()
        .Apply(BIND([=] (const TObjectServiceProxy::TErrorOrRspExecuteBatchPtr& batchRspOrError) {
            auto cumulativeError = GetCumulativeError(batchRspOrError);
            if (!cumulativeError.IsOK()) {
                THROW_ERROR_EXCEPTION("Error fetching content of virtual map %v from cell %v",
                    path,
                    cellTag)
                    << cumulativeError;
            }

            const auto& batchRsp = batchRspOrError.Value();

            auto rspOrError = batchRsp->GetResponse<TCypressYPathProxy::TRspEnumerate>("enumerate");
            const auto& rsp = rspOrError.Value();

            session->Incomplete |= rsp->incomplete();
            for (const auto& protoItem : rsp->items()) {
                if (std::ssize(session->Items) >= session->Limit) {
                    break;
                }
                TFetchItem item;
                item.Key = protoItem.key();
                if (protoItem.has_attributes()) {
                    item.Attributes = TYsonString(protoItem.attributes(), EYsonType::MapFragment);
                }
                session->Items.push_back(item);
            }
        }).AsyncVia(session->Invoker));
}

TFuture<TYsonString> TVirtualMulticellMapBase::GetOwningNodeAttributes(const TAttributeFilter& attributeFilter)
{
    TAsyncYsonWriter writer(EYsonType::MapFragment);
    if (OwningNode_) {
        OwningNode_->WriteAttributesFragment(&writer, attributeFilter, false);
    }
    return writer.Finish();
}

bool TVirtualMulticellMapBase::NeedSuppressUpstreamSync() const
{
    return true;
}

bool TVirtualMulticellMapBase::NeedSuppressTransactionCoordinatorSync() const
{
    return true;
}

DEFINE_YPATH_SERVICE_METHOD(TVirtualMulticellMapBase, Enumerate)
{
    auto attributeFilter = request->has_attributes()
        ? FromProto<TAttributeFilter>(request->attributes())
        : TAttributeFilter();

    i64 limit = request->limit();

    context->SetRequestInfo("Limit: %v, AttributeFilter: %v", limit, attributeFilter);

    GetKeys(limit)
        .Subscribe(BIND([=, this, this_ = MakeStrong(this)] (const TErrorOr<std::vector<TObjectId>>& keysOrError) {
            if (!keysOrError.IsOK()) {
                context->Reply(keysOrError);
                return;
            }

            const auto& objectManager = Bootstrap_->GetObjectManager();

            std::vector<TFuture<TYsonString>> asyncValues;
            const auto& keys = keysOrError.Value();
            for (const auto& key : keys) {
                auto* object = objectManager->FindObject(key);
                if (!IsObjectAlive(object)) {
                    continue;
                }
                if (IgnoreForeignObjects_ && object->IsForeign()) {
                    continue;
                }

                auto* protoItem = response->add_items();
                protoItem->set_key(ToString(key));
                TAsyncYsonWriter writer(EYsonType::MapFragment);
                auto proxy = objectManager->GetProxy(object, nullptr);
                proxy->WriteAttributesFragment(&writer, attributeFilter, false);
                asyncValues.push_back(writer.Finish());
            }

            response->set_incomplete(response->items_size() == limit);

            AllSucceeded(asyncValues)
                .Subscribe(BIND([=] (const TErrorOr<std::vector<TYsonString>>& valuesOrError) {
                    if (!valuesOrError.IsOK()) {
                        context->Reply(valuesOrError);
                        return;
                    }

                    const auto& values = valuesOrError.Value();
                    YT_VERIFY(response->items_size() == std::ssize(values));
                    for (int index = 0; index < response->items_size(); ++index) {
                        const auto& value = values[index];
                        if (!value.AsStringBuf().empty()) {
                            response->mutable_items(index)->set_attributes(value.ToString());
                        }
                    }

                    context->SetResponseInfo("Count: %v, Incomplete: %v",
                        response->items_size(),
                        response->incomplete());
                    context->Reply();
                }));
        }).Via(GetCurrentInvoker()));
}

////////////////////////////////////////////////////////////////////////////////

class TVirtualNode
    : public TCypressNode
{
public:
    using TCypressNode::TCypressNode;

    ENodeType GetNodeType() const override
    {
        return ENodeType::Entity;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TVirtualNodeProxy
    : public TCypressNodeProxyBase<TNontemplateCypressNodeProxyBase, IEntityNode, TVirtualNode>
{
public:
    TVirtualNodeProxy(
        TBootstrap* bootstrap,
        TObjectTypeMetadata* metadata,
        TTransaction* transaction,
        TVirtualNode* trunkNode,
        EVirtualNodeOptions options,
        TYPathServiceProducer producer)
        : TBase(
            bootstrap,
            metadata,
            transaction,
            trunkNode)
        , Options_(options)
        , Producer_(producer)
    { }

    ENodeType GetType() const override
    {
        return ENodeType::Entity;
    }

private:
    using TBase = TCypressNodeProxyBase<TNontemplateCypressNodeProxyBase, IEntityNode, TVirtualNode>;

    const EVirtualNodeOptions Options_;
    const TYPathServiceProducer Producer_;


    static EPermission PermissionFromRequest(const IYPathServiceContextPtr& context)
    {
        return IsRequestMutating(context->RequestHeader()) ? EPermission::Write : EPermission::Read;
    }

    TResolveResult Resolve(const TYPath& path, const IYPathServiceContextPtr& context) override
    {
        NYPath::TTokenizer tokenizer(path);
        tokenizer.Advance();

        if (tokenizer.GetType() == NYPath::ETokenType::Ampersand) {
            // We are explicitly asked not to redirect to the underlying service.
            return TBase::ResolveSelf(TYPath(tokenizer.GetSuffix()), context);
        }

        if (tokenizer.GetType() == NYPath::ETokenType::EndOfStream) {
            return ResolveSelf(TYPath(tokenizer.GetSuffix()), context);
        }

        tokenizer.Expect(NYPath::ETokenType::Slash);

        if (tokenizer.Advance() == NYPath::ETokenType::At) {
            return ResolveAttributes(TYPath(tokenizer.GetSuffix()), context);
        } else {
            return ResolveRecursive(TYPath(tokenizer.GetInput()), context);
        }
    }

    TResolveResult ResolveSelf(const TYPath& path, const IYPathServiceContextPtr& context) override
    {
        auto service = GetService();
        const auto& method = context->GetMethod();
        if ((Options_ & EVirtualNodeOptions::RedirectSelf) != EVirtualNodeOptions::None &&
            method != "Remove" &&
            method != "GetBasicAttributes" &&
            method != "Create" &&
            method != "CheckPermission")
        {
            auto permission = PermissionFromRequest(context);
            ValidatePermission(EPermissionCheckScope::This, permission);
            return TResolveResultThere{std::move(service), path};
        } else {
            return TBase::ResolveSelf(path, context);
        }
    }

    TResolveResult ResolveRecursive(const TYPath& path, const IYPathServiceContextPtr& /*context*/) override
    {
        auto service = GetService();
        NYPath::TTokenizer tokenizer(path);
        switch (tokenizer.Advance()) {
            case NYPath::ETokenType::EndOfStream:
            case NYPath::ETokenType::Slash:
                return TResolveResultThere{std::move(service), path};
            default:
                return TResolveResultThere{std::move(service), "/" + path};
        }
    }


    void ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors) override
    {
        auto service = GetService();
        auto* provider = GetTargetBuiltinAttributeProvider(service);
        if (provider) {
            provider->ListSystemAttributes(descriptors);
        }

        TBase::ListSystemAttributes(descriptors);
    }

    bool GetBuiltinAttribute(TInternedAttributeKey key, IYsonConsumer* consumer) override
    {
        auto service = GetService();
        auto* provider = GetTargetBuiltinAttributeProvider(service);
        if (provider && provider->GetBuiltinAttribute(key, consumer)) {
            return true;
        }

        return TBase::GetBuiltinAttribute(key, consumer);
    }

    TFuture<TYsonString> GetBuiltinAttributeAsync(TInternedAttributeKey key) override
    {
        auto service = GetService();
        auto* provider = GetTargetBuiltinAttributeProvider(service);
        if (provider) {
            auto result = provider->GetBuiltinAttributeAsync(key);
            if (result) {
                return result;
            }
        }

        return TBase::GetBuiltinAttributeAsync(key);
    }

    bool SetBuiltinAttribute(TInternedAttributeKey key, const TYsonString& value, bool force) override
    {
        auto service = GetService();
        auto* provider = GetTargetBuiltinAttributeProvider(service);
        if (provider && provider->SetBuiltinAttribute(key, value, force)) {
            return true;
        }

        return TBase::SetBuiltinAttribute(key, value, force);
    }

    static ISystemAttributeProvider* GetTargetBuiltinAttributeProvider(IYPathServicePtr service)
    {
        return dynamic_cast<ISystemAttributeProvider*>(service.Get());
    }

    IYPathServicePtr GetService()
    {
        return Producer_.Run(this);
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

    EObjectType GetObjectType() const override
    {
        return ObjectType_;
    }

    ENodeType GetNodeType() const override
    {
        return ENodeType::Entity;
    }

    bool HasBranchedChangesImpl(TVirtualNode* /*originatingNode*/, TVirtualNode* /*branchedNode*/) override
    {
        // Treat virtual nodes as always different because explicitly unlocking
        // them makes little sense anyway.
        return true;
    }

private:
    const TYPathServiceProducer Producer_;
    const EObjectType ObjectType_;
    const EVirtualNodeOptions Options_;


    ICypressNodeProxyPtr DoGetProxy(
        TVirtualNode* trunkNode,
        TTransaction* transaction) override
    {
        return New<TVirtualNodeProxy>(
            GetBootstrap(),
            &Metadata_,
            transaction,
            trunkNode,
            Options_,
            Producer_);
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
