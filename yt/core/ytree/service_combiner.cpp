#include "service_combiner.h"
#include "ypath_client.h"
#include "ypath_proxy.h"

#include <yt/core/concurrency/periodic_executor.h>

#include <yt/core/rpc/dispatcher.h>

#include <yt/core/yson/async_writer.h>
#include <yt/core/yson/tokenizer.h>

#include <yt/core/ypath/tokenizer.h>

#include <yt/core/ytree/convert.h>
#include <yt/core/ytree/fluent.h>

namespace NYT {
namespace NYTree {

using namespace NRpc;
using namespace NConcurrency;
using namespace NYPath;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

class TServiceCombiner::TImpl
    : public TSupportsAttributes
{
public:
    TImpl(
        std::vector<IYPathServicePtr> services,
        std::optional<TDuration> keysUpdatePeriod)
        : Services_(std::move(services))
    {
        auto workerInvoker = TDispatcher::Get()->GetHeavyInvoker();
        auto keysUpdateCallback = BIND(&TImpl::UpdateKeys, MakeWeak(this));
        if (keysUpdatePeriod) {
            UpdateKeysExecutor_ = New<TPeriodicExecutor>(workerInvoker, keysUpdateCallback, *keysUpdatePeriod);
            UpdateKeysExecutor_->Start();
        } else {
            workerInvoker->Invoke(keysUpdateCallback);
        }
    }

    TFuture<void> GetInitialized()
    {
        return InitializedPromise_.ToFuture();
    }

    void SetUpdatePeriod(TDuration period)
    {
        YCHECK(UpdateKeysExecutor_);
        UpdateKeysExecutor_->SetPeriod(period);
    }

private:
    const std::vector<IYPathServicePtr> Services_;

    NConcurrency::TPeriodicExecutorPtr UpdateKeysExecutor_;

    TPromise<void> InitializedPromise_ = NewPromise<void>();

    TSpinLock KeyMappingSpinLock_;
    using TKeyMappingOrError = TErrorOr<THashMap<TString, IYPathServicePtr>>;
    TKeyMappingOrError KeyMapping_;

private:
    virtual bool DoInvoke(const NRpc::IServiceContextPtr& context) override
    {
        DISPATCH_YPATH_SERVICE_METHOD(Get);
        DISPATCH_YPATH_SERVICE_METHOD(List);
        DISPATCH_YPATH_SERVICE_METHOD(Exists);
        return TSupportsAttributes::DoInvoke(context);
    }

    virtual TResolveResult ResolveRecursive(const TYPath& path, const NRpc::IServiceContextPtr& context) override
    {
        TGuard<TSpinLock> guard(KeyMappingSpinLock_);
        const auto& keyMapping = KeyMapping_.ValueOrThrow();

        NYPath::TTokenizer tokenizer(path);
        tokenizer.Advance();
        tokenizer.Expect(NYPath::ETokenType::Literal);
        const auto& key = tokenizer.GetLiteralValue();
        auto iterator = keyMapping.find(key);
        if (iterator == keyMapping.end()) {
            if (context->GetMethod() == "Exists") {
                return TResolveResultHere{path};
            }
            THROW_ERROR_EXCEPTION("Node has no child with key %Qv", ToYPathLiteral(key));
        }
        return TResolveResultThere{iterator->second, "/" + path};
    }


    virtual void GetSelf(TReqGet* request, TRspGet* response, const TCtxGetPtr& context) override
    {
        ValidateKeyMapping();

        i64 limit = request->has_limit()
            ? request->limit()
            : DefaultVirtualChildLimit;

        context->SetRequestInfo("Limit: %v", limit);

        std::atomic<bool> incomplete = {false};

        // TODO(max42): Make it more efficient :(
        std::vector<TFuture<THashMap<TString, INodePtr>>> serviceResultFutures;
        for (const auto& service : Services_) {
            auto innerRequest = TYPathProxy::Get(TYPath());
            innerRequest->set_limit(limit);
            if (request->has_attributes()) {
                innerRequest->mutable_attributes()->mutable_keys()->CopyFrom(request->attributes().keys());
            }
            auto asyncInnerResult = ExecuteVerb(service, innerRequest)
                .Apply(BIND([&] (TYPathProxy::TRspGetPtr response) {
                    auto node = ConvertToNode(TYsonString(response->value()));
                    if (node->Attributes().Get("incomplete", false)) {
                        incomplete = true;
                    }
                    return ConvertTo<THashMap<TString, INodePtr>>(node);
                }));
            serviceResultFutures.push_back(asyncInnerResult);
        }
        auto asyncResult = Combine(serviceResultFutures);
        auto serviceResults = WaitFor(asyncResult)
            .ValueOrThrow();

        THashMap<TString, INodePtr> combinedServiceResults;
        for (const auto& serviceResult : serviceResults) {
            if (static_cast<i64>(serviceResult.size() + combinedServiceResults.size()) > limit) {
                combinedServiceResults.insert(
                    serviceResult.begin(),
                    std::next(serviceResult.begin(), limit - static_cast<i64>(serviceResult.size())));
                incomplete = true;
                break;
            } else {
                combinedServiceResults.insert(serviceResult.begin(), serviceResult.end());
            }
        }

        TStringStream stream;
        TYsonWriter writer(&stream);

        if (incomplete) {
            BuildYsonFluently(&writer)
                .BeginAttributes()
                    .Item("incomplete").Value(true)
                .EndAttributes();
        }
        BuildYsonFluently(&writer)
            .DoMapFor(combinedServiceResults, [] (TFluentMap fluent, const decltype(combinedServiceResults)::value_type& item) {
                fluent
                    .Item(item.first).Value(item.second);
            });

        writer.Flush();
        response->set_value(stream.Str());
        context->Reply();
    }

    virtual void ListSelf(TReqList* request, TRspList* response, const TCtxListPtr& context) override
    {
        ValidateKeyMapping();

        i64 limit = request->has_limit()
            ? request->limit()
            : DefaultVirtualChildLimit;

        context->SetRequestInfo("Limit: %v", limit);

        std::atomic<bool> incomplete = {false};

        std::vector<TFuture<std::vector<IStringNodePtr>>> serviceResultFutures;
        for (const auto& service : Services_) {
            auto innerRequest = TYPathProxy::List(TYPath());
            innerRequest->set_limit(limit);
            if (request->has_attributes()) {
                innerRequest->mutable_attributes()->mutable_keys()->CopyFrom(request->attributes().keys());
            }
            auto asyncInnerResult = ExecuteVerb(service, innerRequest)
                .Apply(BIND([&] (TYPathProxy::TRspListPtr response) {
                    auto node = ConvertToNode(TYsonString(response->value()));
                    if (node->Attributes().Get("incomplete", false)) {
                        incomplete = true;
                    }
                    return ConvertTo<std::vector<IStringNodePtr>>(node);
                }));
            serviceResultFutures.push_back(asyncInnerResult);
        }
        auto asyncResult = Combine(serviceResultFutures);
        auto serviceResults = WaitFor(asyncResult)
            .ValueOrThrow();

        std::vector<IStringNodePtr> combinedServiceResults;
        for (const auto& serviceResult : serviceResults) {
            if (static_cast<i64>(serviceResult.size() + combinedServiceResults.size()) > limit) {
                combinedServiceResults.insert(
                    combinedServiceResults.end(),
                    serviceResult.begin(),
                    std::next(serviceResult.begin(), limit - static_cast<i64>(serviceResult.size())));
                incomplete = true;
                break;
            } else {
                combinedServiceResults.insert(combinedServiceResults.end(), serviceResult.begin(), serviceResult.end());
            }
        }

        TStringStream stream;
        TYsonWriter writer(&stream);

        if (incomplete) {
            BuildYsonFluently(&writer)
                .BeginAttributes()
                    .Item("incomplete").Value(true)
                .EndAttributes();
        }

        // There is a small chance that while we waited for all services to respond, they moved into an inconsistent
        // state and provided us with non-disjoint lists. In this case we force the list to contain only unique keys.
        THashSet<TString> keys;

        BuildYsonFluently(&writer)
            .DoListFor(combinedServiceResults, [&] (TFluentList fluent, const IStringNodePtr& item) {
                if (!keys.contains(item->GetValue())) {
                    fluent
                        .Item().Value(item);
                    keys.insert(item->GetValue());
                }
            });

        writer.Flush();
        response->set_value(stream.Str());
        context->Reply();
    }


    void UpdateKeys()
    {
        std::vector<TFuture<std::vector<TString>>> serviceListFutures;
        for (const auto& service : Services_) {
            auto asyncList = AsyncYPathList(service, TYPath() /* path */, std::numeric_limits<i64>::max() /* limit */);
            serviceListFutures.push_back(asyncList);
        }
        auto asyncResult = Combine(serviceListFutures);
        auto serviceListsOrError = WaitFor(asyncResult);

        if (!serviceListsOrError.IsOK()) {
            SetKeyMapping(TError(serviceListsOrError));
            return;
        }
        const auto& serviceLists = serviceListsOrError.Value();

        TKeyMappingOrError newKeyMappingOrError;
        auto& newKeyMapping = newKeyMappingOrError.Value();

        for (int index = 0; index < static_cast<int>(Services_.size()); ++index) {
            for (const auto& key : serviceLists[index]) {
                auto pair = newKeyMapping.emplace(key, Services_[index]);
                if (!pair.second) {
                    SetKeyMapping(TError("Key %Qv is operated by more than one YPathService",
                        key));
                    return;
                }
            }
        }

        SetKeyMapping(std::move(newKeyMappingOrError));
    }

    void ValidateKeyMapping()
    {
        TGuard<TSpinLock> guard(KeyMappingSpinLock_);
        // If several services already share the same key, we'd better throw an error and do nothing.
        KeyMapping_.ThrowOnError();
    }

    void SetKeyMapping(TKeyMappingOrError keyMapping)
    {
        {
            TGuard<TSpinLock> guard(KeyMappingSpinLock_);
            KeyMapping_ = std::move(keyMapping);
        }
        InitializedPromise_.TrySet();
    }
};

////////////////////////////////////////////////////////////////////////////////

TServiceCombiner::TServiceCombiner(std::vector<IYPathServicePtr> services, std::optional<TDuration> keysUpdatePeriod)
    : Impl_(New<TImpl>(std::move(services), keysUpdatePeriod))
{ }

void TServiceCombiner::SetUpdatePeriod(TDuration period)
{
    Impl_->SetUpdatePeriod(period);
}

IYPathService::TResolveResult TServiceCombiner::Resolve(const TYPath& path, const NRpc::IServiceContextPtr& /*context*/)
{
    return TResolveResultHere{path};
}

void TServiceCombiner::Invoke(const NRpc::IServiceContextPtr& context)
{
    Impl_->GetInitialized().Subscribe(BIND([impl = Impl_, context = context] (const TError& error) {
        if (error.IsOK()) {
            ExecuteVerb(impl, context);
        } else {
            context->Reply(error);
        }
    }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

