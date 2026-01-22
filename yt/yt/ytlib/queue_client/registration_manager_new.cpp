#include "registration_manager_new.h"

#include "config.h"
#include "private.h"

#include "registration_manager_base.h"

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/hive/cluster_directory.h>

#include <yt/yt/client/federated/client.h>
#include <yt/yt/client/federated/config.h>

#include <yt/yt/client/object_client/public.h>

#include <yt/yt/client/tablet_client/table_mount_cache.h>

#include <yt/yt/core/concurrency/throughput_throttler.h>

#include <yt/yt/core/misc/async_expiring_cache.h>
#include <yt/yt/core/misc/jitter.h>

#include <library/cpp/iterator/zip.h>

#include <util/random/normal.h>

////////////////////////////////////////////////////////////////////////////////

template <>
struct THash<NYT::NQueueClient::NDetail::TRegistrationCacheKey>
{
    size_t operator()(const NYT::NQueueClient::NDetail::TRegistrationCacheKey& key) const;
};

////////////////////////////////////////////////////////////////////////////////

template <>
struct THash<NYT::NQueueClient::NDetail::TListRegistrationsCacheKey>
{
    size_t operator()(const NYT::NQueueClient::NDetail::TListRegistrationsCacheKey& key) const;
};

////////////////////////////////////////////////////////////////////////////////

template <>
struct THash<NYT::NQueueClient::NDetail::TReplicaMappingCacheKey>
{
    size_t operator()(const NYT::NQueueClient::NDetail::TReplicaMappingCacheKey& key) const;
};

////////////////////////////////////////////////////////////////////////////////

namespace NYT::NQueueClient::NDetail {

using namespace NApi;
using namespace NClient;
using namespace NConcurrency;
using namespace NHiveClient;
using namespace NLogging;
using namespace NObjectClient;
using namespace NProfiling;
using namespace NThreading;
using namespace NTracing;
using namespace NYPath;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

// XXX(apachee): Didn't see anything similar to that in our library.
template <typename T, typename F>
auto GroupBy(F f, std::vector<T> values)
{
    THashMap<std::invoke_result_t<F, const T&>, std::vector<T>> groupedValues;
    for (auto& value : values) {
        auto groupKey = f(value);
        groupedValues[groupKey].push_back(std::move(value));
    }
    return groupedValues;
}

////////////////////////////////////////////////////////////////////////////////

struct TRegistrationCacheKey
{
    TCrossClusterReference Queue;
    TCrossClusterReference Consumer;

    std::strong_ordering operator<=>(const TRegistrationCacheKey&) const = default;
};

void FormatValue(TStringBuilderBase* builder, const TRegistrationCacheKey& value, TStringBuf /*spec*/)
{
    builder->AppendFormat("{Queue: %v, Consumer: %v}", value.Queue, value.Consumer);
}

////////////////////////////////////////////////////////////////////////////////

//! Wrapper for non-trivial list registrations request.
//!
//! Non-trivial list registrations requests are those that request registrations for either a specific queue or a specific consumer.
//! Listing registrations with both queue and consumer set can be done using other cache.
//! Listing all registrations is forbidden.
struct TListRegistrationsCacheKey
{
    std::optional<TCrossClusterReference> Queue;
    std::optional<TCrossClusterReference> Consumer;

    explicit TListRegistrationsCacheKey(std::optional<TCrossClusterReference> queue, std::optional<TCrossClusterReference> consumer)
        : Queue(std::move(queue))
        , Consumer(std::move(consumer))
    {
        if (queue.has_value() && consumer.has_value()) {
            THROW_ERROR_EXCEPTION("Internal failure: trivial list registrations requests should be resolved using TRegistrationLookupCache");
        }
        if (!queue.has_value() && !consumer.has_value()) {
            THROW_ERROR_EXCEPTION("Internal failure: listing all registrations is forbidden");
        }
    }

    std::strong_ordering operator<=>(const TListRegistrationsCacheKey&) const = default;
};

void FormatValue(TStringBuilderBase* builder, const TListRegistrationsCacheKey& value, TStringBuf /*spec*/)
{
    builder->AppendFormat("{Queue: %v, Consumer: %v}", value.Queue, value.Consumer);
}

////////////////////////////////////////////////////////////////////////////////

struct TReplicaMappingCacheKey
{
    TCrossClusterReference Replica;
    std::strong_ordering operator<=>(const TReplicaMappingCacheKey&) const = default;
};

void FormatValue(TStringBuilderBase* builder, const TReplicaMappingCacheKey& value, TStringBuf /*spec*/)
{
    builder->AppendFormat("{Replica: %v}", value.Replica);
}

////////////////////////////////////////////////////////////////////////////////

// NB(apachee): Reflection is needed to use this enum as profiling tag.
// XXX(apachee): Profiling will be added separately.
DEFINE_ENUM(ELookupReason,
    (InitialLookup)
    (PeriodicUpdate)
);

////////////////////////////////////////////////////////////////////////////////

template <typename TTable, typename TCacheKeyType, typename TLookupResultType = TErrorOr<typename TTable::TRowType>>
class TLookupSessionBase
    : public TRefCounted
{
public:
    using TCacheKey = TCacheKeyType;
    using TLookupResult = TLookupResultType;
    using TLookupResultWrapped = TErrorTraits<TLookupResult>::TWrapped;
    using TLookupResultUnwrapped = TErrorTraits<TLookupResult>::TUnwrapped;

public:
    TLookupSessionBase(
        TWeakPtr<NNative::IConnection> connection,
        TLookupSessionConfigPtr config,
        std::vector<TCacheKey> keys,
        const TLogger logger)
        : Config_(std::move(config))
        , Keys_(std::move(keys))
        , Logger(logger)
        , Connection_(std::move(connection))
    { }

    auto Run() const
    {
        try {
            return RunGuarded();
        } catch (const std::exception& ex) {
            YT_LOG_ERROR(ex, "Lookup session failed");
            throw;
        }
    }

    std::vector<TLookupResultWrapped> RunGuarded() const
    {
        auto client = CreateTableClientOrThrow();
        auto table = New<TTable>(Config_->Table.GetPath(), client);

        // NB(apachee): Wrap result in TErrorOr in case it isn't already.
        auto unwrappedResult = DoLookup(std::move(table));
        if constexpr (std::is_same_v<TLookupResult, TLookupResultWrapped>) {
            return unwrappedResult;
        } else {
            return std::vector<TLookupResultWrapped>(
                std::make_move_iterator(unwrappedResult.begin()),
                std::make_move_iterator(unwrappedResult.end()));
        }
    }

protected:
    const TLookupSessionConfigPtr Config_;
    const std::vector<TCacheKey> Keys_;
    const TLogger Logger;

    virtual std::vector<TLookupResult> DoLookup(TIntrusivePtr<TTable> table) const = 0;

private:
    inline static const auto FederationConfig_ = New<NFederated::TFederationConfig>();

    const TWeakPtr<NNative::IConnection> Connection_;

    IClientPtr CreateTableClientOrThrow() const
    {
        auto localConnection = Connection_.Lock();
        if (!localConnection) {
            THROW_ERROR_EXCEPTION("Queue consumer registration cache owning connection expired");
        }

        auto clientOptions = NApi::TClientOptions::FromUser(Config_->User);

        const auto& tablePath = Config_->Table;
        auto clustersOrNull = tablePath.GetClusters();
        if (!clustersOrNull) {
            return localConnection->CreateClient(clientOptions);
        }
        const auto& clusters = *clustersOrNull;

        auto clientDirectory = New<TClientDirectory>(localConnection->GetClusterDirectory(), clientOptions);

        std::vector<IClientPtr> clients;
        clients.reserve(clusters.size());
        for (const auto& readCluster : clusters) {
            clients.push_back(DynamicPointerCast<IClient>(clientDirectory->GetClientOrThrow(readCluster)));
        }

        return NFederated::CreateClient(clients, FederationConfig_);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TRegistrationLookupSession
    : public TLookupSessionBase<TConsumerRegistrationTable, TRegistrationCacheKey>
{
public:
    using TLookupSessionBase::TLookupSessionBase;

private:
    std::vector<TErrorOr<TConsumerRegistrationTableRow>> DoLookup(TConsumerRegistrationTablePtr table) const override
    {
        std::vector<TConsumerRegistrationTableRow> dynamicStateKeys;
        dynamicStateKeys.reserve(Keys_.size());
        for (const auto& key : Keys_) {
            dynamicStateKeys.push_back(TConsumerRegistrationTableRow{
                .Queue = key.Queue,
                .Consumer = key.Consumer,
            });
        }

        return WaitFor(table->Lookup(dynamicStateKeys))
            .ValueOrThrow();
    }
};

DEFINE_REFCOUNTED_TYPE(TRegistrationLookupSession)

////////////////////////////////////////////////////////////////////////////////

class TListRegistrationsSession
    : public TLookupSessionBase<TConsumerRegistrationTable, TListRegistrationsCacheKey, std::vector<TConsumerRegistrationTableRow>>
{
public:
    using TLookupSessionBase::TLookupSessionBase;

private:
    static constexpr auto QueueClusterColumnName_ = "queue_cluster";
    static constexpr auto QueuePathColumnName_ = "queue_path";
    static constexpr auto QueuesPlaceholderValueName_ = "queues";

    static constexpr auto ConsumerClusterColumnName_ = "consumer_cluster";
    static constexpr auto ConsumerPathColumnName_ = "consumer_path";
    static constexpr auto ConsumersPlaceholderValueName_ = "consumers";

    std::vector<std::vector<TConsumerRegistrationTableRow>> DoLookup(TConsumerRegistrationTablePtr table) const override
    {
        // NB(apachee): Splitting queries should lead to better performance, since one of them can deduce row ranges from query,
        // and the other query becomes lighter in terms of complexity.
        auto asyncRegistrationsByQueue = ListByQueue(table);
        auto asyncRegistrationsByConsumer = ListByConsumer(table);
        WaitFor(AllSucceeded(std::vector{
            asyncRegistrationsByQueue.AsVoid(),
            asyncRegistrationsByConsumer.AsVoid(),
        }))
            .ThrowOnError();

        auto registrationsByQueue = asyncRegistrationsByQueue
            .Get()
            .Value();
        auto registrationsByConsumer = asyncRegistrationsByConsumer
            .Get()
            .Value();

        std::vector<std::vector<TConsumerRegistrationTableRow>> result;
        for (const auto& key : Keys_) {
            YT_VERIFY(!key.Queue.has_value() || !key.Consumer.has_value());
            // FIXME(apachee): Add test with duplicate keys after batching is introduced.
            if (auto queue = key.Queue; queue.has_value()) {
                result.push_back(GetOrDefault(registrationsByQueue, *queue, {}));
            } else if (auto consumer = key.Consumer; consumer.has_value()) {
                result.push_back(GetOrDefault(registrationsByConsumer, *consumer, {}));
            } else {
                YT_ABORT();
            }
        }

        return result;
    }

private:
    TFuture<THashMap<TCrossClusterReference, std::vector<TConsumerRegistrationTableRow>>> ListByQueue(const TConsumerRegistrationTablePtr& table) const
    {
        static const auto query = Format(
            "([%v], [%v]) IN {%v}",
            QueueClusterColumnName_,
            QueuePathColumnName_,
            QueuesPlaceholderValueName_);

        std::vector<std::pair<TString, TString>> queues;
        for (const auto& key : Keys_) {
            if (auto queue = key.Queue; queue.has_value()) {
                queues.emplace_back(queue->Cluster, queue->Path);
            }
        }

        TSelectRowsOptions options;
        options.PlaceholderValues = BuildYsonStringFluently()
            .BeginMap()
                .Item(QueuesPlaceholderValueName_).Value(std::move(queues))
            .EndMap();

        return table->Select(query, options)
            .AsUnique()
            .Apply(BIND([] (std::vector<TConsumerRegistrationTableRow>&& result) {
                return GroupBy([] (const auto& v) { return v.Queue; }, std::move(result));
            }));
    }

    TFuture<THashMap<TCrossClusterReference, std::vector<TConsumerRegistrationTableRow>>> ListByConsumer(const TConsumerRegistrationTablePtr& table) const
    {
        static const auto query = Format(
            "([%v], [%v]) IN {%v}",
            ConsumerClusterColumnName_,
            ConsumerPathColumnName_,
            ConsumersPlaceholderValueName_);

        std::vector<std::pair<TString, TString>> consumers;
        for (const auto& key : Keys_) {
            if (auto consumer = key.Consumer; consumer.has_value()) {
                consumers.emplace_back(consumer->Cluster, consumer->Path);
            }
        }

        TSelectRowsOptions options;
        options.PlaceholderValues = BuildYsonStringFluently()
            .BeginMap()
                .Item(ConsumersPlaceholderValueName_).Value(std::move(consumers))
            .EndMap();

        return table->Select(query, options)
            .AsUnique()
            .Apply(BIND([] (std::vector<TConsumerRegistrationTableRow>&& result) {
                return GroupBy([] (const auto& v) { return v.Consumer; }, std::move(result));
            }));
    }
};

DEFINE_REFCOUNTED_TYPE(TListRegistrationsSession)

////////////////////////////////////////////////////////////////////////////////

class TReplicaMappingLookupSession
    : public TLookupSessionBase<TReplicaMappingTable, TReplicaMappingCacheKey>
{
public:
    using TLookupSessionBase::TLookupSessionBase;

private:
    static constexpr auto ReplicaColumnName_ = "replica_list";
    static constexpr auto ReplicasPlaceholderValueName_ = "replicas";

    std::vector<TErrorOr<TReplicaMappingTableRow>> DoLookup(TReplicaMappingTablePtr table) const override
    {
        static const auto query = Format(
            "([%v]) IN {%v}",
            ReplicaColumnName_,
            ReplicasPlaceholderValueName_);

        TSelectRowsOptions options;
        options.PlaceholderValues = BuildYsonStringFluently()
            .BeginMap()
                .Item(ReplicasPlaceholderValueName_)
                .BeginList()
                    .DoFor(Keys_, [] (TFluentList fluent, const TReplicaMappingCacheKey& key) {
                        fluent
                            .Item()
                            .Value(ToString(key.Replica));
                    })
                .EndList()
            .EndMap();

        auto rows = WaitFor(table->Select(query, options))
            .ValueOrThrow();
        THashMap<TCrossClusterReference, TReplicaMappingTableRow> replicaMapping;
        for (auto& row : rows) {
            // NB(apachee): This does not take into account that chaos replica might have 2 or more corresponding CRTs.
            // Current implementation somewhat mirrors the old one, but not exactly, as both choose random corresponding CRT.
            // TODO(apachee): In case of chaos replicas only choose CRTs that own its replication card.
            auto replica = row.ReplicaRef;
            replicaMapping[replica] = std::move(row);
        }

        std::vector<TErrorOr<TReplicaMappingTableRow>> result;
        result.reserve(Keys_.size());
        for (const auto& key : Keys_) {
            auto it = replicaMapping.find(key.Replica);
            if (it == replicaMapping.end()) {
                result.push_back(TError(EErrorCode::DynamicStateMissingRow, "Requested key does not exist"));
            } else {
                result.push_back(std::move(it->second));
            }
        }

        return result;
    }
};

DEFINE_REFCOUNTED_TYPE(TReplicaMappingLookupSession)

////////////////////////////////////////////////////////////////////////////////

template <typename TLookupSession, EQueueConsumerRegistrationManagerCacheKind CacheKindValue>
class TStateLookupCache
    : public TAsyncExpiringCache<typename TLookupSession::TCacheKey, typename TLookupSession::TLookupResultUnwrapped>
{
public:
    using TBase = TAsyncExpiringCache<typename TLookupSession::TCacheKey, typename TLookupSession::TLookupResultUnwrapped>;
    using TKey = typename TLookupSession::TCacheKey;
    using TValue = typename TLookupSession::TLookupResultUnwrapped;

    static constexpr auto CacheKind = CacheKindValue;

public:
    TStateLookupCache(
        TWeakPtr<NApi::NNative::IConnection> connection,
        NLogging::TLogger logger,
        NProfiling::TProfiler profiler,
        IInvokerPtr invoker,
        const TQueueConsumerRegistrationManagerConfigPtr& config)
        : TBase(
            TCompoundStateLookupCacheConfig::FromQueueConsumerRegistrationManagerConfig(config, CacheKind)->Cache,
            invoker,
            logger,
            profiler)
        , Connection_(std::move(connection))
        , Invoker_(std::move(invoker))
        , Logger(logger)
        , Config_(TCompoundStateLookupCacheConfig::FromQueueConsumerRegistrationManagerConfig(config, CacheKind))
    { }

    void Reconfigure(const TQueueConsumerRegistrationManagerConfigPtr& newConfig)
    {
        DoReconfigure(TCompoundStateLookupCacheConfig::FromQueueConsumerRegistrationManagerConfig(newConfig, CacheKind));
    }

    void BuildOrchid(NYTree::TFluentAny fluent)
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        fluent
            .BeginMap()
            // XXX(apachee): Batch lookup orchid will be added.
            .EndMap();
    }

private:
    const TWeakPtr<NNative::IConnection> Connection_;
    const IInvokerPtr Invoker_;
    const TLogger Logger;

    YT_DECLARE_SPIN_LOCK(TSpinLock, ConfigurationLock_);
    TCompoundStateLookupCacheConfigPtr Config_;

    void DoReconfigure(TCompoundStateLookupCacheConfigPtr newConfig)
    {
        auto guard = Guard(ConfigurationLock_);

        auto oldConfig = std::move(Config_);
        Config_ = newConfig;

        TBase::Reconfigure(newConfig->Cache);

        if (static_cast<TLookupSessionConfig&>(*oldConfig) != static_cast<TLookupSessionConfig&>(*newConfig)) {
            // XXX(apachee): Batch lookup reconfigure will be added here.

            if (oldConfig->Table != newConfig->Table) {
                YT_LOG_DEBUG(
                    "Invalidating all cache entries, since table path has changed (OldTablePath: %v, NewTablePath: %v)",
                    oldConfig->Table,
                    newConfig->Table);
                TBase::Clear();
            }
        }

        // XXX(apachee): Batch lookup reconfigure will be added here.
    }

    // XXX(apachee): #isBatched will be used later.
    static TError EnrichError(TError error, bool isBatched = false)
    {
        return error
            << TErrorAttribute("cache_kind", Format("%lv", CacheKind))
            << TErrorAttribute("lookup_request_batched", isBatched);
    }

    TFuture<TValue> DoGet(
        const TKey& key,
        bool isPeriodicUpdate) noexcept override
    {
        auto config = GetConfig();

        // TODO(apachee): Add conditional lookup batching.

        return DoGetManyImpl(
            std::vector{key},
            isPeriodicUpdate,
            std::move(config))
            .AsUnique()
            .Apply(BIND([] (TErrorOr<std::vector<TErrorOr<TValue>>>&& resultOrError)
                -> TErrorOr<TValue>
            {
                if (!resultOrError.IsOK()) {
                    return TError(std::move(resultOrError));
                }

                auto& result = resultOrError.Value();
                YT_VERIFY(result.size() == 1);

                return std::move(result[0]);
            }));
    }

    TFuture<std::vector<TErrorOr<TValue>>> DoGetMany(
        const std::vector<TKey>& keys,
        bool isPeriodicUpdate) noexcept override
    {
        return DoGetManyImpl(
            keys,
            isPeriodicUpdate,
            GetConfig());
    }

    TFuture<std::vector<TErrorOr<TValue>>> DoGetManyImpl(
        const std::vector<TKey>& keys,
        bool /*isPeriodicUpdate*/,
        TLookupSessionConfigPtr config) noexcept
    {
        auto session = New<TLookupSession>(
            Connection_,
            std::move(config),
            keys,
            Logger);

        return BIND(
            &TLookupSession::Run,
            std::move(session))
            .AsyncVia(Invoker_)
            .Run()
            .AsUnique()
            .Apply(BIND([] (TErrorOr<std::vector<TErrorOr<TValue>>>&& resultOrError) -> TErrorOr<std::vector<TErrorOr<TValue>>> {
                if (!resultOrError.IsOK()) {
                    return EnrichError(std::move(resultOrError));
                }

                for (auto& result : resultOrError.Value()) {
                    if (!result.IsOK()) {
                        result = EnrichError(std::move(result));
                    }
                }

                return std::move(resultOrError);
            }));
    }

    void OnAdded(const TKey& key) noexcept override
    {
        YT_LOG_DEBUG("State lookup added to cache (Key: %v)", key);
    }

    void OnRemoved(const TKey& key) noexcept override
    {
        YT_LOG_DEBUG("State lookup removed from cache (Key: %v)", key);
    }

    TCompoundStateLookupCacheConfigPtr GetConfig() const
    {
        auto guard = Guard(ConfigurationLock_);
        return Config_;
    }
};

////////////////////////////////////////////////////////////////////////////////

template <typename TCache>
auto CreateCache(
    TWeakPtr<NApi::NNative::IConnection> connection,
    NLogging::TLogger logger,
    NProfiling::TProfiler profiler,
    IInvokerPtr invoker,
    const TQueueConsumerRegistrationManagerConfigPtr& config)
{
    static const TString CacheKindTag = Format("%lv", TCache::CacheKind);

    // With prefix we get these names:
    // - QueueConsumerRegistrationLookup,
    // - QueueConsumerListRegistrations,
    // - QueueConsumerReplicaMappingLookup.
    static constexpr TStringBuf CacheNamePrefix = "QueueConsumer";
    logger = std::move(logger)
        .WithTag("Cache: %v%v", CacheNamePrefix, Format("%v", TCache::CacheKind));

    profiler = profiler
        .WithPrefix("/lookup_cache")
        .WithTag("cache_kind", CacheKindTag);

    return New<TCache>(
        std::move(connection),
        std::move(logger),
        std::move(profiler),
        std::move(invoker),
        config);
}

////////////////////////////////////////////////////////////////////////////////

#define XX(cacheKind) \
    template class TStateLookupCache<T ## cacheKind ## Session, EQueueConsumerRegistrationManagerCacheKind::cacheKind>; \
    using T ## cacheKind ## Cache = TStateLookupCache<T ## cacheKind ## Session, EQueueConsumerRegistrationManagerCacheKind::cacheKind>; \
    using T ## cacheKind ## CachePtr = TIntrusivePtr<T ## cacheKind ## Cache>; \
    DEFINE_REFCOUNTED_TYPE(T ## cacheKind ## Cache) \

XX(RegistrationLookup)
XX(ListRegistrations)
XX(ReplicaMappingLookup)

static_assert(std::is_same_v<TRegistrationLookupCache::TValue, TConsumerRegistrationTableRow>);
static_assert(std::is_same_v<TListRegistrationsCache::TValue, std::vector<TConsumerRegistrationTableRow>>);
static_assert(std::is_same_v<TReplicaMappingLookupCache::TValue, TReplicaMappingTableRow>);

#undef XX

// TODO(apachee): Use SubscribeOnClusterUpdated of cluster directory for better
class TQueueConsumerRegistrationManagerNewImpl
    : public TQueueConsumerRegistrationManagerBase
{
public:
    using TBase = TQueueConsumerRegistrationManagerBase;

public:
    TQueueConsumerRegistrationManagerNewImpl(
        TQueueConsumerRegistrationManagerConfigPtr config,
        TWeakPtr<NApi::NNative::IConnection> connection,
        std::optional<std::string> clusterName,
        IInvokerPtr invoker,
        TProfiler profiler,
        TLogger logger)
        : TBase(
            config,
            connection,
            std::move(clusterName),
            std::move(invoker),
            profiler,
            logger)
        , RegistrationLookupCache_(CreateCache<TRegistrationLookupCache>(
            connection,
            Logger,
            profiler,
            Invoker_,
            config))
        , ListRegistrationsCache_(CreateCache<TListRegistrationsCache>(connection,
            Logger,
            profiler,
            Invoker_,
            config))
        , ReplicaMappingLookupCache_(CreateCache<TReplicaMappingLookupCache>(
            connection,
            Logger,
            profiler,
            Invoker_,
            config))
    { }

private:
    TRegistrationLookupCachePtr RegistrationLookupCache_;
    TListRegistrationsCachePtr ListRegistrationsCache_;
    TReplicaMappingLookupCachePtr ReplicaMappingLookupCache_;

    std::optional<TConsumerRegistrationTableRow> DoFindRegistration(
        NYPath::TRichYPath resolvedQueue,
        NYPath::TRichYPath resolvedConsumer) override
    {
        auto resultOrError = WaitFor(RegistrationLookupCache_->Get(TRegistrationCacheKey{
            .Queue = TCrossClusterReference::FromRichYPath(resolvedQueue),
            .Consumer = TCrossClusterReference::FromRichYPath(resolvedConsumer),
        }));

        if (!resultOrError.IsOK()) {
            // NB(apachee): Error for missing registration is handled in base class.
            if (resultOrError.FindMatching(EErrorCode::DynamicStateMissingRow)) {
                return std::nullopt;
            }

            THROW_ERROR_EXCEPTION("Queue consumer registration resolution failed")
                << TError(std::move(resultOrError));
        }

        return resultOrError.Value();
    }

    std::vector<TConsumerRegistrationTableRow> DoListRegistrations(
        std::optional<NYPath::TRichYPath> resolvedQueue,
        std::optional<NYPath::TRichYPath> resolvedConsumer) override
    {
        YT_VERIFY(resolvedQueue || resolvedConsumer);

        std::vector<TConsumerRegistrationTableRow> result;

        try {
            result = DoListRegistrationsGuarded(resolvedQueue, resolvedConsumer);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Failed to list queue consumer registrations")
                << ex;
        }

        return result;
    }

    std::vector<TConsumerRegistrationTableRow> DoListRegistrationsGuarded(
        std::optional<NYPath::TRichYPath> resolvedQueue,
        std::optional<NYPath::TRichYPath> resolvedConsumer)
    {
        // NB(apachee): #TListRegistrationsCache is only used for listing registrations by queue or consumer.
        if (resolvedQueue && resolvedConsumer) {
            auto registrationOrError = WaitFor(RegistrationLookupCache_->Get(TRegistrationCacheKey{
                .Queue = TCrossClusterReference::FromRichYPath(*resolvedQueue),
                .Consumer = TCrossClusterReference::FromRichYPath(*resolvedConsumer),
            }));

            if (!registrationOrError.IsOK() && !registrationOrError.FindMatching(EErrorCode::DynamicStateMissingRow)) {
                THROW_ERROR_EXCEPTION(registrationOrError);
            }

            return registrationOrError.IsOK()
                ? std::vector{std::move(registrationOrError.Value())}
                : std::vector<TConsumerRegistrationTableRow>();
        }

        return WaitFor(ListRegistrationsCache_->Get(TListRegistrationsCacheKey(
            resolvedQueue
                ? std::optional(TCrossClusterReference::FromRichYPath(*resolvedQueue))
                : std::nullopt,
            resolvedConsumer
                ? std::optional(TCrossClusterReference::FromRichYPath(*resolvedConsumer))
                : std::nullopt)))
            .ValueOrThrow();
    }

    NYPath::TRichYPath ResolveReplica(
        const NYPath::TRichYPath& objectPath,
        const NTabletClient::TTableMountInfoPtr& tableMountInfo,
        bool throwOnFailure) const override
    {
        // TODO(apachee): Metric for successful and failed replica mapping lookups. That helps verifying that new implementation is working as expected.

        // NB(apachee): I kept the old implementation, which is:
        // - Lookup replicated table mapping table to resolve replicated table path from replica path.
        // - Check upstream replica id, iff lookup failed.
        //
        // I think that leaves the possibility of resolving replicas with null upstream replica id in case replicated table
        // has preserve_timestamps equal to false. That's how it worked before, but in future it will be better to
        // not resolve such replicas, since Queue API, generally, can't work with those. After changing cypress synchronizer
        // logic to verify replicated table mapping by checking upstream replica id of replicas we won't be able to resolve such replicas,
        // so at this point in the future this code can be re-worked for better clarity.
        // TODO(apachee): Re-work this code for better clarity as mentioned above.

        auto resultOrError = WaitFor(ReplicaMappingLookupCache_->Get(TReplicaMappingCacheKey{
            .Replica = TCrossClusterReference::FromRichYPath(objectPath),
        }));

        if (resultOrError.IsOK()) {
            auto result = resultOrError.Value().ReplicatedTableRef;
            YT_LOG_DEBUG(
                "Using corresponding replicated table path in request instead of replica path (ReplicaPath: %v, ReplicatedTablePath: %v)",
                objectPath,
                result);
            return result;
        }

        if (tableMountInfo->UpstreamReplicaId != NullObjectId && throwOnFailure) {
            if (resultOrError.FindMatching(EErrorCode::DynamicStateMissingRow)) {
                // NB(apachee): This may happen, since replicated table mapping is not updated immediately after the replica is created, i.e. we need to wait for a fresh cypress synchronizer pass.
                THROW_ERROR_EXCEPTION("Unable to map replica %Qv to replicated table; if issue persists for more than 10 minutes, please contact YT support", objectPath);
            } else {
                THROW_ERROR_EXCEPTION(
                    "Cannot perform request for replica %Qv with unknown [chaos_]replicated_table; please contact YT support,"
                    " unless you specifically understand what this error means",
                    objectPath)
                    << TError(std::move(resultOrError));
            }
        }

        return objectPath;
    }

    void Reconfigure(
        const TQueueConsumerRegistrationManagerConfigPtr& oldConfig,
        const TQueueConsumerRegistrationManagerConfigPtr& newConfig) override
    {
        TBase::Reconfigure(oldConfig, newConfig);

        RegistrationLookupCache_->Reconfigure(newConfig);
        ListRegistrationsCache_->Reconfigure(newConfig);
        ReplicaMappingLookupCache_->Reconfigure(newConfig);

        YT_LOG_DEBUG(
            "Queue consumer registration manager dynamic config changed (OldConfig: %v, NewConfig: %v)",
            ConvertToYsonString(oldConfig, EYsonFormat::Text),
            ConvertToYsonString(newConfig, EYsonFormat::Text));
    }

    void RefreshCache() override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();
        // NB(apachee): Clearing all caches effectively refreshes the cache, since new implementation fetches data upon initial lookup.
        Clear();
    }

    EQueueConsumerRegistrationManagerImplementation GetImplementationType() const override
    {
        return EQueueConsumerRegistrationManagerImplementation::AsyncExpiringCache;
    }

    void Clear() override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        // NB(apachee): It should be OK to clear caches without synchronization.

        RegistrationLookupCache_->Clear();
        ListRegistrationsCache_->Clear();
        ReplicaMappingLookupCache_->Clear();

        YT_LOG_DEBUG("Cleared queue consumer registration manager cache");
    }

    void BuildOrchid(NYTree::TFluentAny fluent) override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        auto config = GetDynamicConfig();
        YT_VERIFY(config);

        fluent
            .BeginAttributes()
                .Item("queue_consumer_registration_manager_implementation").Value(GetImplementationType())
            .EndAttributes()
            .BeginMap()
                .Item("effective_config").Value(config)
                // XXX(apachee): Currently always empty, but will be used later on.
                .Item("unrecognized_config_options").Value(config->GetRecursiveUnrecognized())
                .Item("cache")
                    .BeginMap()
                    .Item("registration_lookup").Do([this] (TFluentAny fluent) {
                        RegistrationLookupCache_->BuildOrchid(fluent);
                    })
                    .Item("list_registrations").Do([this] (TFluentAny fluent) {
                        ListRegistrationsCache_->BuildOrchid(fluent);
                    })
                    .Item("replica_mapping_lookup").Do([this] (TFluentAny fluent) {
                        ReplicaMappingLookupCache_->BuildOrchid(fluent);
                    })
                    .EndMap()
            .EndMap();
    }
};

////////////////////////////////////////////////////////////////////////////////

TQueueConsumerRegistrationManagerBasePtr CreateQueueConsumerRegistrationManagerNewImpl(
    TQueueConsumerRegistrationManagerConfigPtr config,
    TWeakPtr<NApi::NNative::IConnection> connection,
    std::optional<std::string> clusterName,
    IInvokerPtr invoker,
    NProfiling::TProfiler profiler,
    NLogging::TLogger logger)
{
    auto result = New<TQueueConsumerRegistrationManagerNewImpl>(
        std::move(config),
        std::move(connection),
        std::move(clusterName),
        std::move(invoker),
        std::move(profiler),
        std::move(logger));

    result->Initialize();
    return result;
}

////////////////////////////////////////////////////////////////////////////////

}  // namespace NYT::NQueueClient::NDetail

////////////////////////////////////////////////////////////////////////////////

size_t THash<NYT::NQueueClient::NDetail::TRegistrationCacheKey>::operator()(
    const NYT::NQueueClient::NDetail::TRegistrationCacheKey& key) const
{
    size_t result = 0;
    NYT::HashCombine(result, key.Queue);
    NYT::HashCombine(result, key.Consumer);
    return result;
}

////////////////////////////////////////////////////////////////////////////////

size_t THash<NYT::NQueueClient::NDetail::TListRegistrationsCacheKey>::operator()(
    const NYT::NQueueClient::NDetail::TListRegistrationsCacheKey& key) const
{
    size_t result = 0;
    NYT::HashCombine(result, key.Queue);
    NYT::HashCombine(result, key.Consumer);
    return result;
}

////////////////////////////////////////////////////////////////////////////////

size_t THash<NYT::NQueueClient::NDetail::TReplicaMappingCacheKey>::operator()(
    const NYT::NQueueClient::NDetail::TReplicaMappingCacheKey& key) const
{
    return THash<NYT::NQueueClient::TCrossClusterReference>()(key.Replica);
}

////////////////////////////////////////////////////////////////////////////////
