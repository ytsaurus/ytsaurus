#include "medium_throttler_manager.h"

#include "distributed_throttler_manager.h"
#include "private.h"

#include <yt/yt/server/node/cellar_node/bundle_dynamic_config_manager.h>
#include <yt/yt/server/node/cellar_node/config.h>

#include <yt/yt/core/concurrency/throughput_throttler.h>

namespace NYT::NTabletNode {

using namespace NConcurrency;
using namespace NCellarNode;
using namespace NDistributedThrottler;
using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto UnlimitedThroughput = 1024_TB;

////////////////////////////////////////////////////////////////////////////////

namespace {

DEFINE_ENUM(EMediumLoadDirection,
    ((Write)    (0))
    ((Read)     (1))
);

std::string GetMediumThrottlerName(
    EMediumLoadDirection direction,
    const std::string& mediumName)
{
    static const TEnumIndexedArray<EMediumLoadDirection, std::string> DirectionNames{
        {EMediumLoadDirection::Write, "write"},
        {EMediumLoadDirection::Read, "read"},
    };

    return Format("%v_medium_%v", mediumName, DirectionNames[direction]);
}

std::optional<long> GetMediumThrottlerLimit(
    EMediumLoadDirection direction,
    const std::string& mediumName,
    const TBundleDynamicConfigPtr& bundleConfig)
{
    static const TEnumIndexedArray<EMediumLoadDirection, std::function<i64(const TMediumThroughputLimitsPtr&)>> DirectionLimitGetter = {
        {EMediumLoadDirection::Write, [] (const auto& mediumLimits) {
            return mediumLimits->WriteByteRate;
        }},
        {EMediumLoadDirection::Read, [] (const auto& mediumLimits) {
            return mediumLimits->ReadByteRate;
        }},
    };

    if (!bundleConfig) {
        return std::nullopt;
    }

    const auto& mediumThrottlerConfig = bundleConfig->MediumThroughputLimits;
    auto it = mediumThrottlerConfig.find(mediumName);
    if (it == mediumThrottlerConfig.end()) {
        return std::nullopt;
    }

    if (auto limit = DirectionLimitGetter[direction](it->second); limit) {
        return limit;
    }

    return std::nullopt;
}

TThroughputThrottlerConfigPtr GetMediumThrottlerConfig(
    EMediumLoadDirection direction,
    const std::string& mediumName,
    const TBundleDynamicConfigPtr& bundleConfig)
{
    auto result = New<TThroughputThrottlerConfig>();
    auto limit = GetMediumThrottlerLimit(direction, mediumName, bundleConfig);

    result->Limit = limit.value_or(UnlimitedThroughput);

    return result;
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
class TCopyOnWriteSet
    : private TNonCopyable
{
public:
    void Insert(const T& value)
    {
        auto guard = Guard(SpinLock_);
        if (Data_->contains(value)) {
            return;
        }

        if (!Data_.unique()) {
            Data_ = std::make_shared<THashSet<T>>(*Data_);
        }

        Data_->insert(value);
    }

    std::shared_ptr<const THashSet<T>> MakeSnapshot() const
    {
        auto guard = Guard(SpinLock_);

        return Data_;
    }

private:
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);
    std::shared_ptr<THashSet<T>> Data_ = std::make_shared<THashSet<T>>();
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TMediumThrottlerManager
    : public IMediumThrottlerManager
{
public:
    TMediumThrottlerManager(
        std::string bundleName,
        TBundleDynamicConfigManagerPtr dynamicConfigManager,
        IDistributedThrottlerManagerPtr distributedThrottlerManager)
        : BundleName_(std::move(bundleName))
        , BundlePath_(Format("//sys/tablet_cell_bundles/%v", ToYPathLiteral(BundleName_)))
        , DynamicConfigManager_(std::move(dynamicConfigManager))
        , DistributedThrottlerManager_(std::move(distributedThrottlerManager))
        , Profiler_(TabletNodeProfiler().WithPrefix("/distributed_throttlers")
            .WithRequiredTag("tablet_cell_bundle", BundleName_))
        , DynamicConfigCallback_(BIND(&TMediumThrottlerManager::OnDynamicConfigChanged, MakeWeak(this)))
    {
        OnDynamicConfigChanged(nullptr, DynamicConfigManager_->GetConfig());

        DynamicConfigManager_->SubscribeConfigChanged(DynamicConfigCallback_);
    }

    ~TMediumThrottlerManager()
    {
        DynamicConfigManager_->UnsubscribeConfigChanged(DynamicConfigCallback_);
    }

    IReconfigurableThroughputThrottlerPtr GetOrCreateMediumWriteThrottler(
        const std::string& mediumName,
        ETabletDistributedThrottlerKind kind)
    {
        TKey key(mediumName, kind);
        RegisteredWriteThrottlers_.Insert(key);

        return GetOrCreateThrottler(
            EMediumLoadDirection::Write,
            key,
            DynamicConfigManager_->GetConfig());
    }

    IReconfigurableThroughputThrottlerPtr GetOrCreateMediumReadThrottler(
        const std::string& mediumName,
        ETabletDistributedThrottlerKind kind)
    {
        TKey key(mediumName, kind);
        RegisteredReadThrottlers_.Insert(key);

        return GetOrCreateThrottler(
            EMediumLoadDirection::Read,
            key,
            DynamicConfigManager_->GetConfig());
    }

private:
    using TDynamicConfigCallback = TCallback<void(
        const TBundleDynamicConfigPtr& oldConfig,
        const TBundleDynamicConfigPtr& newConfig)>;
    using TKey = std::tuple<std::string, ETabletDistributedThrottlerKind>;

    const std::string BundleName_;
    const NYPath::TYPath BundlePath_;
    const TBundleDynamicConfigManagerPtr DynamicConfigManager_;
    const IDistributedThrottlerManagerPtr DistributedThrottlerManager_;
    const NProfiling::TProfiler Profiler_;

    TCopyOnWriteSet<TKey> RegisteredWriteThrottlers_;
    TCopyOnWriteSet<TKey> RegisteredReadThrottlers_;

    TEnumIndexedArray<EMediumLoadDirection, THashMap<std::string, NProfiling::TGauge>> ConfiguredLimits_ = {
        {EMediumLoadDirection::Write, { }},
        {EMediumLoadDirection::Read, { }}
    };
    const TDynamicConfigCallback DynamicConfigCallback_;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);

    IReconfigurableThroughputThrottlerPtr GetOrCreateThrottler(
        const EMediumLoadDirection& direction,
        const TKey& key,
        const TBundleDynamicConfigPtr& bundleConfig)
    {
        if (!DistributedThrottlerManager_) {
            return GetUnlimitedThrottler();
        }

        const auto& [mediumName, kind] = key;
        auto throttlerName = GetMediumThrottlerName(direction, mediumName);

        return DistributedThrottlerManager_->GetOrCreateThrottler(
            BundlePath_,
            /*cellTag*/ {},
            GetMediumThrottlerConfig(direction, mediumName, bundleConfig),
            throttlerName,
            kind,
            WriteThrottlerRpcTimeout,
            /*admitUnlimitedThrottler*/ true,
            Profiler_);
    }

    void OnDynamicConfigChanged(
        const TBundleDynamicConfigPtr& /*oldConfig*/,
        const TBundleDynamicConfigPtr& newConfig)
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        // In order to apply new parameters we have to just call GetOrCreateThrottler with a new config.
        for (const auto& key : *RegisteredWriteThrottlers_.MakeSnapshot()) {
            GetOrCreateThrottler(EMediumLoadDirection::Write, key, newConfig);
        }

        for (const auto& key : *RegisteredReadThrottlers_.MakeSnapshot()) {
            GetOrCreateThrottler(EMediumLoadDirection::Read, key, newConfig);
        }

        for (auto direction : TEnumTraits<EMediumLoadDirection>::GetDomainValues()) {
            auto& configuredLimits = ConfiguredLimits_[direction];
            if (!newConfig) {
                configuredLimits.clear();
                continue;
            }

            for (auto it = configuredLimits.begin(); it != configuredLimits.end(); ) {
                auto mediumName = it->first;
                if (auto limit = GetMediumThrottlerLimit(direction, mediumName, newConfig)) {
                    it->second.Update(limit.value());
                    ++it;
                } else {
                    configuredLimits.erase(it++);
                }
            }

            const auto& mediumThrottlerConfig = newConfig->MediumThroughputLimits;
            for (const auto& mediumName : GetKeys(mediumThrottlerConfig)) {
                if (auto limit = GetMediumThrottlerLimit(direction, mediumName, newConfig)) {
                    auto throttlerName = GetMediumThrottlerName(direction, mediumName);
                    auto configuredLimit = Profiler_.WithTag("throttler_id", throttlerName).Gauge("/configured_limit");
                    configuredLimit.Update(limit.value());
                    configuredLimits.emplace(mediumName, configuredLimit);
                }
            }
        }
    }
};

DEFINE_REFCOUNTED_TYPE(TMediumThrottlerManager);

////////////////////////////////////////////////////////////////////////////////

class TMediumThrottlerManagerFactory
    : public IMediumThrottlerManagerFactory
{
public:
    TMediumThrottlerManagerFactory(
        TBundleDynamicConfigManagerPtr dynamicConfigManager,
        IDistributedThrottlerManagerPtr distributedThrottlerManager)
        : DynamicConfigManager_(std::move(dynamicConfigManager))
        , DistributedThrottlerManager_(std::move(distributedThrottlerManager))
    { }

    IMediumThrottlerManagerPtr GetOrCreateMediumThrottlerManager(const std::string& bundleName) override
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        for (auto it = Managers_.begin(); it != Managers_.end(); ) {
            if (it->second.IsExpired()) {
                Managers_.erase(it++);
            } else {
                ++it;
            }
        }

        typename decltype(Managers_)::insert_ctx context;
        auto it = Managers_.find(bundleName, context);

        if (it != Managers_.end()) {
            if (auto manager = it->second.Lock()) {
                return manager;
            }
        }

        auto manager = New<TMediumThrottlerManager>(bundleName, DynamicConfigManager_, DistributedThrottlerManager_);
        auto weakManager = MakeWeak(manager);
        if (it != Managers_.end()) {
            it->second = std::move(weakManager);
        } else {
            Managers_.emplace_direct(context, bundleName, std::move(weakManager));
        }

        return manager;
    }

private:
    const TBundleDynamicConfigManagerPtr DynamicConfigManager_;
    const IDistributedThrottlerManagerPtr DistributedThrottlerManager_;

    THashMap<std::string, TWeakPtr<TMediumThrottlerManager>> Managers_;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
};

DEFINE_REFCOUNTED_TYPE(TMediumThrottlerManagerFactory);

////////////////////////////////////////////////////////////////////////////////

IMediumThrottlerManagerFactoryPtr CreateMediumThrottlerManagerFactory(
    TBundleDynamicConfigManagerPtr dynamicConfigManager,
    IDistributedThrottlerManagerPtr distributedThrottlerManager)
{
    return New<TMediumThrottlerManagerFactory>(
        std::move(dynamicConfigManager),
        std::move(distributedThrottlerManager));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
