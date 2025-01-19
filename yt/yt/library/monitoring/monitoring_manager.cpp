#include "monitoring_manager.h"
#include "private.h"

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/ephemeral_node_factory.h>
#include <yt/yt/core/ytree/node.h>
#include <yt/yt/core/ytree/tree_visitor.h>
#include <yt/yt/core/ytree/ypath_detail.h>
#include <yt/yt/core/ytree/ypath_client.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NMonitoring {

using namespace NYTree;
using namespace NYPath;
using namespace NYson;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = MonitoringLogger;

static constexpr auto UpdatePeriod = TDuration::Seconds(3);
static const auto EmptyRoot = GetEphemeralNodeFactory()->CreateMap();

////////////////////////////////////////////////////////////////////////////////

class TMonitoringManager
    : public IMonitoringManager
{
public:
    TMonitoringManager()
        : PeriodicExecutor_(New<TPeriodicExecutor>(
            ActionQueue_->GetInvoker(),
            BIND(&TMonitoringManager::Update, MakeWeak(this)),
            UpdatePeriod))
    { }

    void Register(const TYPath& path, TYsonProducer producer) final
    {
        auto guard = Guard(SpinLock_);
        YT_VERIFY(PathToProducer_.emplace(path, producer).second);
    }

    void Unregister(const TYPath& path) final
    {
        auto guard = Guard(SpinLock_);
        YT_VERIFY(PathToProducer_.erase(path) == 1);
    }

    IYPathServicePtr GetService() final
    {
        return New<TYPathService>(this);
    }

    void Start() final
    {
        auto guard = Guard(SpinLock_);

        YT_VERIFY(!Started_);

        PeriodicExecutor_->Start();

        Started_ = true;
    }

    void Stop() final
    {
        auto guard = Guard(SpinLock_);

        if (!Started_)
            return;

        Started_ = false;
        YT_UNUSED_FUTURE(PeriodicExecutor_->Stop());
        Root_.Reset();
    }

private:
    class TYPathService
        : public TYPathServiceBase
    {
    public:
        explicit TYPathService(TIntrusivePtr<TMonitoringManager> owner)
            : Owner_(std::move(owner))
        { }

        TResolveResult Resolve(const TYPath& path, const IYPathServiceContextPtr& /*context*/) override
        {
            return TResolveResultThere{Owner_->GetRoot(), path};
        }

    private:
        const TIntrusivePtr<TMonitoringManager> Owner_;
    };

    const TActionQueuePtr ActionQueue_ = New<TActionQueue>("Monitoring");
    const TPeriodicExecutorPtr PeriodicExecutor_;

    bool Started_ = false;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);
    THashMap<TString, NYson::TYsonProducer> PathToProducer_;
    IMapNodePtr Root_;

    void Update()
    {
        YT_LOG_DEBUG("Started updating monitoring state");

        YT_PROFILE_TIMING("/monitoring/update_time") {
            auto newRoot = GetEphemeralNodeFactory()->CreateMap();

            THashMap<TString, NYson::TYsonProducer> pathToProducer;;
            {
                auto guard = Guard(SpinLock_);
                pathToProducer = PathToProducer_;
            }

            for (const auto& [path, producer] : pathToProducer) {
                auto value = ConvertToYsonString(producer);
                SyncYPathSet(newRoot, path, value);
            }

            if (Started_) {
                auto guard = Guard(SpinLock_);
                std::swap(Root_, newRoot);
            }
        }
        YT_LOG_DEBUG("Finished updating monitoring state");
    }

    IMapNodePtr GetRoot()
    {
        auto guard = Guard(SpinLock_);
        return Root_ ? Root_ : EmptyRoot;
    }
};

////////////////////////////////////////////////////////////////////////////////

IMonitoringManagerPtr CreateMonitoringManager()
{
    return New<TMonitoringManager>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NMonitoring
