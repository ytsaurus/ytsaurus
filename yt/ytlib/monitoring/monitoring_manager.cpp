#include "stdafx.h"
#include "monitoring_manager.h"
#include "private.h"

#include <core/concurrency/action_queue.h>
#include <core/concurrency/periodic_executor.h>

#include <core/ytree/ephemeral_node_factory.h>
#include <core/ytree/ypath_detail.h>
#include <core/ytree/tree_visitor.h>
#include <core/ytree/node.h>
#include <core/ytree/convert.h>

namespace NYT {
namespace NMonitoring {

using namespace NYTree;
using namespace NYPath;
using namespace NYson;
using namespace NRpc;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = MonitoringLogger;
static auto& Profiler = MonitoringProfiler;

static const auto UpdatePeriod = TDuration::Seconds(3);
static const auto EmptyRoot = GetEphemeralNodeFactory()->CreateMap();

////////////////////////////////////////////////////////////////////////////////

class TMonitoringManager::TImpl
    : public TRefCounted
{
public:
    void Register(const TYPath& path, TYsonProducer producer)
    {
        TGuard<TSpinLock> guard(SpinLock_);
        YCHECK(PathToProducer_.insert(std::make_pair(path, producer)).second);
    }

    void Unregister(const TYPath& path)
    {
        TGuard<TSpinLock> guard(SpinLock_);
        YCHECK(PathToProducer_.erase(path) == 1);
    }

    IYPathServicePtr GetService()
    {
        return New<TYPathService>(this);
    }

    void Start()
    {
        TGuard<TSpinLock> guard(SpinLock_);

        YCHECK(!Started_);

        PeriodicExecutor_ = New<TPeriodicExecutor>(
            ActionQueue_->GetInvoker(),
            BIND(&TImpl::Update, MakeWeak(this)),
            UpdatePeriod);
        PeriodicExecutor_->Start();

        Started_ = true;
    }

    void Stop()
    {
        TGuard<TSpinLock> guard(SpinLock_);

        if (!Started_)
            return;

        Started_ = false;
        PeriodicExecutor_->Stop();
        Root_.Reset();
    }

private:
    class TYPathService
        : public TYPathServiceBase
    {
    public:
        explicit TYPathService(TIntrusivePtr<TImpl> owner)
            : Owner_(owner)
        { }

        virtual TResolveResult Resolve(const TYPath& path, IServiceContextPtr context) override
        {
            return TResolveResult::There(Owner_->GetRoot(), path);
        }

    private:
        TIntrusivePtr<TImpl> Owner_;

    };

    bool Started_ = false;
    TActionQueuePtr ActionQueue_ = New<TActionQueue>("Monitoring");
    TPeriodicExecutorPtr PeriodicExecutor_;

    TSpinLock SpinLock_;
    yhash<Stroka, NYTree::TYsonProducer> PathToProducer_;
    INodePtr Root_;

    void Update()
    {
        LOG_DEBUG("Started updating monitoring state");
        PROFILE_TIMING ("/update_time") {
            auto newRoot = GetEphemeralNodeFactory()->CreateMap();

            for (const auto& pair : PathToProducer_) {
                auto value = ConvertToYsonString(pair.second);
                SyncYPathSet(newRoot, pair.first, value);
            }

            if (Started_) {
                TGuard<TSpinLock> guard(SpinLock_);
                Root_ = newRoot;
            }
        }
        LOG_DEBUG("Finished updating monitoring state");
    }

    INodePtr GetRoot()
    {
        TGuard<TSpinLock> guard(SpinLock_);
        return Root_ ? Root_ : EmptyRoot;
    }

};

////////////////////////////////////////////////////////////////////////////////

TMonitoringManager::TMonitoringManager()
    : Impl_(New<TImpl>())
{ }

TMonitoringManager::~TMonitoringManager()
{ }

void TMonitoringManager::Register(const TYPath& path, TYsonProducer producer)
{
    Impl_->Register(path, producer);
}

void TMonitoringManager::Unregister(const TYPath& path)
{
    Impl_->Unregister(path);
}

IYPathServicePtr TMonitoringManager::GetService()
{
    return Impl_->GetService();
}

void TMonitoringManager::Start()
{
    Impl_->Start();
}

void TMonitoringManager::Stop()
{
    Impl_->Stop();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMonitoring
} // namespace NYT
