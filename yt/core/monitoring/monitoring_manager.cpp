#include "stdafx.h"
#include "monitoring_manager.h"

#include <core/actions/bind.h>

#include <core/yson/writer.h>

#include <core/ytree/ephemeral_node_factory.h>
#include <core/ytree/ypath_detail.h>
#include <core/ytree/tree_visitor.h>
#include <core/ytree/ypath_proxy.h>
#include <core/ytree/node.h>
#include <core/ytree/convert.h>

#include <core/profiling/profiler.h>

namespace NYT {
namespace NMonitoring {

using namespace NYTree;
using namespace NRpc;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger Logger("Monitoring");
static NProfiling::TProfiler Profiler("/monitoring");

static const TDuration UpdatePeriod = TDuration::Seconds(3);

////////////////////////////////////////////////////////////////////////////////

class TMonitoringManager::TYPathService
    : public TYPathServiceBase
{
public:
    explicit TYPathService(TMonitoringManagerPtr owner)
        : Owner(owner)
    { }

    virtual TResolveResult Resolve(const TYPath& path, IServiceContextPtr context) override
    {
        auto root = Owner->Root;
        return root->Resolve(path, context);
    }

private:
    TMonitoringManagerPtr Owner;

};

////////////////////////////////////////////////////////////////////////////////

TMonitoringManager::TMonitoringManager()
    : IsStarted(false)
    , ActionQueue(New<TActionQueue>("Monitoring"))
{ }

void TMonitoringManager::Register(const TYPath& path, TYsonProducer producer)
{
    TGuard<TSpinLock> guard(SpinLock);
    YCHECK(ProducerMap.insert(std::make_pair(path, producer)).second);
}

void TMonitoringManager::Unregister(const TYPath& path)
{
    TGuard<TSpinLock> guard(SpinLock);
    YCHECK(ProducerMap.erase(path) == 1);
}

IYPathServicePtr TMonitoringManager::GetService()
{
    return New<TYPathService>(this);
}

void TMonitoringManager::Start()
{
    YASSERT(!IsStarted);

    // Create an empty root immediately to prevent GetRoot from returning NULL.
    Root = GetEphemeralNodeFactory()->CreateMap();

    PeriodicInvoker = New<TPeriodicInvoker>(
        ActionQueue->GetInvoker(),
        BIND(&TMonitoringManager::Update, MakeStrong(this)),
        UpdatePeriod);
    PeriodicInvoker->Start();

    IsStarted = true;
}

void TMonitoringManager::Stop()
{
    if (!IsStarted)
        return;

    IsStarted = false;
    PeriodicInvoker->Stop();
    Root.Reset();
}

void TMonitoringManager::Update()
{
    PROFILE_TIMING ("/update_time") {
        INodePtr newRoot = GetEphemeralNodeFactory()->CreateMap();

        FOREACH (const auto& pair, ProducerMap) {
            TYsonString value = ConvertToYsonString(pair.second);
            SyncYPathSet(newRoot, pair.first, value);
        }

        if (IsStarted) {
            Root = newRoot;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMonitoring
} // namespace NYT
