#include "stdafx.h"
#include "monitoring_manager.h"

#include <ytlib/ytree/ephemeral.h>
#include <ytlib/ytree/yson_writer.h>
#include <ytlib/ytree/tree_visitor.h>
#include <ytlib/ytree/ypath_proxy.h>
#include <ytlib/actions/bind.h>
#include <ytlib/profiling/profiler.h>

namespace NYT {
namespace NMonitoring {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger Logger("Monitoring");
static NProfiling::TProfiler Profiler("/monitoring");
const TDuration TMonitoringManager::Period = TDuration::Seconds(3);

////////////////////////////////////////////////////////////////////////////////

TMonitoringManager::TMonitoringManager()
    : IsStarted(false)
{
    PeriodicInvoker = New<TPeriodicInvoker>(
        BIND(&TMonitoringManager::Update, MakeStrong(this)),
        Period);
}

void TMonitoringManager::Register(const TYPath& path, TYsonProducer producer)
{
    TGuard<TSpinLock> guard(SpinLock);
    YVERIFY(MonitoringMap.insert(MakePair(path, MoveRV(producer))).second);
}

void TMonitoringManager::Unregister(const TYPath& path)
{
    TGuard<TSpinLock> guard(SpinLock);
    YVERIFY(MonitoringMap.erase(Stroka(path)));
}

INodePtr TMonitoringManager::GetRoot() const
{
    return Root;
}

void TMonitoringManager::Start()
{
    YASSERT(!IsStarted);

    IsStarted = true;
    // Update the root right away to prevent GetRoot from returning NULL.
    Update();
    PeriodicInvoker->Start();
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
    PROFILE_TIMING("update_time") {
        try {
            auto newRoot = GetEphemeralNodeFactory()->CreateMap();

            FOREACH(const auto& pair, MonitoringMap) {
                TStringStream output;
                TYsonWriter writer(&output, EYsonFormat::Binary);
                pair.second.Run(&writer);

                SyncYPathSet(~newRoot, pair.first, output.Str());
            }

            if (IsStarted) {
                Root = newRoot;
            }
        } catch (const std::exception& ex) {
            LOG_FATAL("Error collecting monitoring data\n%s",
                ex.what());
        }
    }
}

void TMonitoringManager::Visit(IYsonConsumer* consumer)
{
    PROFILE_TIMING("visit_time") {
        VisitTree(~GetRoot(), consumer);
    }
}

TYsonProducer TMonitoringManager::GetProducer()
{
    YASSERT(IsStarted);
    YASSERT(Root);

    return BIND(&TMonitoringManager::Visit, MakeStrong(this));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMonitoring
} // namespace NYT
