#include "stdafx.h"
#include "monitoring_manager.h"

#include "../ytree/ephemeral.h"
#include "../ytree/tree_visitor.h"
#include "../ytree/ypath_client.h"
#include "../ytree/ypath_service.h"
#include "../logging/log.h"
#include "../actions/action_util.h"
#include "../misc/assert.h"

namespace NYT {
namespace NMonitoring {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

NLog::TLogger Logger("Monitoring");
const TDuration TMonitoringManager::Period = TDuration::Seconds(3);

////////////////////////////////////////////////////////////////////////////////

TMonitoringManager::TMonitoringManager()
    : IsStarted(false)
{ 
    PeriodicInvoker = new TPeriodicInvoker(
        FromMethod(&TMonitoringManager::Update, TPtr(this)),
        Period);
}

void TMonitoringManager::Register(TYPath path, TYsonProducer::TPtr producer)
{
    TGuard<TSpinLock> guard(SpinLock);
    YVERIFY(MonitoringMap.insert(MakePair(path, producer)).Second());
}

void TMonitoringManager::Unregister(TYPath path)
{
    TGuard<TSpinLock> guard(SpinLock);
    YVERIFY(MonitoringMap.erase(Stroka(path)));
}

INode::TPtr TMonitoringManager::GetRoot() const
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
    try {
        auto newRoot = GetEphemeralNodeFactory()->CreateMap();
        auto newRootService = IYPathService::FromNode(~newRoot);
        FOREACH(const auto& pair, MonitoringMap) {
            //SetYPath(newRootService, pair.first, pair.second);
        }

        if (IsStarted) {
            Root = newRoot;
        }
    } catch (...) {
        LOG_FATAL("Error collecting monitoring data: %s",
            ~CurrentExceptionMessage());
    }
}

void TMonitoringManager::Visit(IYsonConsumer* consumer)
{
    TTreeVisitor visitor(consumer);
    visitor.Visit(GetRoot());
}

TYsonProducer::TPtr TMonitoringManager::GetProducer()
{
    YASSERT(IsStarted);
    YASSERT(~Root != NULL);

    return FromMethod(&TMonitoringManager::Visit, TPtr(this));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMonitoring
} // namespace NYT
