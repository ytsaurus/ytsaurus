#include "monitoring_manager.h"

#include "../logging/log.h"
#include "../ytree/ephemeral.h"
#include "../actions/action_util.h"
#include "../misc/assert.h"
#include "../ytree/tree_visitor.h"

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
    Root.Drop();
}

void TMonitoringManager::Update()
{
    try {
        INode::TPtr newRoot = ~NEphemeral::TNodeFactory::Get()->CreateMap();
        auto newRootService = AsYPath(newRoot);
        FOREACH(const auto& pair, MonitoringMap) {
            SetYPath(newRootService, pair.first, pair.second);
        }

        if (IsStarted) {
            Root = ~newRoot;
        }
    } catch (...) {
        LOG_ERROR("Error collecting monitoring data: %s",
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
