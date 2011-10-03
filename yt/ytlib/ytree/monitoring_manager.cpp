#include "monitoring_manager.h"

#include "ephemeral.h"

#include "actions/action_util.h"

namespace NYT {
namespace NYTree {

const TDuration TMonitoringManager::Period = TDuration::Seconds(3);

TMonitoringManager::TMonitoringManager()
{ 
    PeriodicInvoker = new TPeriodicInvoker(
        FromMethod(&TMonitoringManager::Process, TPtr(this)),
        Period);
}

void TMonitoringManager::Register(TYPath path, TYsonProducer::TPtr producer)
{
    YVERIFY(MonitoringMap.insert(MakePair(Stroka(path), producer)).Second());
}

void TMonitoringManager::Unregister(TYPath path)
{
    YVERIFY(MonitoringMap.erase(Stroka(path)));
}

INode::TPtr TMonitoringManager::GetRoot() const
{
    return Root;
}

void TMonitoringManager::Start()
{
    PeriodicInvoker->Start();
}

void TMonitoringManager::Stop()
{
    PeriodicInvoker->Stop();
}

void TMonitoringManager::Process()
{
    auto newRoot = NEphemeral::TNodeFactory::Get()->CreateMap();
    FOREACH(const auto& pair, MonitoringMap) {
        SetYPath(~newRoot, pair.first, pair.second);
    }
    Root = ~newRoot;
}

} // namespace NYTree
} // namespace NYT
