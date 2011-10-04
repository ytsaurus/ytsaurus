#include "monitoring_manager.h"

#include "../ytree/ephemeral.h"
#include "../actions/action_util.h"
#include "../misc/assert.h"

namespace NYT {
namespace NMonitoring {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

const TDuration TMonitoringManager::Period = TDuration::Seconds(3);

TMonitoringManager::TMonitoringManager()
{ 
    PeriodicInvoker = new TPeriodicInvoker(
        FromMethod(&TMonitoringManager::Update, TPtr(this)),
        Period);
}

void TMonitoringManager::Register(TYPath path, TYsonProducer::TPtr producer)
{
    YVERIFY(MonitoringMap.insert(MakePair(path, producer)).Second());
}

void TMonitoringManager::Unregister(TYPath path)
{
    YVERIFY(MonitoringMap.erase(Stroka(path)));
}

INode::TConstPtr TMonitoringManager::GetRoot() const
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

void TMonitoringManager::Update()
{
    auto newRoot = NEphemeral::TNodeFactory::Get()->CreateMap();
    FOREACH(const auto& pair, MonitoringMap) {
        SetYPath(~newRoot, pair.first, pair.second);
    }
    Root = ~newRoot;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMonitoring
} // namespace NYT
