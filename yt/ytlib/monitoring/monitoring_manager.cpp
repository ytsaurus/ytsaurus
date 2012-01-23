#include "stdafx.h"
#include "monitoring_manager.h"

#include <ytlib/ytree/ephemeral.h>
#include <ytlib/ytree/yson_writer.h>
#include <ytlib/ytree/tree_visitor.h>
#include <ytlib/ytree/ypath_proxy.h>
#include <ytlib/actions/action_util.h>

#include "stat.h"

namespace NYT {
namespace NMonitoring {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger Logger("Monitoring");
const TDuration TMonitoringManager::Period = TDuration::Seconds(3);

////////////////////////////////////////////////////////////////////////////////

TMonitoringManager::TMonitoringManager()
    : IsStarted(false)
{
    PeriodicInvoker = New<TPeriodicInvoker>(
        FromMethod(&TMonitoringManager::Update, TPtr(this)),
        Period);
}

void TMonitoringManager::Register(const TYPath& path, TYsonProducer::TPtr producer)
{
    TGuard<TSpinLock> guard(SpinLock);
    YVERIFY(MonitoringMap.insert(MakePair(path, producer)).Second());
}

void TMonitoringManager::Unregister(const TYPath& path)
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
        TIMEIT("stateman.updatetime", "tv",

        auto newRoot = GetEphemeralNodeFactory()->CreateMap();

        FOREACH(const auto& pair, MonitoringMap) {
            TStringStream output;
            TYsonWriter writer(&output, TYsonWriter::EFormat::Binary);
            pair.second->Do(&writer);

            SyncYPathSet(~newRoot, pair.first, output.Str());
        }

        if (IsStarted) {
            Root = newRoot;
        }

        )

    } catch (const std::exception& ex) {
        LOG_FATAL("Error collecting monitoring data\n%s",
            ex.what());
    }
}

void TMonitoringManager::Visit(IYsonConsumer* consumer)
{
    TIMEIT("stateman.visittime", "tv",

    TTreeVisitor visitor(consumer);
    visitor.Visit(~GetRoot());

    )
}

TYsonProducer::TPtr TMonitoringManager::GetProducer()
{
    YASSERT(IsStarted);
    YASSERT(Root);

    return FromMethod(&TMonitoringManager::Visit, TPtr(this));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMonitoring
} // namespace NYT
