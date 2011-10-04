#pragma once

#include "../ytree/ytree.h"
#include "../misc/periodic_invoker.h"

namespace NYT {
namespace NMonitoring {

////////////////////////////////////////////////////////////////////////////////

class TMonitoringManager
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TMonitoringManager> TPtr;

    TMonitoringManager();

    void Register(NYTree::TYPath path, NYTree::TYsonProducer::TPtr producer);
    void Unregister(NYTree::TYPath path);
    NYTree::INode::TConstPtr GetRoot() const;

    void Start();
    void Stop();

    NYTree::TYsonProducer::TPtr GetProducer();

private:
    typedef yhash<Stroka, NYTree::TYsonProducer::TPtr> TProducerMap;

    static const TDuration Period; // TODO: make configurable

    bool IsStarted;
    TPeriodicInvoker::TPtr PeriodicInvoker;

    //! Protects #MonitoringMap.
    TSpinLock SpinLock;
    TProducerMap MonitoringMap;

    NYTree::INode::TConstPtr Root;

    void Update();
    void Visit(NYTree::IYsonConsumer* consumer);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NMonitoring
} // namespace NYT
