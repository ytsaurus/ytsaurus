#pragma once

#include "common.h"
#include "ytree_fwd.h"
#include "ytree.h"

#include "../misc/periodic_invoker.h"

namespace NYT {
namespace NYTree {

class TMonitoringManager
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TMonitoringManager> TPtr;

    TMonitoringManager();

    void Register(TYPath path, TYsonProducer::TPtr producer);
    void Unregister(TYPath path);
    INode::TPtr GetRoot() const;

    void Start();
    void Stop();

private:
    typedef yhash<Stroka, TYsonProducer::TPtr> TProducerMap;

    static const TDuration Period; // TODO: make configurable

    TProducerMap MonitoringMap;
    INode::TPtr Root;

    TPeriodicInvoker::TPtr PeriodicInvoker;

    void Process();
};

} // namespace NYTree
} // namespace NYT
