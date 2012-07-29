#pragma once

#include <ytlib/ytree/public.h>
#include <ytlib/ytree/yson_producer.h>
#include <ytlib/actions/action_queue.h>
#include <ytlib/misc/periodic_invoker.h>

namespace NYT {
namespace NMonitoring {

////////////////////////////////////////////////////////////////////////////////

//! Provides monitoring info for registered systems in YSON format
/*!
 * \note Periodically updates info for all registered systems
 */
class TMonitoringManager
    : public TRefCounted
{
public:
    typedef TIntrusivePtr<TMonitoringManager> TPtr;

    //! Empty constructor.
    TMonitoringManager();

    //! Registers system for specified path.
    /*!
     * \param path      YPath for specified monitoring info.
     * \param producer  Monitoring info producer for the system.
     */
    void Register(const NYTree::TYPath& path, NYTree::TYsonProducer producer);

    //! Unregisters system for specified path.
    /*!
     * \param path  YPath for specified monitoring info.
     */
    void Unregister(const NYTree::TYPath& path);

    //! Provides a root node containing info for all registered systems.
    /*!
     * \note Every update, the previous root expires and a new root is generated.
     */
    NYTree::INodePtr GetRoot() const;

    //! Starts periodic updates.
    void Start();

    //! Stops periodic updates.
    void Stop();

    //! Provides YSON producer for all monitoring infos.
    /*!
     * \note Producer is sustained between updates.
     */
    NYTree::TYsonProducer GetProducer();

private:
    typedef yhash<Stroka, NYTree::TYsonProducer> TProducerMap;

    static const TDuration Period; // TODO: make yson serializable

    bool IsStarted;
    TActionQueuePtr ActionQueue;
    TPeriodicInvokerPtr PeriodicInvoker;

    //! Protects #MonitoringMap.
    TSpinLock SpinLock;
    TProducerMap MonitoringMap;

    NYTree::INodePtr Root;

    void Update();
    void Visit(NYTree::IYsonConsumer* consumer);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NMonitoring
} // namespace NYT
