#pragma once

#include "public.h"

#include <yt/yt/core/yson/consumer.h>
#include <yt/yt/core/yson/producer.h>

#include <yt/yt/core/ypath/public.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NMonitoring {

////////////////////////////////////////////////////////////////////////////////

//! Exposes a tree assembled from results returned by a set of
//! registered NYson::TYsonProducer-s.
/*!
 *  \note
 *  The results are cached and periodically updated.
 */
struct IMonitoringManager
    : public TRefCounted
{
    //! Registers a new #producer for a given #path.
    virtual void Register(const NYPath::TYPath& path, NYson::TYsonProducer producer) = 0;

    //! Unregisters an existing producer for the specified #path.
    virtual void Unregister(const NYPath::TYPath& path) = 0;

    //! Returns the service representing the whole tree.
    /*!
     * \note The service is thread-safe.
     */
    virtual NYTree::IYPathServicePtr GetService() = 0;

    //! Starts periodic updates.
    virtual void Start() = 0;

    //! Stops periodic updates.
    virtual void Stop() = 0;
};

DEFINE_REFCOUNTED_TYPE(IMonitoringManager)

////////////////////////////////////////////////////////////////////////////////

IMonitoringManagerPtr CreateMonitoringManager();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NMonitoring
