#pragma once

#include "config.h"

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

//! An interaface for keeping and updating a list of participants that are alive
//! in the certain group with their attributes.
struct IDiscovery
    : public virtual TRefCounted
{
    //! Make this participant exposed to the group.
    //! It doesn't update the stored list of participants,
    //! but will add {name, attributes} in every result of List().
    virtual TFuture<void> Enter(TString name, NYTree::IAttributeDictionaryPtr attributes) = 0;
    //! Make this participant unexposed to the group.
    //! It doesn't update the stored list of participants.
    virtual TFuture<void> Leave() = 0;

    //! Return the list of participants stored in data structure.
    virtual THashMap<TString, NYTree::IAttributeDictionaryPtr> List(bool includeBanned = false) const = 0;
    //! Temporary add |name| to the ban list (timeout is specified in discovery config).
    //! Instances from the ban list are excluded from the list of available participants.
    virtual void Ban(const TString& name) = 0;
    virtual void Ban(const std::vector<TString>& names) = 0;
    //! Remove |name| from the ban list.
    virtual void Unban(const TString& name) = 0;
    virtual void Unban(const::std::vector<TString>& names) = 0;

    //! Force update the list of participants if stored data is older than |maxDivergency|.
    //! Returns a future that becomes set when data is up to date.
    virtual TFuture<void> UpdateList(TDuration maxDivergency = TDuration::Zero()) = 0;

    //! Start updating the list of available participants.
    //! Returns a future that becomes set after first update.
    virtual TFuture<void> StartPolling() = 0;
    //! Stop updating the list of available participants.
    //! Returns a future that becomes set after stopping PeriodicExecutor.
    virtual TFuture<void> StopPolling() = 0;

    //! Returns a version of the discovery.
    virtual int Version() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IDiscovery)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
