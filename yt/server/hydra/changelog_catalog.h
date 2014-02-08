#pragma once

#include "public.h"

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

//! A bunch of changelog stores. Each store is keyed by TCellGuid.
/*!
 *  \note
 *  Implementations must be thread-safe.
 */
struct IChangelogCatalog
    : public TRefCounted
{
    //! Enumerates all stores in the catalog.
    virtual std::vector<IChangelogStorePtr> GetStores() = 0;

    //! Synchronously creates a new store.
    virtual IChangelogStorePtr CreateStore(const TCellGuid& cellGuid) = 0;

    //! Synchronously removes an existing store.
    virtual void RemoveStore(const TCellGuid& cellGuid) = 0 ;

    //! Finds a store by cell id. Returns |nullptr| if no store with a given
    //! id is registered.
    virtual IChangelogStorePtr FindStore(const TCellGuid& cellGuid) = 0;


    // Extension methods

    //! Finds a store by cell id. Fails if no store with a given
    //! id is registered.
    IChangelogStorePtr GetStore(const TCellGuid& cellGuid);

};

DEFINE_REFCOUNTED_TYPE(IChangelogCatalog)

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
