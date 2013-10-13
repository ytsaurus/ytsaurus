#pragma once

#include "public.h"

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

//! A bunch of snapshot stores. Each store is keyed by TCellGuid.
/*!
 *  \note
 *  Implementations must be thread-safe.
 */
struct ISnapshotCatalog
    : public TRefCounted
{
    //! Enumerates all stores in the catalog.
    virtual std::vector<ISnapshotStorePtr> GetStores() = 0;

    //! Synchronously creates a new store.
    virtual ISnapshotStorePtr CreateStore(const TCellGuid& cellGuid) = 0;

    //! Synchronously removes an existing store.
    virtual void RemoveStore(const TCellGuid& cellGuid) = 0 ;

    //! Finds a store by cell id. Returns |nullptr| if no store with a given
    //! id is registered.
    virtual ISnapshotStorePtr FindStore(const TCellGuid& cellGuid) = 0;


    // Extension methods

    //! Finds a store by cell id. Fails if no store with a given
    //! id is registered.
    ISnapshotStorePtr GetStore(const TCellGuid& cellGuid);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
