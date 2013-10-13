#pragma once

#include "public.h"

#include <core/rpc/public.h>

#include <ytlib/hydra/hydra_manager.pb.h>

namespace NYT {
namespace NHive {

////////////////////////////////////////////////////////////////////////////////

//! Caches channels to all known cells.
//! Provides methods to register new cells, unregister existing ones,
//! list and update configurations.
/*!
 *  Thread affinity: thread-safe
 */
class TCellRegistry
    : public TRefCounted
{
public:
    TCellRegistry();

    typedef NHydra::NProto::TCellConfig TCellConfig;

    //! Returns a leader channel for a given cell GUID (|nullptr| if none is known).
    NRpc::IChannelPtr GetChannel(const TCellGuid& cellGuid);


    //! Registers a new cell or updates the configuration of an existing cell
    //! (if new configuration has a higher version).
    //! Returns |true| if the cell was registered (or an update took place).
    bool RegisterCell(const TCellGuid& cellGuid, const TCellConfig& config);

    //! Unregisters the cell. Returns |true| if the cell was found.
    bool UnregisterCell(const TCellGuid& cellGuid);

    //! Drops all known cells.
    void Clear();


    //! Returns the list of all registered cells and their configurations.
    std::vector<std::pair<TCellGuid, TCellConfig>> GetRegisteredCells();

private:
    class TImpl;
    std::unique_ptr<TImpl> Impl;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NHive
} // namespace NYT
