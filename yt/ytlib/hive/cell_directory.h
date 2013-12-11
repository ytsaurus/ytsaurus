#pragma once

#include "public.h"

#include <core/rpc/public.h>

#include <ytlib/hydra/public.h>
#include <ytlib/hydra/hydra_manager.pb.h>

#include <ytlib/election/public.h>

namespace NYT {
namespace NHive {

////////////////////////////////////////////////////////////////////////////////

//! Caches channels to all known cells.
//! Provides methods to register new cells, unregister existing ones,
//! list and update configurations.
/*!
 *  Thread affinity: thread-safe
 */
class TCellDirectory
    : public TRefCounted
{
public:
    TCellDirectory(
        TCellDirectoryConfigPtr config,
        NRpc::IChannelFactoryPtr channelFactory);
    ~TCellDirectory();

    typedef NHydra::NProto::TCellConfig TCellConfig;

    //! Returns a leader channel for a given cell GUID (|nullptr| if none is known).
    NRpc::IChannelPtr FindChannel(const TCellGuid& cellGuid);

    //! Similar to #FindChannel but throws an exception if no channel is known.
    NRpc::IChannelPtr GetChannelOrThrow(const TCellGuid& cellGuid);


    //! Registers a new cell or updates the configuration of an existing cell
    //! (if new configuration has a higher version).
    //! Returns |true| if the cell was registered (or an update took place).
    bool RegisterCell(const TCellGuid& cellGuid, const TCellConfig& config);

    //! Similar to the above but accepts discovery configuration.
    bool RegisterCell(NHydra::TPeerDiscoveryConfigPtr config);

    //! Similar to the above but accepts cell configuration.
    bool RegisterCell(NElection::TCellConfigPtr config);

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
