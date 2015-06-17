#pragma once

#include "public.h"

#include <core/misc/nullable.h>

#include <core/rpc/public.h>

#include <ytlib/hydra/public.h>
#include <ytlib/hydra/hydra_manager.pb.h>

#include <ytlib/election/public.h>

#include <yt/ytlib/hive/cell_directory.pb.h>

#include <yt/ytlib/node_tracker_client/node_directory.h>

namespace NYT {
namespace NHive {

////////////////////////////////////////////////////////////////////////////////

struct TCellInfo
{
    TCellId CellId;
    int ConfigVersion = -1;
};

void ToProto(NProto::TCellInfo* protoInfo, const TCellInfo& info);
void FromProto(TCellInfo* info, const NProto::TCellInfo& protoInfo);

////////////////////////////////////////////////////////////////////////////////

struct TCellDescriptor
{
    TCellId CellId;
    int ConfigVersion = -1;
    std::vector<TNullable<NNodeTrackerClient::TNodeDescriptor>> Peers;

    NElection::TCellConfigPtr ToConfig(const Stroka& networkName) const;
    TCellInfo ToInfo() const;
};

void ToProto(NProto::TCellDescriptor* protoDescriptor, const TCellDescriptor& descriptor);
void FromProto(TCellDescriptor* descriptor, const NProto::TCellDescriptor& protoDescriptor);

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
        NRpc::IChannelFactoryPtr channelFactory,
        const Stroka& networkName);
    ~TCellDirectory();


    //! Returns a peer channel of a given kind for a given cell id (|nullptr| if none is known).
    NRpc::IChannelPtr FindChannel(
        const TCellId& cellId,
        NHydra::EPeerKind peerKind = NHydra::EPeerKind::Leader);

    //! Similar to #FindChannel but throws an exception if no channel is known.
    NRpc::IChannelPtr GetChannelOrThrow(
        const TCellId& cellId,
        NHydra::EPeerKind peerKind = NHydra::EPeerKind::Leader);


    //! Returns the list of peer addresses for a given cell id (|Null| if the cell is not known).
    TNullable<std::vector<Stroka>> FindAddresses(const TCellId& cellId);

    //! Similar to #FindAddresses but throws an exception if the cell is not known.
    std::vector<Stroka> GetAddressesOrThrow(const TCellId& cellId);


    //! Returns the list of all registered cells, their versions, and configurations.
    std::vector<TCellInfo> GetRegisteredCells();


    //! Registers a new cell or updates the configuration of an existing cell
    //! (if new configuration has a higher version).
    //! Returns |true| if the cell was registered (or an update took place).
    bool ReconfigureCell(NElection::TCellConfigPtr config, int configVersion = 0);

    //! Similar to the above but accepts discovery configuration.
    bool ReconfigureCell(NHydra::TPeerConnectionConfigPtr config, int configVersion = 0);

    //! Checks versions and updates cell configuration, if needed.
    bool ReconfigureCell(const TCellDescriptor& descriptor);

    //! Unregisters the cell. Returns |true| if the cell was found.
    bool UnregisterCell(const TCellId& cellId);

    //! Drops all known cells.
    void Clear();


private:
    class TImpl;
    const std::unique_ptr<TImpl> Impl_;

};

DEFINE_REFCOUNTED_TYPE(TCellDirectory)

////////////////////////////////////////////////////////////////////////////////

} // namespace NHive
} // namespace NYT
