#pragma once

#include "public.h"

#include <yt/ytlib/election/public.h>

#include <yt/ytlib/hive/proto/cell_directory.pb.h>

#include <yt/ytlib/hydra/hydra_manager.pb.h>
#include <yt/ytlib/hydra/public.h>

#include <yt/client/node_tracker_client/node_directory.h>

#include <yt/core/misc/optional.h>

#include <yt/core/actions/future.h>

#include <yt/core/rpc/public.h>

#include <yt/core/logging/public.h>

namespace NYT::NHiveClient {

////////////////////////////////////////////////////////////////////////////////

struct TCellInfo
{
    TCellId CellId;
    int ConfigVersion = -1;
};

void ToProto(NProto::TCellInfo* protoInfo, const TCellInfo& info);
void FromProto(TCellInfo* info, const NProto::TCellInfo& protoInfo);

////////////////////////////////////////////////////////////////////////////////

class TCellPeerDescriptor
    : public NNodeTrackerClient::TNodeDescriptor
{
public:
    DEFINE_BYVAL_RW_PROPERTY(bool, Voting);

public:
    TCellPeerDescriptor();
    TCellPeerDescriptor(const NNodeTrackerClient::TNodeDescriptor& other, bool voting = true);
    TCellPeerDescriptor(const TCellPeerDescriptor& other) = default;
    TCellPeerDescriptor(const NElection::TCellPeerConfig& config, const NNodeTrackerClient::TNetworkPreferenceList& networks);

    NElection::TCellPeerConfig ToConfig(const NNodeTrackerClient::TNetworkPreferenceList& networkName) const;

};

void ToProto(NProto::TCellPeerDescriptor* protoDescriptor, const TCellPeerDescriptor& descriptor);
void FromProto(TCellPeerDescriptor* descriptor, const NProto::TCellPeerDescriptor& protoDescriptor);

////////////////////////////////////////////////////////////////////////////////

struct TCellDescriptor
{
    TCellDescriptor() = default;
    explicit TCellDescriptor(TCellId cellId);

    NElection::TCellConfigPtr ToConfig(const NNodeTrackerClient::TNetworkPreferenceList& networks) const;
    TCellInfo ToInfo() const;

    TCellId CellId;
    int ConfigVersion = -1;
    std::vector<TCellPeerDescriptor> Peers;
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
        const NNodeTrackerClient::TNetworkPreferenceList& networks,
        const NLogging::TLogger& logger);
    ~TCellDirectory();

    //! Returns a peer channel of a given kind for a given cell id (|nullptr| if none is known).
    /*!
     *  No user or timeout is configured for the returned channel.
     */
    NRpc::IChannelPtr FindChannel(
        TCellId cellId,
        NHydra::EPeerKind peerKind = NHydra::EPeerKind::Leader);

    //! Similar to #FindChannel but throws an exception if no channel is known.
    NRpc::IChannelPtr GetChannelOrThrow(
        TCellId cellId,
        NHydra::EPeerKind peerKind = NHydra::EPeerKind::Leader);

    //! Similar to #FindChannel but fails if no channel is known.
    NRpc::IChannelPtr GetChannel(
        TCellId cellId,
        NHydra::EPeerKind peerKind = NHydra::EPeerKind::Leader);


    //! Returns the descriptor for a given cell id (null if the cell is not known).
    std::optional<TCellDescriptor> FindDescriptor(TCellId cellId);

    //! Returns the descriptor for a given cell id (throws if the cell is not known).
    TCellDescriptor GetDescriptorOrThrow(TCellId cellId);


    //! Returns the list of all registered cells, their versions, and configurations.
    std::vector<TCellInfo> GetRegisteredCells();

    //! Returns |true| if the cell was unregistered by calling #UnregisterCell.
    bool IsCellUnregistered(TCellId cellId);


    //! Registers a new cell or updates the configuration of an existing cell
    //! (if new configuration has a higher version).
    //! Returns |true| if the cell was registered (or an update took place).
    bool ReconfigureCell(NElection::TCellConfigPtr config, int configVersion = 0);

    //! Similar to the above but accepts discovery configuration.
    bool ReconfigureCell(NHydra::TPeerConnectionConfigPtr config, int configVersion = 0);

    //! Checks versions and updates cell configuration, if needed.
    bool ReconfigureCell(const TCellDescriptor& descriptor);

    //! Registers a cell with empty description.
    /*!
     *  This call could be used in conjuction with #IsCellUnregistered to make sure that
     *  a given #cellId is no longer valid.
     */
    void RegisterCell(TCellId cellId);

    //! Unregisters the cell. Returns |true| if the cell was found.
    /*!
     *  The ids of all unregistered cells are kept forever.
     *  Once the cell is unregistered, no further reconfigurations are possible.
     */
    bool UnregisterCell(TCellId cellId);

    //! Clears the state; i.e. drops all known registered and unregistered cells.
    void Clear();


private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;

};

DEFINE_REFCOUNTED_TYPE(TCellDirectory)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHiveClient
