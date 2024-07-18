#pragma once

#include "public.h"

#include <yt/yt/ytlib/election/public.h>

#include <yt/yt/ytlib/hive/proto/cell_directory.pb.h>

#include <yt/yt/ytlib/hydra/proto/hydra_manager.pb.h>
#include <yt/yt/ytlib/hydra/public.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/client/object_client/public.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/core/logging/public.h>

#include <optional>

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
    DEFINE_BYVAL_RW_PROPERTY(std::optional<TString>, AlienCluster);

public:
    TCellPeerDescriptor();
    TCellPeerDescriptor(const NNodeTrackerClient::TNodeDescriptor& other, bool voting = true);
    TCellPeerDescriptor(const TCellPeerDescriptor& other) = default;
    TCellPeerDescriptor(const NElection::TCellPeerConfigPtr& config, const NNodeTrackerClient::TNetworkPreferenceList& networks);

    TCellPeerDescriptor& operator=(const TCellPeerDescriptor& other) = default;

    NElection::TCellPeerConfigPtr ToConfig(const NNodeTrackerClient::TNetworkPreferenceList& networkName) const;
};

void ToProto(NProto::TCellPeerDescriptor* protoDescriptor, const TCellPeerDescriptor& descriptor);
void FromProto(TCellPeerDescriptor* descriptor, const NProto::TCellPeerDescriptor& protoDescriptor);

// TODO(ifsmirnov): use TExternalizedYsonStruct.
void Serialize(const TCellPeerDescriptor& descriptor, NYson::IYsonConsumer* consumer);
void Deserialize(TCellPeerDescriptor& descriptor, NYTree::INodePtr node);

////////////////////////////////////////////////////////////////////////////////

struct TCellDescriptor final
{
    TCellDescriptor() = default;
    explicit TCellDescriptor(TCellId cellId);

    NElection::TCellConfigPtr ToConfig(const NNodeTrackerClient::TNetworkPreferenceList& networks) const;
    TCellInfo ToInfo() const;

    TCellId CellId;
    int ConfigVersion = -1;
    std::vector<TCellPeerDescriptor> Peers;
};

DEFINE_REFCOUNTED_TYPE(TCellDescriptor)

void ToProto(NProto::TCellDescriptor* protoDescriptor, const TCellDescriptor& descriptor);
void FromProto(TCellDescriptor* descriptor, const NProto::TCellDescriptor& protoDescriptor);

// TODO(ifsmirnov): use TExternalizedYsonStruct.
void Serialize(const TCellDescriptor& descriptor, NYson::IYsonConsumer* consumer);
void Deserialize(TCellDescriptor& descriptor, NYTree::INodePtr node);

////////////////////////////////////////////////////////////////////////////////

//! Caches channels to all known cells.
//! Provides methods to register new cells, unregister existing ones,
//! list and update configurations.
/*!
 *  Thread affinity: thread-safe
 */
struct ICellDirectory
    : public virtual TRefCounted
{
    //! Returns a peer channel of a given kind for a given cell id (|nullptr| if none is known).
    /*!
     *  No user or timeout is configured for the returned channel.
     */
    virtual NRpc::IChannelPtr FindChannelByCellId(
        TCellId cellId,
        NHydra::EPeerKind peerKind = NHydra::EPeerKind::Leader) = 0;

    //! Similar to #FindChannelByCellId but throws an exception if no channel is known.
    virtual NRpc::IChannelPtr GetChannelByCellIdOrThrow(
        TCellId cellId,
        NHydra::EPeerKind peerKind = NHydra::EPeerKind::Leader) = 0;

    //! Similar to #FindChannelByCellId but fails if no channel is known.
    virtual NRpc::IChannelPtr GetChannelByCellId(
        TCellId cellId,
        NHydra::EPeerKind peerKind = NHydra::EPeerKind::Leader) = 0;


    //! Similar to #FindChannelByCellId but relies on cell tag rather than full cell id.
    //! Only works for global cells (see #NObjectClient::IsGlobalCellId).
    virtual NRpc::IChannelPtr FindChannelByCellTag(
        NObjectClient::TCellTag cellTag,
        NHydra::EPeerKind peerKind = NHydra::EPeerKind::Leader) = 0;

    //! Similar to #FindChannelByCellTag but throws an exception if no channel is known.
    virtual NRpc::IChannelPtr GetChannelByCellTagOrThrow(
        NObjectClient::TCellTag cellTag,
        NHydra::EPeerKind peerKind = NHydra::EPeerKind::Leader) = 0;

    //! Similar to #FindChannelByCellTag but fails if no channel is known.
    virtual NRpc::IChannelPtr GetChannelByCellTag(
        NObjectClient::TCellTag cellTag,
        NHydra::EPeerKind peerKind = NHydra::EPeerKind::Leader) = 0;


    //! Returns the descriptor for a given cell id (null if the cell is not known).
    virtual TCellDescriptorPtr FindDescriptorByCellId(TCellId cellId) = 0;

    //! Returns the descriptor for a given cell id (throws if the cell is not known).
    virtual TCellDescriptorPtr GetDescriptorByCellIdOrThrow(TCellId cellId) = 0;

    //! Similar to #FindDescriptor but relies on cell tag rather than full cell id.
    virtual TCellDescriptorPtr FindDescriptorByCellTag(NObjectClient::TCellTag cellTag) = 0;

    //! Returns peer address for a given cell (null if cell is not known or peer has no address).
    virtual std::optional<TString> FindPeerAddress(TCellId cellId, int peerId) = 0;


    //! Returns the list of all registered cells, their versions, and configurations.
    virtual std::vector<TCellInfo> GetRegisteredCells()  = 0;

    //! Returns |true| if the cell was unregistered by calling #UnregisterCell.
    virtual bool IsCellUnregistered(TCellId cellId) = 0;

    //! Returns |true| if the cell is registered.
    virtual bool IsCellRegistered(TCellId cellId) = 0;

    struct TSynchronizationResult
    {
        struct TUnregisterRequest
        {
            TCellId CellId;
        };
        std::vector<TUnregisterRequest> UnregisterRequests;

        struct TReconfigureRequest
        {
            TCellDescriptorPtr NewDescriptor;
            int OldConfigVersion;
        };
        std::vector<TReconfigureRequest> ReconfigureRequests;
    };

    //! Checks cell versions in #knownCells against the actual state;
    //! requests reconfiguration of outdated cells and unregistartion of stale cells.
    virtual TSynchronizationResult Synchronize(const std::vector<TCellInfo>& knownCells)  = 0;


    //! Registers a new cell or updates the configuration of an existing cell
    //! (if new configuration has a higher version).
    //! Returns |true| if the cell was registered (or an update took place).
    virtual bool ReconfigureCell(NElection::TCellConfigPtr config, int configVersion = 0) = 0;

    //! Similar to the above but accepts discovery configuration.
    virtual bool ReconfigureCell(NHydra::TPeerConnectionConfigPtr config, int configVersion = 0) = 0;

    //! Checks versions and updates cell configuration, if needed.
    virtual bool ReconfigureCell(const TCellDescriptor& descriptor) = 0;

    //! Registers a cell with empty description.
    /*!
     *  This call could be used in conjunction with #IsCellUnregistered to make sure that
     *  a given #cellId is no longer valid.
     */
    virtual void RegisterCell(TCellId cellId) = 0;

    //! Unregisters the cell. Returns |true| if the cell was found.
    /*!
     *  The ids of all unregistered cells are kept forever.
     *  Once the cell is unregistered, no further reconfigurations are possible.
     */
    virtual bool UnregisterCell(TCellId cellId) = 0;

    //! Clears the state; i.e. drops all known registered and unregistered cells.
    virtual void Clear() = 0;
};

DEFINE_REFCOUNTED_TYPE(ICellDirectory)

ICellDirectoryPtr CreateCellDirectory(
    TCellDirectoryConfigPtr config,
    NRpc::IChannelFactoryPtr channelFactory,
    TClusterDirectoryPtr clusterDirectory,
    const NNodeTrackerClient::TNetworkPreferenceList& networks,
    NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHiveClient
