#pragma once

#include "public.h"

#include <yt/server/lib/hive/public.h>

#include <yt/ytlib/election/public.h>

#include <yt/ytlib/object_client/public.h>

#include <yt/ytlib/hydra/public.h>

#include <yt/core/misc/ref.h>

#include <yt/core/actions/signal.h>

#include <yt/core/rpc/public.h>

#include <variant>

namespace NYT::NCellMaster {

////////////////////////////////////////////////////////////////////////////////

//! Provides a convenient helper for constructing cross-cell messages of various
//! kinds. Note that ctors are intentionally implicit.
struct TCrossCellMessage
{
    template <class TRequest, class TResponse>
    TCrossCellMessage(TIntrusivePtr<NYTree::TTypedYPathRequest<TRequest, TResponse>> request)
        : Payload(TClientMessage{std::move(request)})
    { }

    TCrossCellMessage(const ::google::protobuf::MessageLite& message)
        : Payload(TProtoMessage{&message})
    { }

    TCrossCellMessage(NObjectClient::TObjectId objectId, NTransactionClient::TTransactionId transactionId, NRpc::IServiceContextPtr context)
        : Payload(TServiceMessage{objectId, transactionId, std::move(context)})
    { }

    struct TClientMessage
    {
        NRpc::IClientRequestPtr Request;
    };

    struct TProtoMessage
    {
        const ::google::protobuf::MessageLite* Message;
    };

    struct TServiceMessage
    {
        NObjectClient::TObjectId ObjectId;
        NTransactionClient::TTransactionId TransactionId;
        NRpc::IServiceContextPtr Context;
    };

    std::variant<
        TClientMessage,
        TProtoMessage,
        TServiceMessage
    > Payload;
};

////////////////////////////////////////////////////////////////////////////////

class TMulticellManager
    : public TRefCounted
{
public:
    TMulticellManager(
        TMulticellManagerConfigPtr config,
        TBootstrap* bootstrap);

    ~TMulticellManager();

    void Initialize();

    bool IsPrimaryMaster();
    bool IsSecondaryMaster();
    bool IsMulticell();

    NObjectClient::TCellId GetCellId();
    NObjectClient::TCellId GetCellId(NObjectClient::TCellTag cellTag);
    NObjectClient::TCellTag GetCellTag();

    NObjectClient::TCellId GetPrimaryCellId();
    NObjectClient::TCellTag GetPrimaryCellTag();

    const NObjectClient::TCellTagList& GetSecondaryCellTags();

    void PostToMaster(
        const TCrossCellMessage& message,
        NObjectClient::TCellTag cellTag,
        bool reliable = true);
    void PostToMasters(
        const TCrossCellMessage& message,
        const NObjectClient::TCellTagList& cellTags,
        bool reliable = true);
    void PostToSecondaryMasters(
        const TCrossCellMessage& message,
        bool reliable = true);

    //! For primary masters, always returns |true|.
    //! For secondary masters, returns |true| if the local secondary cell is registered at the primary cell.
    bool IsLocalMasterCellRegistered();

    //! Returns |true| if there is a registered master cell with a given cell tag.
    bool IsRegisteredMasterCell(NObjectClient::TCellTag cellTag);

    //! Returns the set of roles the cell is configured for.
    EMasterCellRoles GetMasterCellRoles(NObjectClient::TCellTag cellTag);

    //! Returns the list of cell tags for all registered master cells (other than the local one),
    //! in a stable order.
    /*!`
     *  For secondary masters, the primary master is always the first element.
     */
    const NObjectClient::TCellTagList& GetRegisteredMasterCellTags();

    //! Returns a stable index of a given (registered) master cell (other than the local one).
    int GetRegisteredMasterCellIndex(NObjectClient::TCellTag cellTag);

    //! Picks a random (but deterministically chosen) secondary master cell to
    //! host an external chunk-owning node.
    /*!
     *  Only cells with EMasterCellRoles::ChunkHost are considered.
     *  Cells with less-than-average number of chunks are typically preferred.
     *  The exact amount of skewness is controlled by #bias argument, 0 indicating no preference,
     *  and 1.0 indicating that cells with low number of chunks are picked twice as more often as those
     *  with the high number of chunks.
     *
     *  If no secondary cells are registered then #InvalidCellTag is returned.
     */
    NObjectClient::TCellTag PickSecondaryChunkHostCell(double bias);

    //! Computes the total cluster statistics by summing counters for all cells (including primary).
    NProto::TCellStatistics ComputeClusterStatistics();

    //! Returns the channel to be used for communicating with another master.
    //! This channel has a properly configured timeout.
    //! Throws on error.
    NRpc::IChannelPtr GetMasterChannelOrThrow(NObjectClient::TCellTag cellTag, NHydra::EPeerKind peerKind);

    //! Same as #GetMasterChannelOrThrow but returns |nullptr| if no channel is currently known.
    NRpc::IChannelPtr FindMasterChannel(NObjectClient::TCellTag cellTag, NHydra::EPeerKind peerKind);

    //! Returns the mailbox used for communicating with the primary master cell.
    //! May return null if the cell is not connected yet.
    NHiveServer::TMailbox* FindPrimaryMasterMailbox();


    //! Synchronizes with the upstream.
    /*!
     *  Used to prevent stale reads by ensuring that the automaton has seen enough mutations
     *  from all "upstream" services.
     *
     *  Synchronization requests are automatically batched together.
     *
     *  Internally, this combines two means of synchronization:
     *  1) follower-with-leader synchronization
     *  2) primary-to-secondary cell synchronization
     *
     *  Synchronizer (1) has no effect at leader.
     *  Synchronizer (2) has no effect at primary cell.
     *
     *  \note Thread affinity: any
     */
    TFuture<void> SyncWithUpstream();

    DECLARE_SIGNAL(void(NObjectClient::TCellTag), ValidateSecondaryMasterRegistration);
    DECLARE_SIGNAL(void(NObjectClient::TCellTag), ReplicateKeysToSecondaryMaster);
    DECLARE_SIGNAL(void(NObjectClient::TCellTag), ReplicateValuesToSecondaryMaster);

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TMulticellManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster
