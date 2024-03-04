#pragma once

#include "public.h"

#include <yt/yt/server/lib/hive/public.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/election/public.h>

#include <yt/yt/ytlib/object_client/public.h>

#include <yt/yt/ytlib/hydra/public.h>

#include <yt/yt/core/actions/signal.h>

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/core/ytree/public.h>

#include <library/cpp/yt/memory/ref.h>

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

struct IMulticellManager
    : public virtual TRefCounted
{
    virtual void Initialize() = 0;

    virtual bool IsPrimaryMaster() const = 0;
    virtual bool IsSecondaryMaster() const = 0;
    virtual bool IsMulticell() const = 0;

    virtual NObjectClient::TCellId GetCellId() const = 0;
    virtual NObjectClient::TCellId GetCellId(NObjectClient::TCellTag cellTag) const = 0;
    virtual NObjectClient::TCellTag GetCellTag() const = 0;

    virtual NObjectClient::TCellId GetPrimaryCellId() const = 0;
    virtual NObjectClient::TCellTag GetPrimaryCellTag() const = 0;

    virtual const NObjectClient::TCellTagList& GetSecondaryCellTags() const = 0;
    virtual const NApi::NNative::TConnectionStaticConfigPtr& GetMasterCellConnectionConfigs() const = 0;

    virtual int GetCellCount() const = 0;
    virtual int GetSecondaryCellCount() const = 0;

    virtual void PostToMaster(
        const TCrossCellMessage& message,
        NObjectClient::TCellTag cellTag,
        bool reliable = true) = 0;
    virtual void PostToMasters(
        const TCrossCellMessage& message,
        const NObjectClient::TCellTagList& cellTags,
        bool reliable = true) = 0;
    virtual void PostToPrimaryMaster(
        const TCrossCellMessage& message,
        bool reliable = true) = 0;
    virtual void PostToSecondaryMasters(
        const TCrossCellMessage& message,
        bool reliable = true) = 0;

    //! For primary masters, always returns |true|.
    //! For secondary masters, returns |true| if the local secondary cell is registered at the primary cell.
    virtual bool IsLocalMasterCellRegistered() const = 0;

    //! Returns |true| if there is a registered master cell with a given cell tag.
    virtual bool IsRegisteredMasterCell(NObjectClient::TCellTag cellTag) const = 0;

    //! Returns the set of roles the cell is configured for.
    /*!
     *  \note Thread affinity: any
     */
    virtual EMasterCellRoles GetMasterCellRoles(NObjectClient::TCellTag cellTag) const = 0;

    //! Returns the set of cells configured for a given role.
    /*!
     *  \note Thread affinity: any
     */
    virtual NObjectClient::TCellTagList GetRoleMasterCells(EMasterCellRole cellRole) const = 0;

    //! Returns the number of cells configured for a given role.
    /*!
     *  \note Thread affinity: any
     */
    virtual int GetRoleMasterCellCount(EMasterCellRole cellRole) const = 0;

    //! Returns master cell name (cell tag is default cell name).
    //! Decimal representation of cell tag is the default.
    /*!
     *  \note Thread affinity: any
     */
    virtual TString GetMasterCellName(NObjectClient::TCellTag cellTag) const = 0;

    //! Returns master cell tag or std::nullopt, if there is no cell with given name.
    /*!
     *  \note Thread affinity: any
     */
    virtual std::optional<NObjectClient::TCellTag> FindMasterCellTagByName(const TString& cellName) const = 0;

    //! Returns the list of cell tags for all registered master cells (other than the local one),
    //! in a stable order.
    /*!`
     *  For secondary masters, the primary master is always the first element.
     */
    virtual const NObjectClient::TCellTagList& GetRegisteredMasterCellTags() const = 0;

    //! Returns a stable index of a given (registered) master cell (other than the local one).
    virtual int GetRegisteredMasterCellIndex(NObjectClient::TCellTag cellTag) const = 0;

    //! Picks a random (but deterministically chosen) secondary master cell to
    //! host an external chunk-owning node.
    /*!
     *  Only cells with EMasterCellRole::ChunkHost are considered.
     *  Cells with less-than-average number of chunks are typically preferred.
     *  The exact amount of skewness is controlled by #bias argument, 0 indicating no preference,
     *  and 1.0 indicating that cells with low number of chunks are picked twice as more often as those
     *  with the high number of chunks.
     *
     *  If no secondary cells are registered then #InvalidCellTag is returned.
     */
    virtual NObjectClient::TCellTag PickSecondaryChunkHostCell(double bias) = 0;

    //! Returns the total cluster statistics by summing counters for all cells (including primary).
    virtual const NProto::TCellStatistics& GetClusterStatistics() const = 0;

    //! Returns the channel to be used for communicating with another master.
    //! This channel has a properly configured timeout.
    //! Throws on error.
    virtual NRpc::IChannelPtr GetMasterChannelOrThrow(NObjectClient::TCellTag cellTag, NHydra::EPeerKind peerKind) = 0;

    //! Same as #GetMasterChannelOrThrow but returns |nullptr| if no channel is currently known.
    virtual NRpc::IChannelPtr FindMasterChannel(NObjectClient::TCellTag cellTag, NHydra::EPeerKind peerKind) = 0;

    //! Returns the mailbox used for communicating with the primary master cell.
    //! May return null if the cell is not connected yet.
    virtual NHiveServer::TMailbox* FindPrimaryMasterMailbox() = 0;


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
    virtual TFuture<void> SyncWithUpstream() = 0;

    DECLARE_INTERFACE_SIGNAL(void(NObjectClient::TCellTag), ValidateSecondaryMasterRegistration);
    DECLARE_INTERFACE_SIGNAL(void(NObjectClient::TCellTag), ReplicateKeysToSecondaryMaster);
    DECLARE_INTERFACE_SIGNAL(void(NObjectClient::TCellTag), ReplicateValuesToSecondaryMaster);
};

DEFINE_REFCOUNTED_TYPE(IMulticellManager)

////////////////////////////////////////////////////////////////////////////////

IMulticellManagerPtr CreateMulticellManager(TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster
