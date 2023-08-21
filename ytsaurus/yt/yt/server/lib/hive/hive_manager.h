#pragma once

#include "public.h"

#include <yt/yt/server/lib/hive/proto/hive_manager.pb.h>

#include <yt/yt/server/lib/hydra_common/entity_map.h>
#include <yt/yt/server/lib/hydra_common/public.h>

#include <yt/yt/ytlib/hive/public.h>

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NHiveServer {

////////////////////////////////////////////////////////////////////////////////

//! Returns |true| if the current fiber currently handles a mutation
//! posted via Hive.
bool IsHiveMutation();

//! Returns the id of the cell that posted a mutation currently handed
//! by a current fiber or null id if that mutation is not a Hive one.
TCellId GetHiveMutationSenderId();

////////////////////////////////////////////////////////////////////////////////

struct TSerializedMessage
    : public TRefCounted
{
    TString Type;
    TString Data;
};

DEFINE_REFCOUNTED_TYPE(TSerializedMessage)

////////////////////////////////////////////////////////////////////////////////

/*!
 *  \note Thread affinity: single (unless noted otherwise)
 */
struct IHiveManager
    : public virtual TRefCounted
{
public:
    /*!
     *  \note Thread affinity: any
     */
    virtual NRpc::IServicePtr GetRpcService() = 0;

    /*!
     *  \note Thread affinity: any
     */
    virtual NYTree::IYPathServicePtr GetOrchidService() = 0;

    /*!
     *  \note Thread affinity: any
     */
    virtual TCellId GetSelfCellId() const = 0;

    virtual TCellMailbox* CreateCellMailbox(TCellId cellId, bool allowResurrection = false) = 0;
    virtual TMailbox* FindMailbox(TEndpointId endpointId) const = 0;
    virtual TMailbox* GetMailbox(TEndpointId endpointId) const = 0;
    virtual TCellMailbox* GetOrCreateCellMailbox(TCellId cellId) = 0;
    virtual TMailbox* GetMailboxOrThrow(TEndpointId endpointId) const = 0;

    virtual void RemoveCellMailbox(TCellMailbox* mailbox) = 0;

    virtual void RegisterAvenueEndpoint(
        TAvenueEndpointId selfEndpointId,
        TPersistentMailboxState&& cookie) = 0;

    virtual TPersistentMailboxState UnregisterAvenueEndpoint(TAvenueEndpointId selfEndpointId) = 0;

    //! Posts a message for delivery (either reliable or not).
    virtual void PostMessage(
        TMailbox* mailbox,
        const TSerializedMessagePtr& message,
        bool reliable = true) = 0;
    virtual void PostMessage(
        const TMailboxList& mailboxes,
        const TSerializedMessagePtr& message,
        bool reliable = true) = 0;
    virtual void PostMessage(
        TMailbox* mailbox,
        const ::google::protobuf::MessageLite& message,
        bool reliable = true) = 0;
    virtual void PostMessage(
        const TMailboxList& mailboxes,
        const ::google::protobuf::MessageLite& message,
        bool reliable = true) = 0;

    //! When called at instant T, returns a future which gets set
    //! when all mutations enqueued at the remote side (represented by #mailbox)
    //! prior to T, are received and applied.
    //! If #enableBatching is |true| then syncs are additionally batched.
    /*!
     *  \note Thread affinity: any
     */
    virtual TFuture<void> SyncWith(TCellId cellId, bool enableBatching) = 0;

    DECLARE_INTERFACE_ENTITY_WITH_IRREGULAR_PLURAL_MAP_ACCESSORS(CellMailbox, CellMailboxes, TCellMailbox);
    DECLARE_INTERFACE_ENTITY_WITH_IRREGULAR_PLURAL_MAP_ACCESSORS(AvenueMailbox, AvenueMailboxes, TAvenueMailbox);
};

DEFINE_REFCOUNTED_TYPE(IHiveManager)

////////////////////////////////////////////////////////////////////////////////

IHiveManagerPtr CreateHiveManager(
    THiveManagerConfigPtr config,
    NHiveClient::ICellDirectoryPtr cellDirectory,
    IAvenueDirectoryPtr avenueDirectory,
    TCellId selfCellId,
    IInvokerPtr automatonInvoker,
    NHydra::IHydraManagerPtr hydraManager,
    NHydra::TCompositeAutomatonPtr automaton,
    NHydra::IUpstreamSynchronizerPtr upstreamSynchronizer,
    NRpc::IAuthenticatorPtr authenticator);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHiveServer
