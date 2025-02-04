#pragma once

#include "public.h"

#include <yt/yt/server/lib/hive/proto/hive_manager.pb.h>

#include <yt/yt/server/lib/hydra/entity_map.h>
#include <yt/yt/server/lib/hydra/public.h>

#include <yt/yt/ytlib/cell_master_client/public.h>

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

//! Installs the mutation sender id into FLS.
class THiveMutationGuard
    : private TNonCopyable
{
public:
    explicit THiveMutationGuard(TCellId senderId);
    ~THiveMutationGuard();
};

//! Removes the mutation sender id from FLS. Restores it in destructor.
class TInverseHiveMutationGuard
    : private TNonCopyable
{
public:
    TInverseHiveMutationGuard();
    ~TInverseHiveMutationGuard();

private:
    const TCellId SenderId_;
};

////////////////////////////////////////////////////////////////////////////////

struct TSerializedMessage
    : public TRefCounted
{
    TString Type;
    TString Data;
};

DEFINE_REFCOUNTED_TYPE(TSerializedMessage)

////////////////////////////////////////////////////////////////////////////////

struct THiveEdge
{
    TCellId SourceCellId;
    TCellId DestinationCellId;

    bool operator==(const THiveEdge&) const = default;
};

void FormatValue(TStringBuilderBase* builder, const THiveEdge& edge, TStringBuf spec);

////////////////////////////////////////////////////////////////////////////////

/*!
 *  \note Thread affinity: single (unless noted otherwise)
 */
struct IHiveManager
    : public virtual TRefCounted
{
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

    virtual TMailboxHandle FindMailbox(TEndpointId endpointId) const = 0;
    virtual TMailboxHandle GetMailbox(TEndpointId endpointId) const = 0;
    virtual TMailboxHandle GetMailboxOrThrow(TEndpointId endpointId) const = 0;

    virtual TMailboxHandle CreateCellMailbox(TCellId cellId, bool allowResurrection = false) = 0;
    virtual TMailboxHandle GetOrCreateCellMailbox(TCellId cellId) = 0;

    virtual bool TryRemoveCellMailbox(TCellId cellId) = 0;
    virtual bool TryUnregisterCellMailbox(TCellId cellId) = 0;

    virtual void RegisterAvenueEndpoint(
        TAvenueEndpointId selfEndpointId,
        TPersistentMailboxStateCookie&& cookie) = 0;

    //! Unregisters given avenue endpoint and returns its state that allows
    //! to re-register this endpoint at some other cell later.
    /*!
     *  \note If the method is executed within a mutation received via current avenue
     *    then the return value will be empty. |allowDestructionInMessageToSelf| must
     *    be explicitly set to true to allow such behaviour.
     */
    virtual TPersistentMailboxStateCookie UnregisterAvenueEndpoint(
        TAvenueEndpointId selfEndpointId,
        bool allowDestructionInMessageToSelf = false) = 0;

    //! Posts a message for delivery (either reliable or not).
    virtual void PostMessage(
        TMailboxHandle mailbox,
        const TSerializedMessagePtr& message,
        bool reliable = true) = 0;
    virtual void PostMessage(
        TRange<TMailboxHandle> mailboxes,
        const TSerializedMessagePtr& message,
        bool reliable = true) = 0;
    virtual void PostMessage(
        TMailboxHandle mailbox,
        const ::google::protobuf::MessageLite& message,
        bool reliable = true) = 0;
    virtual void PostMessage(
        TRange<TMailboxHandle> mailboxes,
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

    //! This is intended to be used in tests to simulate bad connection.
    virtual void FreezeEdges(std::vector<THiveEdge> edgesToFreeze) = 0;
};

DEFINE_REFCOUNTED_TYPE(IHiveManager)

////////////////////////////////////////////////////////////////////////////////

IHiveManagerPtr CreateHiveManager(
    THiveManagerConfigPtr config,
    NHiveClient::ICellDirectoryPtr cellDirectory,
    NCellMasterClient::ICellDirectoryPtr masterDirectory,
    IAvenueDirectoryPtr avenueDirectory,
    TCellId selfCellId,
    IInvokerPtr automatonInvoker,
    NHydra::IHydraManagerPtr hydraManager,
    NHydra::TCompositeAutomatonPtr automaton,
    NHydra::IUpstreamSynchronizerPtr upstreamSynchronizer,
    NRpc::IAuthenticatorPtr authenticator);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHiveServer
