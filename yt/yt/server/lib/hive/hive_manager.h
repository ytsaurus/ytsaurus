#pragma once

#include "public.h"

#include <yt/server/lib/hive/proto/hive_manager.pb.h>

#include <yt/server/lib/hydra/entity_map.h>
#include <yt/server/lib/hydra/public.h>

#include <yt/ytlib/hive/public.h>

#include <yt/core/rpc/public.h>

#include <yt/core/ytree/public.h>

namespace NYT::NHiveServer {

////////////////////////////////////////////////////////////////////////////////

//! Returns |true| if the current fiber currently handles a mutation
//! posted via Hive.
bool IsHiveMutation();

////////////////////////////////////////////////////////////////////////////////

struct TSerializedMessage
    : public TIntrinsicRefCounted
{
    TString Type;
    TString Data;
};

DEFINE_REFCOUNTED_TYPE(TSerializedMessage)

////////////////////////////////////////////////////////////////////////////////

/*!
 *  \note Thread affinity: single (unless noted otherwise)
 */
class THiveManager
    : public TRefCounted
{
public:
    THiveManager(
        THiveManagerConfigPtr config,
        NHiveClient::TCellDirectoryPtr cellDirectory,
        TCellId selfCellId,
        IInvokerPtr automatonInvoker,
        NHydra::IHydraManagerPtr hydraManager,
        NHydra::TCompositeAutomatonPtr automaton);

    ~THiveManager();

    /*!
     *  \note Thread affinity: any
     */
    NRpc::IServicePtr GetRpcService();

    /*!
     *  \note Thread affinity: any
     */
    NYTree::IYPathServicePtr GetOrchidService();

    /*!
     *  \note Thread affinity: any
     */
    TCellId GetSelfCellId() const;

    TMailbox* CreateMailbox(TCellId cellId);
    TMailbox* FindMailbox(TCellId cellId);
    TMailbox* GetOrCreateMailbox(TCellId cellId);
    TMailbox* GetMailboxOrThrow(TCellId cellId);

    void RemoveMailbox(TMailbox* mailbox);

    //! Posts a message for delivery (either reliable or not).
    void PostMessage(
        TMailbox* mailbox,
        const TSerializedMessagePtr& message,
        bool reliable = true);
    void PostMessage(
        const TMailboxList& mailboxes,
        const TSerializedMessagePtr& message,
        bool reliable = true);
    void PostMessage(
        TMailbox* mailbox,
        const ::google::protobuf::MessageLite& message,
        bool reliable = true);
    void PostMessage(
        const TMailboxList& mailboxes,
        const ::google::protobuf::MessageLite& message,
        bool reliable = true);

    //! When called at instant T, returns a future which gets set
    //! when all mutations enqueued at the remote side (represented by #mailbox)
    //! prior to T, are received and applied.
    //! If #enableBatching is |true| then syncs are additionally batched.
    /*!
     *  \note Thread affinity: any
     */
    TFuture<void> SyncWith(TCellId cellId, bool enableBatching);

    DECLARE_ENTITY_MAP_ACCESSORS(Mailbox, TMailbox);

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(THiveManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHiveServer
