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

    NRpc::IServicePtr GetRpcService();

    TCellId GetSelfCellId() const;

    TMailbox* CreateMailbox(TCellId cellId);
    TMailbox* GetOrCreateMailbox(TCellId cellId);
    TMailbox* GetMailboxOrThrow(TCellId cellId);

    void RemoveMailbox(TMailbox* mailbox);

    //! Posts a message for delivery (either reliable or not).
    void PostMessage(
        TMailbox* mailbox,
        TRefCountedEncapsulatedMessagePtr message,
        bool reliable = true);
    void PostMessage(
        const TMailboxList& mailboxes,
        TRefCountedEncapsulatedMessagePtr message,
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
    TFuture<void> SyncWith(TMailbox* mailbox);

    NYTree::IYPathServicePtr GetOrchidService();

    //! Raised upon receiving incoming messages from another Hive instance.
    //! The handler must start an appropriate synchronization process and return a future
    //! that gets set when sync is reached.
    DECLARE_SIGNAL(TFuture<void>(NHiveClient::TCellId srcCellId), IncomingMessageUpstreamSync);

    DECLARE_ENTITY_MAP_ACCESSORS(Mailbox, TMailbox);

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;

};

DEFINE_REFCOUNTED_TYPE(THiveManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHiveServer
