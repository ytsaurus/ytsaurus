#pragma once

#include "public.h"

#include <core/rpc/public.h>

#include <core/yson/public.h>

#include <ytlib/hydra/hydra_manager.pb.h>

#include <server/hydra/public.h>
#include <server/hydra/entity_map.h>

#include <server/hive/hive_manager.pb.h>

namespace NYT {
namespace NHive {

////////////////////////////////////////////////////////////////////////////////

// WinAPI is still with us :)
#undef PostMessage
#undef SendMessage

class THiveManager
    : public TRefCounted
{
public:
    THiveManager(
        THiveManagerConfigPtr config,
        TCellDirectoryPtr cellDirectory,
        const TCellId& selfCellId,
        IInvokerPtr automatonInvoker,
        NHydra::IHydraManagerPtr hydraManager,
        NHydra::TCompositeAutomatonPtr automaton);

    ~THiveManager();

    NRpc::IServicePtr GetRpcService();

    const TCellId& GetSelfCellId() const;

    TMailbox* CreateMailbox(const TCellId& cellId);
    TMailbox* GetOrCreateMailbox(const TCellId& cellId);
    TMailbox* GetMailboxOrThrow(const TCellId& cellId);
    void RemoveMailbox(const TCellId& cellId);

    //! Posts a message for reliable delivery.
    void PostMessage(TMailbox* mailbox, const NProto::TEncapsulatedMessage& message);
    void PostMessage(TMailbox* mailbox, const ::google::protobuf::MessageLite& message);

    //! Sends a message to the cell. No delivery or serialization is guaranteed.
    void SendMessage(TMailbox* mailbox, const NProto::TEncapsulatedMessage& message);
    //! Sends a message to the cell. No delivery or serialization is guaranteed.
    void SendMessage(TMailbox* mailbox, const ::google::protobuf::MessageLite& message);

    void BuildOrchidYson(NYson::IYsonConsumer* consumer);

    DECLARE_ENTITY_MAP_ACCESSORS(Mailbox, TMailbox, TCellId);

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;

};

DEFINE_REFCOUNTED_TYPE(THiveManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NHive
} // namespace NYT
