#pragma once

#include "public.h"

#include <core/actions/invoker.h>

#include <core/rpc/public.h>

#include <ytlib/hydra/hydra_manager.pb.h>

#include <server/hydra/public.h>
#include <server/hydra/entity_map.h>

namespace NYT {
namespace NHive {

////////////////////////////////////////////////////////////////////////////////

#undef PostMessage // WinAPI is still with us :)

class THiveManager
    : public TRefCounted
{
public:
    THiveManager(
        const TCellGuid& selfCellGuid,
        THiveManagerConfigPtr config,
        TCellDirectoryPtr cellRegistry,
        IInvokerPtr automatonInvoker,
        NRpc::IRpcServerPtr rpcServer,
        NHydra::IHydraManagerPtr hydraManager,
        NHydra::TCompositeAutomatonPtr automaton);

    void Start();
    void Stop();

    const TCellGuid& GetSelfCellGuid() const;

    TMailbox* CreateMailbox(const TCellGuid& cellGuid);
    TMailbox* GetOrCreateMailbox(const TCellGuid& cellGuid);
    TMailbox* GetMailboxOrThrow(const TCellGuid& cellGuid);
    void RemoveMailbox(const TCellGuid& cellGuid);

    void PostMessage(TMailbox* mailbox, const TMessage& message);
    void PostMessage(TMailbox* mailbox, const ::google::protobuf::MessageLite& message);

    DECLARE_ENTITY_MAP_ACCESSORS(Mailbox, TMailbox, TCellGuid);

private:
    class TImpl;
    TIntrusivePtr<TImpl> Impl;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NHive
} // namespace NYT
