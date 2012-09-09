#pragma once

#include "public.h"

#include <ytlib/actions/action_queue.h>

#include <ytlib/bus/public.h>

#include <ytlib/rpc/public.h>

#include <ytlib/ytree/public.h>

#include <ytlib/chunk_client/public.h>

#include <server/chunk_holder/public.h>

#include <server/chunk_server/public.h>

#include <server/exec_agent/public.h>

namespace NYT {
namespace NCellNode {

////////////////////////////////////////////////////////////////////////////////

class TBootstrap
{
public:
    TBootstrap(
        const Stroka& configFileName,
        TCellNodeConfigPtr config);
    ~TBootstrap();

    TCellNodeConfigPtr GetConfig() const;
    NChunkServer::TIncarnationId GetIncarnationId() const;
    IInvokerPtr GetControlInvoker() const;
    NRpc::IChannelPtr GetMasterChannel() const;
    NRpc::IChannelPtr GetSchedulerChannel() const;
    NRpc::IServerPtr GetRpcServer() const;
    Stroka GetPeerAddress() const;
    NYTree::IMapNodePtr GetOrchidRoot() const;

    NChunkHolder::TBootstrap* GetChunkHolderBootstrap() const;
    NExecAgent::TBootstrap* GetExecAgentBootstrap() const;

    TNodeMemoryTracker& GetMemoryUsageTracker();

    void Run();

private:
    Stroka ConfigFileName;
    TCellNodeConfigPtr Config;
    
    NChunkServer::TIncarnationId IncarnationId;
    TActionQueuePtr ControlQueue;
    NBus::IBusServerPtr BusServer;
    NRpc::IServerPtr RpcServer;
    NRpc::IChannelPtr MasterChannel;
    NRpc::IChannelPtr SchedulerChannel;
    Stroka PeerAddress;
    NYTree::IMapNodePtr OrchidRoot;

    THolder<NChunkHolder::TBootstrap> ChunkHolderBootstrap;
    THolder<NExecAgent::TBootstrap> ExecAgentBootstrap;
    TMemoryUsageTracker<EMemoryConsumer> MemoryUsageTracker;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellNode
} // namespace NYT
