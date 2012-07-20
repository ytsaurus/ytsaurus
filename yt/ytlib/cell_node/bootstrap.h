#pragma once

#include "public.h"

#include <ytlib/actions/action_queue.h>
// TODO(babenko): replace with public.h
#include <ytlib/bus/server.h>
#include <ytlib/rpc/public.h>
#include <ytlib/ytree/public.h>
#include <ytlib/chunk_server/public.h>
#include <ytlib/chunk_holder/public.h>
#include <ytlib/exec_agent/public.h>

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

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellNode
} // namespace NYT
