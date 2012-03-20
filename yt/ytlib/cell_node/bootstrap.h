#pragma once

#include "public.h"

#include <ytlib/actions/invoker.h>
//#include <ytlib/misc/guid.h>
// TODO(babenko): replace with public.h
#include <ytlib/bus/server.h>
// TODO(babenko): replace with public.h
#include <ytlib/rpc/channel.h>
#include <ytlib/rpc/server.h>
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
    IInvoker::TPtr GetControlInvoker() const;
    NBus::IBusServer::TPtr GetBusServer() const;
    NRpc::IChannel::TPtr GetMasterChannel() const;
    NRpc::IChannel::TPtr GetSchedulerChannel() const;
    NRpc::IServer::TPtr GetRpcServer() const;
    Stroka GetPeerAddress() const;
    NYTree::IMapNodePtr GetOrchidRoot() const;

    NChunkHolder::TBootstrap* GetChunkHolderBootstrap() const;
    NExecAgent::TBootstrap* GetExecAgentBootstrap() const;

    void Run();

private:
    Stroka ConfigFileName;
    TCellNodeConfigPtr Config;
    
    NChunkServer::TIncarnationId IncarnationId;
    IInvoker::TPtr ControlInvoker;
    NBus::IBusServer::TPtr BusServer;
    NRpc::IServer::TPtr RpcServer;
    NRpc::IChannel::TPtr MasterChannel;
    Stroka PeerAddress;
    NYTree::IMapNodePtr OrchidRoot;

    THolder<NChunkHolder::TBootstrap> ChunkHolderBootstrap;
    THolder<NExecAgent::TBootstrap> ExecAgentBootstrap;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellNode
} // namespace NYT
