#pragma once

#include "public.h"

#include <ytlib/actions/action_queue.h>

#include <ytlib/bus/public.h>

#include <ytlib/rpc/public.h>

#include <ytlib/ytree/public.h>

#include <ytlib/chunk_client/public.h>

#include <ytlib/node_tracker_client/node_directory.h>

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
    IInvokerPtr GetControlInvoker() const;
    NRpc::IChannelPtr GetMasterChannel() const;
    NRpc::IChannelPtr GetSchedulerChannel() const;
    NRpc::IServerPtr GetRpcServer() const;
    const NNodeTrackerClient::TNodeDescriptor& GetLocalDescriptor() const;
    NYTree::IMapNodePtr GetOrchidRoot() const;

    NChunkHolder::TBootstrap* GetChunkHolderBootstrap() const;
    NExecAgent::TBootstrap* GetExecAgentBootstrap() const;

    TNodeMemoryTracker& GetMemoryUsageTracker();

    void Run();

private:
    Stroka ConfigFileName;
    TCellNodeConfigPtr Config;

    TActionQueuePtr ControlQueue;
    NBus::IBusServerPtr BusServer;
    NRpc::IServerPtr RpcServer;
    NRpc::IChannelPtr MasterChannel;
    NRpc::IChannelPtr SchedulerChannel;
    NNodeTrackerClient::TNodeDescriptor LocalDescriptor;
    NYTree::IMapNodePtr OrchidRoot;

    THolder<NChunkHolder::TBootstrap> ChunkHolderBootstrap;
    THolder<NExecAgent::TBootstrap> ExecAgentBootstrap;
    TMemoryUsageTracker<EMemoryConsumer> MemoryUsageTracker;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellNode
} // namespace NYT
