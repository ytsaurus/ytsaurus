#pragma once

#include <yql/tools/yqlworker/misc/duplex_ipc_channel.h>

#include <util/system/defaults.h>
#include <util/system/pipe.h>


namespace NYql {
namespace NSubprocessTaskExecutor {

//////////////////////////////////////////////////////////////////////////////
// TControlClient
//////////////////////////////////////////////////////////////////////////////
struct IWorkerTerminationHandler
{
    virtual ~IWorkerTerminationHandler() = default;

    virtual void OnWorkerTermination(pid_t pid, int status) = 0;
    virtual void OnServerTermination() = 0;
    virtual void OnServerReady(bool inTerminatingState) = 0;
    virtual void OnDisconnect() = 0;
};

class TControlClient
{
public:
    TControlClient(
            TIpcChannelEndpoint& serverCh,
            IWorkerTerminationHandler* handler);
    ~TControlClient();

    void SpawnWorkers(ui32 count, int dataServerPort);
    void KillWorker(pid_t pid);
    void RequestReady();

    void EventLoopCycle();

    void RunEventLoopInBackground();
    void Shutdown();

private:
    struct TImpl;
    THolder<TImpl> Impl_;
};

//////////////////////////////////////////////////////////////////////////////
// TControlServer
//////////////////////////////////////////////////////////////////////////////
class TControlServer;

struct IWorkerControlServerHandler
{
    virtual ~IWorkerControlServerHandler() = default;

    virtual void OnSpawnWorkers(ui32 count, int dataServerPort) = 0;
    virtual void OnKillWorker(pid_t pid) = 0;
    virtual void OnRequestReady() = 0;
    virtual void OnDisconnect() = 0;
};

class TControlServer
{
public:
    TControlServer(
            TIpcChannelEndpoint& clientCh,
            PIPEHANDLE signalPipeR,
            IWorkerControlServerHandler* handler);
    ~TControlServer();

    void NotifyAboutTerminatedWorkers(
            const TVector<std::pair<pid_t, int>>& statuses);
    void NotifyAboutSelfTermination();
    void NotifyAboutSelfReady();

    void EventLoopCycle();

    void RunEventLoopInBackground();
    void Shutdown();

private:
    struct TImpl;
    THolder<TImpl> Impl_;
};

} // namespace NSubprocessTaskExecutor
} // namespace NYql
