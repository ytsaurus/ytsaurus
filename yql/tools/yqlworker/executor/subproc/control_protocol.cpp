#include "control_protocol.h"

#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/utils/signals/signals.h>
#include <yql/essentials/utils/signals/utils.h>
#include <yql/tools/yqlworker/executor/subproc/control_protocol.pb.h>
#include <yql/tools/yqlworker/misc/background_threads.h>

#include <atomic>
#include <errno.h>


namespace NYql {
namespace NSubprocessTaskExecutor {
namespace {

const static TDuration DEFAULT_IO_TIMEOUT = TDuration::Seconds(1);

void DrainAll(SOCKET fd) {
    char sinkBuf[0x10];
    ssize_t nRead;

    do {
        nRead = ::read(fd, sinkBuf, Y_ARRAY_SIZE(sinkBuf));
        if (nRead == -1) {
            if (errno == EINTR) continue;
            else if (errno == EAGAIN) break;
            else ythrow TSystemError() << "error while draining fd";
        }
    } while (nRead != 0);
}

} // namespace

//////////////////////////////////////////////////////////////////////////////
// TControlClient
//////////////////////////////////////////////////////////////////////////////
struct TControlClient::TImpl: private TBackgroundThreads {
    TIpcChannelEndpoint& ServerCh;
    IWorkerTerminationHandler* TerminationHandler;
    std::atomic<ui64> CmdSeqNo;


    TImpl(TIpcChannelEndpoint& serverCh,
            IWorkerTerminationHandler* handler)
        : ServerCh(serverCh)
        , TerminationHandler(handler)
        , CmdSeqNo(0)
    {
    }

    void SpawnWorkers(ui32 count, int dataServerPort) {
        NProto::TControlServerCommand cmd;
        cmd.SetType(NProto::TControlServerCommand::SPAWN_WORKERS);
        cmd.SetSeqNo(CmdSeqNo.fetch_add(1, std::memory_order_relaxed));
        cmd.SetCount(count);
        cmd.SetDataServerPort(dataServerPort);
        ServerCh.SendMsg(cmd);
    }

    void KillWorker(pid_t pid) {
        NProto::TControlServerCommand cmd;
        cmd.SetType(NProto::TControlServerCommand::KILL_WORKER);
        cmd.SetSeqNo(CmdSeqNo.fetch_add(1, std::memory_order_relaxed));
        cmd.SetPid(pid);
        ServerCh.SendMsg(cmd);
    }

    void RequestReady() {
        NProto::TControlServerCommand cmd;
        cmd.SetType(NProto::TControlServerCommand::REQUEST_READY);
        cmd.SetSeqNo(CmdSeqNo.fetch_add(1, std::memory_order_relaxed));
        ServerCh.SendMsg(cmd);
    }

    void EventLoopCycle() {
        struct pollfd fd = { ServerCh.GetFd(), POLLIN, 0 };

        ssize_t n = PollD(&fd, 1, DEFAULT_IO_TIMEOUT.ToDeadLine());
        if (n < 0 && n != -ETIMEDOUT) {
            ythrow TSystemError() << "cannot poll on server IPC channel";
        }

        if (fd.revents & POLLIN) {
            HandleSocketEvent();
        }
    }

    void RunEventLoopInBackground() {
        if (Size() == 0) {
            Add([this]() {
                try {
                    EventLoopCycle();
                    return TDuration::Zero();
                } catch (const TPeerDead&) {
                    YQL_LOG(WARN) << "Peer is dead so stop event loop";
                    return TDuration::Max();
                } catch (...) {
                    YQL_LOG(ERROR) << "unhandled exception in control client: "
                            << CurrentExceptionMessage();
                    Y_ABORT();
                }
            });
        }
    }

    void Shutdown() {
        Stop();
        ServerCh.Close();
    }

private:
    void HandleSocketEvent() {
        try {
            ProcessSocketCommands();
        } catch (const TPeerDead&) {
            TerminationHandler->OnDisconnect();
            throw;
        }
    }

    void ProcessSocketCommands() {
        NProto::TControlClientCommand cmd;
        while (ServerCh.RecvMsgNonBlock(&cmd)) {
            HandleCommand(cmd);
            cmd.Clear();
        }
    }

    void HandleCommand(const NProto::TControlClientCommand& cmd) {
        try {
            switch (cmd.GetType()) {
            case NProto::TControlClientCommand::WORKER_TERM:
                TerminationHandler->OnWorkerTermination(cmd.GetPid(), cmd.GetStatus());
                break;
            case NProto::TControlClientCommand::SERVER_TERM:
                TerminationHandler->OnServerTermination();
                break;
            case NProto::TControlClientCommand::SERVER_READY:
                TerminationHandler->OnServerReady(cmd.GetInTerminatingState());
                break;
            default:
                ythrow yexception() << "unknown client_control command: "
                                    << static_cast<int>(cmd.GetType());
            }
        } catch (...) {
            YQL_LOG(ERROR) << "error on handling client_control command {"
                           << PbMessageToStr(cmd) << "} "
                           << CurrentExceptionMessage();
        }
    }
};

TControlClient::TControlClient(
        TIpcChannelEndpoint& serverCh,
        IWorkerTerminationHandler* handler)
    : Impl_(new TImpl(serverCh, handler))
{
}

TControlClient::~TControlClient()
{
    if (Impl_) {
        Impl_->Shutdown();
    }
}

void TControlClient::SpawnWorkers(ui32 count, int dataServerPort)
{
    Y_DEBUG_ABORT_UNLESS(Impl_);
    Impl_->SpawnWorkers(count, dataServerPort);
}

void TControlClient::KillWorker(pid_t pid)
{
    Y_DEBUG_ABORT_UNLESS(Impl_);
    Impl_->KillWorker(pid);
}

void TControlClient::RequestReady() {
    Y_DEBUG_ABORT_UNLESS(Impl_);
    Impl_->RequestReady();
}

void TControlClient::EventLoopCycle()
{
    Y_DEBUG_ABORT_UNLESS(Impl_);
    Impl_->EventLoopCycle();
}

void TControlClient::RunEventLoopInBackground()
{
    Y_DEBUG_ABORT_UNLESS(Impl_);
    Impl_->RunEventLoopInBackground();
}

void TControlClient::Shutdown()
{
    Y_DEBUG_ABORT_UNLESS(Impl_);
    Impl_->Shutdown();
    Impl_.Reset();
}

//////////////////////////////////////////////////////////////////////////////
// TControlServer
//////////////////////////////////////////////////////////////////////////////
struct TControlServer::TImpl: private TBackgroundThreads {
    TIpcChannelEndpoint& ClientCh;
    PIPEHANDLE SignalPipeR;
    IWorkerControlServerHandler* ServerHandler;
    ui64 CmdSeqNo;
    bool InTerminatingState;


    TImpl(TIpcChannelEndpoint& clientCh,
            PIPEHANDLE signalPipeR,
            IWorkerControlServerHandler* handler)
        : ClientCh(clientCh)
        , SignalPipeR(signalPipeR)
        , ServerHandler(handler)
        , CmdSeqNo(0)
        , InTerminatingState(false)
    {
    }

    void NotifyAboutTerminatedWorkers(
            const TVector<std::pair<pid_t, int>>& statuses)
    {
        for (const auto& status: statuses) {
            NProto::TControlClientCommand cmd;
            cmd.SetType(NProto::TControlClientCommand::WORKER_TERM);
            cmd.SetSeqNo(CmdSeqNo++);
            cmd.SetPid(status.first);
            cmd.SetStatus(status.second);
            ClientCh.SendMsg(cmd);
        }
    }

    void NotifyAboutSelfTermination() {
        NProto::TControlClientCommand cmd;
        cmd.SetType(NProto::TControlClientCommand::SERVER_TERM);
        cmd.SetSeqNo(CmdSeqNo++);
        ClientCh.SendMsg(cmd);
    }

    void NotifyAboutSelfReady() {
        NProto::TControlClientCommand cmd;
        cmd.SetType(NProto::TControlClientCommand::SERVER_READY);
        cmd.SetSeqNo(CmdSeqNo++);
        cmd.SetInTerminatingState(InTerminatingState);
        ClientCh.SendMsg(cmd);
    }

    void EventLoopCycle() {
        struct pollfd fds[] = {
            { ClientCh.GetFd(), POLLIN, 0 },
            { SignalPipeR, POLLIN, 0 }
        };
        nfds_t nfds = (SignalPipeR == INVALID_PIPEHANDLE) ? 1 : 2;

        ssize_t n = PollD(fds, nfds, DEFAULT_IO_TIMEOUT.ToDeadLine());
        if (n < 0 && n != -ETIMEDOUT) {
            ythrow TSystemError() << "cannot poll on client IPC channel";
        }

        InTerminatingState = InTerminatingState || (NeedTerminate || NeedQuit);

        for (nfds_t i = 0; i < nfds; i++) {
            if (fds[i].revents & POLLIN) {
                if (fds[i].fd == ClientCh.GetFd()) {
                    HandleSocketEvent();
                } else if (fds[i].fd == SignalPipeR) {
                    HandlePipeEvent();
                }
            }
        }
    }

    void RunEventLoopInBackground() {
        if (Size() == 0) {
            Add([this]() {
                try {
                    EventLoopCycle();
                    return TDuration::Zero();
                } catch (const TPeerDead&) {
                    YQL_LOG(WARN) << "Peer is dead so stop event loop";
                    return TDuration::Max();
                } catch (...) {
                    YQL_LOG(ERROR) << "unhandled exception in control server: "
                                   << CurrentExceptionMessage();
                    Y_ABORT();
                }
            });
        }
    }

    void Shutdown() {
        Stop();
        ClientCh.Close();
    }

private:
    void HandleSocketEvent() {
        try {
            ProcessSocketCommands();
        } catch (const TPeerDead&) {
            ServerHandler->OnDisconnect();
            throw;
        }
    }

    void ProcessSocketCommands() {
        NProto::TControlServerCommand cmd;
        while (ClientCh.RecvMsgNonBlock(&cmd)) {
            YQL_LOG(DEBUG) << "received server_control command "
                << PbMessageToStr(cmd);
            HandleCommand(cmd);
            cmd.Clear();
        }
    }

    void HandleCommand(const NProto::TControlServerCommand& cmd) {
        try {
            switch (cmd.GetType()) {
            case NProto::TControlServerCommand::SPAWN_WORKERS:
                ServerHandler->OnSpawnWorkers(
                            cmd.GetCount(),
                            cmd.GetDataServerPort());
                break;
            case NProto::TControlServerCommand::KILL_WORKER:
                ServerHandler->OnKillWorker(cmd.GetPid());
                break;
            case NProto::TControlServerCommand::REQUEST_READY:
                ServerHandler->OnRequestReady();
                break;
            default:
                ythrow yexception() << "unknown server_control command: "
                                    << static_cast<int>(cmd.GetType());
            }
        } catch (...) {
            YQL_LOG(ERROR) << "error on handling server_control command {"
                           << PbMessageToStr(cmd) << "} "
                           << CurrentExceptionMessage();
        }
    }

    void HandlePipeEvent() {
        DrainAll(SignalPipeR);
    }
};

TControlServer::TControlServer(
        TIpcChannelEndpoint& clientCh,
        PIPEHANDLE signalPipeR,
        IWorkerControlServerHandler* handler)
    : Impl_(new TImpl(clientCh, signalPipeR, handler))
{
}

TControlServer::~TControlServer()
{
    if (Impl_) {
        Impl_->Shutdown();
    }
}

void TControlServer::NotifyAboutTerminatedWorkers(
        const TVector<std::pair<pid_t, int>>& statuses)
{
    Y_DEBUG_ABORT_UNLESS(Impl_);
    Impl_->NotifyAboutTerminatedWorkers(statuses);
}

void TControlServer::NotifyAboutSelfTermination()
{
    Y_DEBUG_ABORT_UNLESS(Impl_);
    Impl_->NotifyAboutSelfTermination();
}

void TControlServer::NotifyAboutSelfReady()
{
    Y_DEBUG_ABORT_UNLESS(Impl_);
    Impl_->NotifyAboutSelfReady();
}

void TControlServer::EventLoopCycle()
{
    Y_DEBUG_ABORT_UNLESS(Impl_);
    Impl_->EventLoopCycle();
}

void TControlServer::RunEventLoopInBackground()
{
    Y_DEBUG_ABORT_UNLESS(Impl_);
    Impl_->RunEventLoopInBackground();
}

void TControlServer::Shutdown()
{
    Y_DEBUG_ABORT_UNLESS(Impl_);
    Impl_->Shutdown();
    Impl_.Reset();
}

} // namespace NSubprocessTaskExecutor
} // namespace NYql
