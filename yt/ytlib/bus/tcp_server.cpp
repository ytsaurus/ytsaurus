#include "stdafx.h"
#include "tcp_server.h"
#include "tcp_dispatcher_impl.h"
#include "server.h"
#include "config.h"
#include "bus.h"
#include "tcp_connection.h"

#include <ytlib/misc/thread_affinity.h>
#include <ytlib/misc/address.h>
#include <ytlib/logging/tagged_logger.h>

#ifndef _WIN32
    #include <netinet/tcp.h>
    #include <sys/socket.h>
#endif

namespace NYT {
namespace NBus {

////////////////////////////////////////////////////////////////////////////////

static NProfiling::TProfiler& Profiler = BusProfiler;
static NProfiling::TAggregateCounter AcceptTime("/accept_time");

////////////////////////////////////////////////////////////////////////////////

class TTcpBusServer
    : public IEventLoopObject
{
public:
    TTcpBusServer(
        TTcpBusServerConfigPtr config,
        IMessageHandlerPtr handler)
        : Config(config)
        , Handler(handler)
        , Logger(BusLogger)
        , ServerSocket(INVALID_SOCKET)
        , ServerFd(INVALID_SOCKET)
    {
        VERIFY_THREAD_AFFINITY_ANY();
        YCHECK(config);
        YCHECK(handler);

        Logger.AddTag(Sprintf("Port: %d", Config->Port));
    }

    // IEventLoopObject implementation.

    virtual void SyncInitialize()
    {
        VERIFY_THREAD_AFFINITY(EventLoop);
        
        // This may throw.
        OpenServerSocket();

        const auto& eventLoop = TTcpDispatcher::TImpl::Get()->GetEventLoop();
        AcceptWatcher.Reset(new ev::io(eventLoop));
        AcceptWatcher->set<TTcpBusServer, &TTcpBusServer::OnAccept>(this);
        AcceptWatcher->start(ServerFd, ev::READ);
    }

    virtual void SyncFinalize()
    {
        VERIFY_THREAD_AFFINITY(EventLoop);

        AcceptWatcher.Destroy();

        CloseServerSocket();

        FOREACH (auto connection, Connections) {
            connection->Terminate(TError("Bus server terminated"));
        }
    }

    virtual Stroka GetLoggingId() const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return Sprintf("Port: %d", Config->Port);
    }

private:
    TTcpBusServerConfigPtr Config;
    IMessageHandlerPtr Handler;

    NLog::TTaggedLogger Logger;
    
    THolder<ev::io> AcceptWatcher;

    int ServerSocket;
    int ServerFd;

    yhash_set<TTcpConnectionPtr> Connections;

    DECLARE_THREAD_AFFINITY_SLOT(EventLoop);


    void OnConnectionTerminated(TTcpConnectionPtr connection, TError error)
    {
        VERIFY_THREAD_AFFINITY_ANY();
        UNUSED(error);

        YVERIFY(Connections.erase(connection) == 1);
    }


    void OpenServerSocket()
    {
        LOG_DEBUG("Opening server socket");

        ServerSocket = socket(AF_INET6, SOCK_STREAM, IPPROTO_TCP);
        if (ServerSocket == INVALID_SOCKET) {
        	int error = LastSystemError();
            ythrow yexception() << Sprintf("Failed to create server socket (ErrorCode: %d)\n%s",
                error,
                LastSystemErrorText(error));
        }

#ifdef _WIN32
        ServerFd = _open_osfhandle(ServerSocket, 0);
#else
        ServerFd = ServerSocket;
#endif

        // TODO(babenko): check for errors
        {
            int flag = 0;
            setsockopt(ServerSocket, IPPROTO_IPV6, IPV6_V6ONLY, (const char*) &flag, sizeof(flag));
        }

        {
            int flag = 1;
            setsockopt(ServerSocket, SOL_SOCKET, SO_REUSEADDR, (const char*) &flag, sizeof(flag));
        }

        {
            sockaddr_in6 serverAddress;
            memset(&serverAddress, 0, sizeof(serverAddress));
            serverAddress.sin6_family = AF_INET6;
            serverAddress.sin6_addr = in6addr_any;
            serverAddress.sin6_port = htons(Config->Port);
            if (bind(ServerSocket, (sockaddr*)&serverAddress, sizeof(serverAddress)) != 0) {
                int error = LastSystemError();
                CloseServerSocket();
                ythrow yexception() << Sprintf("Failed to bind server socket to port %d (ErrorCode: %d)\n%s",
                    Config->Port,
                    error,
                    LastSystemErrorText(error));
            }
        }

        InitSocket(ServerSocket);

        if (listen(ServerSocket, SOMAXCONN) == SOCKET_ERROR) {
            int error = LastSystemError();
            CloseServerSocket();
            ythrow yexception() << Sprintf("Failed to listen to server socket (ErrorCode: %d)\n%s",
                error,
                LastSystemErrorText(error));
        }

        LOG_DEBUG("Server socket opened");
    }

    void CloseServerSocket()
    {
        if (ServerFd != INVALID_SOCKET) {
            close(ServerFd);
            LOG_DEBUG("Server socket closed");
        }
        ServerSocket = INVALID_SOCKET;
        ServerFd = INVALID_SOCKET;
    }

    void InitSocket(SOCKET socket)
    {
        // TODO(babenko): check results
#ifdef _win_
        unsigned long dummy = 1;
        ioctlsocket(socket, FIONBIO, &dummy);
#else
        fcntl(socket, F_SETFL, O_NONBLOCK);
        fcntl(socket, F_SETFD, FD_CLOEXEC);
#endif
    }


    void OnAccept(ev::io&, int revents)
    {
        VERIFY_THREAD_AFFINITY(EventLoop);

        if (revents & ev::ERROR) {
            LOG_WARNING("Accept error");
            return;
        }

        while (true) {
            TNetworkAddress clientAddress;
            int clientAddressLen = clientAddress.GetLength();
            SOCKET clientSocket;
            PROFILE_AGGREGATED_TIMING (AcceptTime) {
                clientSocket = accept(ServerSocket, clientAddress.GetSockAddr(), &clientAddressLen);
            }

            if (clientSocket == INVALID_SOCKET) {
                auto error = LastSystemError();
                if (IsSocketError(error)) {
                    LOG_WARNING("Error accepting connection (ErrorCode: %d)\n%s",
                        error,
                        LastSystemErrorText(error));
                }
                break;
            }

            {
                int flag = 1;
                setsockopt(clientSocket, IPPROTO_TCP, TCP_NODELAY, (const char*) &flag, sizeof(flag));
            }

#if !defined(_WIN32) && !defined(__APPLE__)
            {
                int priority = Config->Priority;
                setsockopt(clientSocket, SOL_SOCKET, SO_PRIORITY, (const char*) &priority, sizeof(priority));
            }
#endif
            InitSocket(clientSocket);

            auto connection = New<TTcpConnection>(
                EConnectionType::Server,
                TConnectionId::Create(),
                clientSocket,
                ToString(clientAddress, true),
                0,
                Handler);
            connection->SubscribeTerminated(BIND(
                &TTcpBusServer::OnConnectionTerminated,
                MakeWeak(this),
                connection));
            YVERIFY(Connections.insert(connection).second);
            TTcpDispatcher::TImpl::Get()->AsyncRegister(connection);
        }       
    }


    bool IsSocketError(ssize_t result)
    {
#ifdef _WIN32
        return result != WSAEINPROGRESS && result != WSAEWOULDBLOCK;
#else
        return result != EINPROGRESS && result != EWOULDBLOCK;
#endif
    }
};

typedef TIntrusivePtr<TTcpBusServer> TTcpBusServerPtr;

////////////////////////////////////////////////////////////////////////////////

//! A lightweight proxy controlling the lifetime of #TTcpBusServer.
/*!
 *  When the last strong reference vanishes, it unregisters the underlying
 *  server instance.
 */
class TTcpBusServerProxy
    : public IBusServer
{
public:
    explicit TTcpBusServerProxy(TTcpBusServerConfigPtr config)
        : Config(config)
        , Running(false)
    {
        YASSERT(config);
    }

    ~TTcpBusServerProxy()
    {
        Stop();
    }

    virtual void Start(IMessageHandlerPtr handler)
    {
        TGuard<TSpinLock> guard(SpinLock);
        
        YCHECK(!Running);

        auto server = New<TTcpBusServer>(Config, handler);
        auto error = TTcpDispatcher::TImpl::Get()->AsyncRegister(server).Get();
        if (!error.IsOK()) {
            ythrow yexception() << error.ToString();
        }

        Server = server;
        Running = true;
    }

    virtual void Stop()
    {
        TGuard<TSpinLock> guard(SpinLock);

        if (!Running) {
            return;
        }

        auto error = TTcpDispatcher::TImpl::Get()->AsyncUnregister(Server).Get();
        // Shutdown will hopefully never fail.
        YCHECK(error.IsOK());

        Server.Reset();
        Running = false;
    }

private:
    TTcpBusServerConfigPtr Config;

    TSpinLock SpinLock;
    bool Running;
    TTcpBusServerPtr Server;

};

////////////////////////////////////////////////////////////////////////////////

IBusServerPtr CreateTcpBusServer(TTcpBusServerConfigPtr config)
{
    return New<TTcpBusServerProxy>(config);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NYT

