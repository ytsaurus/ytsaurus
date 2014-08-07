#include "stdafx.h"
#include "tcp_server.h"
#include "tcp_dispatcher_impl.h"
#include "server.h"
#include "config.h"
#include "bus.h"
#include "tcp_connection.h"

#include <core/misc/address.h>

#include <core/concurrency/thread_affinity.h>

#include <core/rpc/public.h>

#include <core/logging/log.h>

#include <errno.h>

#ifndef _win_
    #include <netinet/tcp.h>
    #include <sys/socket.h>
    #include <sys/un.h>
    #include <sys/types.h>
    #include <sys/stat.h>
#endif

namespace NYT {
namespace NBus {

////////////////////////////////////////////////////////////////////////////////

static auto& Profiler = BusProfiler;

static NProfiling::TAggregateCounter AcceptTime("/accept_time");

////////////////////////////////////////////////////////////////////////////////

class TTcpBusServerBase
    : public IEventLoopObject
{
public:
    TTcpBusServerBase(
        TTcpBusServerConfigPtr config,
        TTcpDispatcherThreadPtr dispatcherThread,
        IMessageHandlerPtr handler)
        : Config(std::move(config))
        , DispatcherThread(std::move(dispatcherThread))
        , Handler(std::move(handler))
        , Logger(BusLogger)
        , ServerSocket(INVALID_SOCKET)
        , ServerFd(INVALID_SOCKET)
    {
        VERIFY_THREAD_AFFINITY_ANY();
        YCHECK(Handler);
    }

    // IEventLoopObject implementation.

    virtual void SyncInitialize() override
    {
        VERIFY_THREAD_AFFINITY(EventLoop);

        // This may throw.
        OpenServerSocket();

        AcceptWatcher.reset(new ev::io(DispatcherThread->GetEventLoop()));
        AcceptWatcher->set<TTcpBusServerBase, &TTcpBusServerBase::OnAccept>(this);
        AcceptWatcher->start(ServerFd, ev::READ);
    }

    virtual void SyncFinalize() override
    {
        VERIFY_THREAD_AFFINITY(EventLoop);

        AcceptWatcher.reset();

        CloseServerSocket();

        {
            TGuard<TSpinLock> guard(SpinLock);
            for (auto connection : Connections) {
                connection->Terminate(TError(
                    NRpc::EErrorCode::TransportError,
                    "Bus server terminated"));
            }
        }
    }

    virtual Stroka GetLoggingId() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return Format("Port: %v", Config->Port);
    }

protected:
    TTcpBusServerConfigPtr Config;
    TTcpDispatcherThreadPtr DispatcherThread;
    IMessageHandlerPtr Handler;

    NLog::TLogger Logger;

    std::unique_ptr<ev::io> AcceptWatcher;

    int ServerSocket;
    int ServerFd;

    //! Protects the following members.
    TSpinLock SpinLock;
    //! The set of all currently active connections.
    yhash_set<TTcpConnectionPtr> Connections;

    DECLARE_THREAD_AFFINITY_SLOT(EventLoop);


    virtual void CreateServerSocket() = 0;

    virtual ETcpInterfaceType GetInterfaceType() = 0;

    virtual void InitClientSocket(SOCKET clientSocket)
    {
        if (Config->EnableNoDelay) {
            int value = 1;
            setsockopt(clientSocket, IPPROTO_TCP, TCP_NODELAY, (const char*) &value, sizeof(value));
        }
        {
            int value = 1;
            setsockopt(clientSocket, SOL_SOCKET, SO_KEEPALIVE, (const char*) &value, sizeof(value));
        }
    }


    void OnConnectionTerminated(TTcpConnectionPtr connection, TError /*error*/)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        {
            TGuard<TSpinLock> guard(SpinLock);
            YCHECK(Connections.erase(connection) == 1);
        }
    }


    void OpenServerSocket()
    {
        LOG_DEBUG("Opening server socket");

        CreateServerSocket();

        InitSocket(ServerSocket);

        if (listen(ServerSocket, Config->MaxBacklogSize) == SOCKET_ERROR) {
            int error = LastSystemError();
            CloseServerSocket();
            THROW_ERROR_EXCEPTION("Failed to listen to server socket")
                << TError::FromSystem(error);
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
        unsigned long value = 1;
        ioctlsocket(socket, FIONBIO, &value);
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
            socklen_t clientAddressLen = clientAddress.GetLength();
            SOCKET clientSocket;
            PROFILE_AGGREGATED_TIMING (AcceptTime) {
#ifdef _linux_
                clientSocket = accept4(
                    ServerSocket,
                    clientAddress.GetSockAddr(),
                    &clientAddressLen,
                    SOCK_CLOEXEC);
#else
                clientSocket = accept(
                    ServerSocket,
                    clientAddress.GetSockAddr(),
                    &clientAddressLen);
#endif
            }

            if (clientSocket == INVALID_SOCKET) {
                auto error = LastSystemError();
                if (IsSocketError(error)) {
                    auto wrappedError = TError(
                        NRpc::EErrorCode::TransportError,
                        "Error accepting connection")
                        << TErrorAttribute("address", ToString(clientAddress, false))
                        << TError::FromSystem(error);
                    LOG_WARNING(wrappedError);
                }
                break;
            }

            LOG_DEBUG("Connection accepted");

            InitClientSocket(clientSocket);
            InitSocket(clientSocket);

            auto dispatcherThread = TTcpDispatcher::TImpl::Get()->AllocateThread();
            
            auto connection = New<TTcpConnection>(
                Config,
                dispatcherThread,
                EConnectionType::Server,
                GetInterfaceType(),
                TConnectionId::Create(),
                clientSocket,
                ToString(clientAddress, false),
                0,
                Handler);

            connection->SubscribeTerminated(BIND(
                &TTcpBusServerBase::OnConnectionTerminated,
                MakeWeak(this),
                connection));

            {
                TGuard<TSpinLock> guard(SpinLock);
                YCHECK(Connections.insert(connection).second);
            }

            dispatcherThread->AsyncRegister(connection);
        }
    }


    bool IsSocketError(ssize_t result)
    {
#ifdef _WIN32
        return result != WSAEINPROGRESS && result != WSAEWOULDBLOCK;
#else
        YCHECK(result != EINTR);
        return result != EINPROGRESS && result != EWOULDBLOCK;
#endif
    }

};

class TRemoteTcpBusServer
    : public TTcpBusServerBase
{
public:
    TRemoteTcpBusServer(
        TTcpBusServerConfigPtr config,
        TTcpDispatcherThreadPtr dispatcherThread,
        IMessageHandlerPtr handler)
        : TTcpBusServerBase(
            std::move(config),
            std::move(dispatcherThread),
            std::move(handler))
    {
        Logger.AddTag("Port: %v", Config->Port);
    }


    // IEventLoopObject implementation.

    virtual Stroka GetLoggingId() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return Format("Port: %v", Config->Port);
    }

private:
    virtual ETcpInterfaceType GetInterfaceType() override
    {
        return ETcpInterfaceType::Remote;
    }

    virtual void CreateServerSocket() override
    {
        int type = SOCK_STREAM;

#ifdef _linux_
        type |= SOCK_CLOEXEC;
#endif

        ServerSocket = socket(AF_INET6, type, IPPROTO_TCP);
        if (ServerSocket == INVALID_SOCKET) {
            THROW_ERROR_EXCEPTION("Failed to create a server socket")
                << TError::FromSystem();
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
                CloseServerSocket();
                THROW_ERROR_EXCEPTION("Failed to bind a server socket to port %v", Config->Port)
                    << TError::FromSystem();
            }
        }
    }

    virtual void InitClientSocket(SOCKET clientSocket) override
    {
        TTcpBusServerBase::InitClientSocket(clientSocket);

#if !defined(_win_) && !defined(__APPLE__)
        {
            int priority = Config->Priority;
            setsockopt(clientSocket, SOL_SOCKET, SO_PRIORITY, (const char*) &priority, sizeof(priority));
        }
#endif
    }
};

class TLocalTcpBusServer
    : public TTcpBusServerBase
{
public:
    TLocalTcpBusServer(
        TTcpBusServerConfigPtr config,
        TTcpDispatcherThreadPtr dispatcherThread,
        IMessageHandlerPtr handler)
        : TTcpBusServerBase(
            std::move(config),
            std::move(dispatcherThread),
            std::move(handler))
    {
        Logger.AddTag("LocalPort: %v", Config->Port);
    }


    // IEventLoopObject implementation.

    virtual Stroka GetLoggingId() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return Format("LocalPort: %v", Config->Port);
    }

private:
    Stroka Path;

    virtual ETcpInterfaceType GetInterfaceType() override
    {
        return ETcpInterfaceType::Local;
    }
    
    virtual void CreateServerSocket() override
    {
        int type = SOCK_STREAM;

#ifdef _linux_
        type |= SOCK_CLOEXEC;
#endif

        ServerSocket = socket(AF_UNIX, type, 0);
        if (ServerSocket == INVALID_SOCKET) {
            THROW_ERROR_EXCEPTION("Failed to create a local server socket")
                << TError::FromSystem();
        }

        ServerFd = ServerSocket;

        {
            auto netAddress = GetLocalBusAddress(Config->Port);
            if (bind(ServerSocket, netAddress.GetSockAddr(), netAddress.GetLength()) != 0) {
                CloseServerSocket();
                THROW_ERROR_EXCEPTION("Failed to bind a local server socket")
                    << TError::FromSystem();
            }
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

//! A lightweight proxy controlling the lifetime of a TCP bus server.
/*!
 *  When the last strong reference vanishes, it unregisters the underlying
 *  server instance.
 */
template <class TServer>
class TTcpBusServerProxy
    : public IBusServer
{
public:
    explicit TTcpBusServerProxy(TTcpBusServerConfigPtr config)
        : Config(std::move(config))
        , DispatcherThread(TTcpDispatcher::TImpl::Get()->AllocateThread())
        , Running(false)
    {
        YCHECK(Config);
    }

    ~TTcpBusServerProxy()
    {
        Stop();
    }

    virtual void Start(IMessageHandlerPtr handler)
    {
        TGuard<TSpinLock> guard(SpinLock);

        YCHECK(!Running);

        auto server = New<TServer>(
            Config,
            DispatcherThread,
            std::move(handler));

        auto error = DispatcherThread->AsyncRegister(server).Get();
        if (!error.IsOK()) {
            THROW_ERROR error;
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

        auto error = DispatcherThread->AsyncUnregister(Server).Get();
        // Shutdown will hopefully never fail.
        YCHECK(error.IsOK());

        Server.Reset();
        Running = false;
    }

private:
    TTcpBusServerConfigPtr Config;

    TSpinLock SpinLock;
    TTcpDispatcherThreadPtr DispatcherThread;
    volatile bool Running;
    TIntrusivePtr<TServer> Server;

};

////////////////////////////////////////////////////////////////////////////////

class TCompositeBusServer
    : public IBusServer
{
public:
    explicit TCompositeBusServer(const std::vector<IBusServerPtr>& servers)
        : Servers(servers)
    { }

    virtual void Start(IMessageHandlerPtr handler) override
    {
        for (auto server : Servers) {
            server->Start(handler);
        }
    }

    virtual void Stop() override
    {
        for (auto server : Servers) {
            server->Stop();
        }
    }

private:
    std::vector<IBusServerPtr> Servers;

};

IBusServerPtr CreateTcpBusServer(TTcpBusServerConfigPtr config)
{
    std::vector<IBusServerPtr> servers;
    servers.push_back(New< TTcpBusServerProxy<TRemoteTcpBusServer> >(config));
#ifdef _linux_
    servers.push_back(New< TTcpBusServerProxy<TLocalTcpBusServer> >(config));
#endif
    return New<TCompositeBusServer>(servers);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NYT

