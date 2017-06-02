#include "tcp_server.h"
#include "bus.h"
#include "config.h"
#include "server.h"
#include "tcp_connection.h"
#include "tcp_dispatcher_impl.h"

#include <yt/core/concurrency/thread_affinity.h>

#include <yt/core/logging/log.h>

#include <yt/core/misc/address.h>
#include <yt/core/misc/string.h>

#include <yt/core/rpc/public.h>

#include <yt/core/ytree/convert.h>
#include <yt/core/ytree/fluent.h>

#include <errno.h>

#ifdef _unix_
    #include <netinet/tcp.h>
    #include <sys/socket.h>
    #include <sys/un.h>
    #include <sys/types.h>
    #include <sys/stat.h>
#endif

namespace NYT {
namespace NBus {

using namespace NYTree;

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
        IMessageHandlerPtr handler,
        ETcpInterfaceType interfaceType)
        : Config_(std::move(config))
        , DispatcherThread_(std::move(dispatcherThread))
        , Handler_(std::move(handler))
        , InterfaceType_(interfaceType)
    {
        VERIFY_THREAD_AFFINITY_ANY();
        YCHECK(Handler_);
 
        if (Config_->Port) {
            Logger.AddTag("ServerPort: %v", *Config_->Port);
        }
        if (Config_->UnixDomainName) {
            Logger.AddTag("UnixDomainName: %v", *Config_->UnixDomainName);
        }
        Logger.AddTag("InterfaceType: %v", InterfaceType_);
   }

    // IEventLoopObject implementation.

    virtual void SyncInitialize() override
    {
        VERIFY_THREAD_AFFINITY(EventLoop);

        // This may throw.
        OpenServerSocket();

        AcceptWatcher_.reset(new ev::io(DispatcherThread_->GetEventLoop()));
        AcceptWatcher_->set<TTcpBusServerBase, &TTcpBusServerBase::OnAccept>(this);
        AcceptWatcher_->start(ServerFD_, ev::READ);
    }

    virtual void SyncFinalize() override
    {
        VERIFY_THREAD_AFFINITY(EventLoop);

        AcceptWatcher_.reset();

        CloseServerSocket();

        {
            TGuard<TSpinLock> guard(ConnectionsLock_);
            for (auto connection : Connections_) {
                connection->Terminate(TError(
                    NRpc::EErrorCode::TransportError,
                    "Bus server terminated"));
            }
        }
    }

    void SyncCheck()
    { }

    virtual TString GetLoggingId() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();
        return Logger.GetContext();
    }

protected:
    const TTcpBusServerConfigPtr Config_;
    const TTcpDispatcherThreadPtr DispatcherThread_;
    const IMessageHandlerPtr Handler_;
    const ETcpInterfaceType InterfaceType_;

    std::unique_ptr<ev::io> AcceptWatcher_;

    int ServerSocket_ = INVALID_SOCKET;
    int ServerFD_ = INVALID_SOCKET;

    //! Protects the following members.
    TSpinLock ConnectionsLock_;
    //! The set of all currently active connections.
    yhash_set<TTcpConnectionPtr> Connections_;

    NLogging::TLogger Logger = BusLogger;

    DECLARE_THREAD_AFFINITY_SLOT(EventLoop);


    virtual void CreateServerSocket() = 0;

    virtual void InitClientSocket(SOCKET clientSocket)
    {
        if (Config_->EnableNoDelay) {
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
            TGuard<TSpinLock> guard(ConnectionsLock_);
            YCHECK(Connections_.erase(connection) == 1);
        }
    }


    void OpenServerSocket()
    {
        LOG_DEBUG("Opening server socket");

        CreateServerSocket();

        InitSocket(ServerSocket_);

        if (listen(ServerSocket_, Config_->MaxBacklogSize) == SOCKET_ERROR) {
            int error = LastSystemError();
            CloseServerSocket();
            THROW_ERROR_EXCEPTION("Failed to listen to server socket")
                << TError::FromSystem(error);
        }

        LOG_DEBUG("Server socket opened");
    }

    void CloseServerSocket()
    {
        if (ServerFD_ != INVALID_SOCKET) {
            close(ServerFD_);
            LOG_DEBUG("Server socket closed");
        }
        ServerSocket_ = INVALID_SOCKET;
        ServerFD_ = INVALID_SOCKET;
    }

    void InitSocket(SOCKET socket)
    {
#ifdef _win_
        unsigned long value = 1;
        int result = ioctlsocket(socket, FIONBIO, &value);
#else
        int flags = fcntl(socket, F_GETFL);
        int result = fcntl(socket, F_SETFL, flags | O_NONBLOCK);
#endif
        if (result != 0) {
            THROW_ERROR_EXCEPTION("Failed to enable nonblocking mode")
                << TError::FromSystem();
        }
#if defined _unix_ && !defined _linux_
        {
            int flags = fcntl(socket, F_GETFD);
            int result = fcntl(socket, F_SETFD, flags | FD_CLOEXEC);
            if (result != 0) {
                THROW_ERROR_EXCEPTION("Failed to enable close-on-exec mode")
                    << TError::FromSystem();
            }
        }
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
                    ServerSocket_,
                    clientAddress.GetSockAddr(),
                    &clientAddressLen,
                    SOCK_CLOEXEC);
#else
                clientSocket = accept(
                    ServerSocket_,
                    clientAddress.GetSockAddr(),
                    &clientAddressLen);
#endif

                if (clientSocket == INVALID_SOCKET) {
                    auto error = LastSystemError();
                    if (IsSocketError(error)) {
                        auto wrappedError = TError(
                            NRpc::EErrorCode::TransportError,
                            "Error accepting connection")
                            << TErrorAttribute("address", ToString(clientAddress))
                            << TError::FromSystem(error);
                        LOG_WARNING(wrappedError);
                    }
                    break;
                }
            }

            auto connectionId = TConnectionId::Create();

            auto connectionCount = TTcpDispatcher::TImpl::Get()->GetServerConnectionCount(InterfaceType_);
            auto connectionLimit = Config_->MaxSimultaneousConnections;
            if (connectionCount >= connectionLimit) {
                LOG_DEBUG("Connection dropped (Address: %v, ConnectionCount: %v, ConnectionLimit: %v)",
                    ToString(clientAddress, false),
                    connectionCount,
                    connectionLimit);
                close(clientSocket);
                continue;
            } else {
                LOG_DEBUG("Connection accepted (ConnectionId: %v, Address: %v, ConnectionCount: %v, ConnectionLimit: %v)",
                    connectionId,
                    ToString(clientAddress, false),
                    connectionCount,
                    connectionLimit);
            }

            InitClientSocket(clientSocket);
            InitSocket(clientSocket);

            auto dispatcherThread = TTcpDispatcher::TImpl::Get()->GetClientThread();

            auto address = ToString(clientAddress);
            auto endpointDescription = address;
            auto endpointAttributes = ConvertToAttributes(BuildYsonStringFluently()
                .BeginMap()
                    .Item("address").Value(address)
                .EndMap());

            auto connection = New<TTcpConnection>(
                Config_,
                dispatcherThread,
                EConnectionType::Server,
                InterfaceType_,
                connectionId,
                clientSocket,
                endpointDescription,
                *endpointAttributes,
                address,
                Null,
                0,
                Handler_);

            connection->SubscribeTerminated(BIND(
                &TTcpBusServerBase::OnConnectionTerminated,
                MakeWeak(this),
                connection));

            {
                TGuard<TSpinLock> guard(ConnectionsLock_);
                YCHECK(Connections_.insert(connection).second);
            }

            dispatcherThread->AsyncRegister(connection);
        }
    }


    bool IsSocketError(ssize_t result)
    {
#ifdef _WIN32
        return
        result != WSAEWOULDBLOCK &&
        result != WSAEINPROGRESS;
#else
        YCHECK(result != EINTR);
        return result != EINPROGRESS && result != EWOULDBLOCK;
#endif
    }

    void BindSocket(sockaddr* address, int size, const TString& errorMessage)
    {
        for (int attempt = 1; attempt <= Config_->BindRetryCount; ++attempt) {
            if (bind(ServerSocket_, address, size) == 0) {
                // Success.
                break;
            }

            if (attempt == Config_->BindRetryCount) {
                int errorCode = LastSystemError();
                CloseServerSocket();
                THROW_ERROR_EXCEPTION(NRpc::EErrorCode::TransportError, errorMessage)
                    << TError::FromSystem(errorCode);
            } else {
                LOG_WARNING(TError::FromSystem(), "%v, starting %v retry", errorMessage, attempt + 1);
                Sleep(Config_->BindRetryBackoff);
            }
        }
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
            std::move(handler),
            ETcpInterfaceType::Remote)
    { }

private:
    virtual void CreateServerSocket() override
    {
        int type = SOCK_STREAM;

#ifdef _linux_
        type |= SOCK_CLOEXEC;
#endif

        ServerSocket_ = socket(AF_INET6, type, IPPROTO_TCP);
        if (ServerSocket_ == INVALID_SOCKET) {
            THROW_ERROR_EXCEPTION(
                NRpc::EErrorCode::TransportError,
                "Failed to create a server socket")
                << TError::FromSystem();
        }

#ifdef _win_
        ServerFD = _open_osfhandle(ServerSocket, 0);
#else
        ServerFD_ = ServerSocket_;
#endif

        {
            int flag = 0;
            if (setsockopt(ServerSocket_, IPPROTO_IPV6, IPV6_V6ONLY, (const char*) &flag, sizeof(flag)) != 0) {
                THROW_ERROR_EXCEPTION(
                    NRpc::EErrorCode::TransportError,
                    "Failed to configure IPv6 protocol")
                    << TError::FromSystem();
            }
        }

        {
            int flag = 1;
            if (setsockopt(ServerSocket_, SOL_SOCKET, SO_REUSEADDR, (const char*) &flag, sizeof(flag)) != 0) {
                THROW_ERROR_EXCEPTION(
                    NRpc::EErrorCode::TransportError,
                    "Failed to configure socket address reuse")
                    << TError::FromSystem();
            }
        }

        {
            sockaddr_in6 serverAddress;
            memset(&serverAddress, 0, sizeof(serverAddress));
            serverAddress.sin6_family = AF_INET6;
            serverAddress.sin6_addr = in6addr_any;
            serverAddress.sin6_port = htons(Config_->Port.Get());
            BindSocket(
                (sockaddr*)&serverAddress,
                sizeof(serverAddress),
                Format("Failed to bind a server socket to port %v", Config_->Port));
        }
    }

    virtual void InitClientSocket(SOCKET clientSocket) override
    {
        TTcpBusServerBase::InitClientSocket(clientSocket);

#ifdef _linux_
        {
            int priority = Config_->Priority;
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
            std::move(handler),
            ETcpInterfaceType::Local)
    { }

private:
    virtual void CreateServerSocket() override
    {
        int type = SOCK_STREAM;

#ifdef _linux_
        type |= SOCK_CLOEXEC;
#endif

        ServerSocket_ = socket(AF_UNIX, type, 0);
        if (ServerSocket_ == INVALID_SOCKET) {
            THROW_ERROR_EXCEPTION(
                NRpc::EErrorCode::TransportError,
                "Failed to create a local server socket")
                << TError::FromSystem();
        }

        ServerFD_ = ServerSocket_;

        {
            TNetworkAddress netAddress;
            if (Config_->UnixDomainName) {
                netAddress = GetUnixDomainAddress(Config_->UnixDomainName.Get());
            } else {
                netAddress = GetLocalBusAddress(Config_->Port.Get());
            }
            BindSocket(
                netAddress.GetSockAddr(),
                netAddress.GetLength(),
                "Failed to bind a local server socket");
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
        : Config_(std::move(config))
        , DispatcherThread_(TTcpDispatcher::TImpl::Get()->GetServerThread())
    {
        YCHECK(Config_);
    }

    ~TTcpBusServerProxy()
    {
        Stop();
    }

    virtual void Start(IMessageHandlerPtr handler)
    {
        TGuard<TSpinLock> guard(SpinLock_);

        YCHECK(!Running_);

        auto server = New<TServer>(
            Config_,
            DispatcherThread_,
            std::move(handler));

        DispatcherThread_->AsyncRegister(server)
            .Get()
            .ThrowOnError();

        Server_ = server;
        Running_ = true;
    }

    virtual void Stop()
    {
        TGuard<TSpinLock> guard(SpinLock_);

        if (!Running_) {
            return;
        }

        auto error = DispatcherThread_->AsyncUnregister(Server_).Get();
        // Shutdown will hopefully never fail.
        YCHECK(error.IsOK());

        Server_.Reset();
        Running_ = false;
    }

private:
    const TTcpBusServerConfigPtr Config_;

    TSpinLock SpinLock_;
    TTcpDispatcherThreadPtr DispatcherThread_;
    volatile bool Running_ = false;
    TIntrusivePtr<TServer> Server_;

};

////////////////////////////////////////////////////////////////////////////////

class TCompositeBusServer
    : public IBusServer
{
public:
    explicit TCompositeBusServer(const std::vector<IBusServerPtr>& servers)
        : Servers_(servers)
    { }

    virtual void Start(IMessageHandlerPtr handler) override
    {
        for (auto server : Servers_) {
            server->Start(handler);
        }
    }

    virtual void Stop() override
    {
        for (auto server : Servers_) {
            server->Stop();
        }
    }

private:
    const std::vector<IBusServerPtr> Servers_;

};

IBusServerPtr CreateTcpBusServer(TTcpBusServerConfigPtr config)
{
    std::vector<IBusServerPtr> servers;
    if (config->Port) {
        servers.push_back(New< TTcpBusServerProxy<TRemoteTcpBusServer> >(config));
    }
#ifdef _linux_
    // Abstract unix sockets are supported only on Linux.
    servers.push_back(New< TTcpBusServerProxy<TLocalTcpBusServer> >(config));
#endif
    return New<TCompositeBusServer>(servers);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NYT

