#include "tcp_server.h"
#include "bus.h"
#include "config.h"
#include "server.h"
#include "tcp_connection.h"
#include "tcp_dispatcher_impl.h"

#include <yt/core/logging/log.h>

#include <yt/core/misc/address.h>
#include <yt/core/misc/socket.h>
#include <yt/core/misc/string.h>

#include <yt/core/ytree/convert.h>
#include <yt/core/ytree/fluent.h>

#include <yt/core/concurrency/periodic_executor.h>
#include <yt/core/concurrency/rw_spinlock.h>

#include <cerrno>

namespace NYT {
namespace NBus {

using namespace NYTree;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const auto& Profiler = BusProfiler;

static constexpr auto CheckPeriod = TDuration::Seconds(15);

static NProfiling::TAggregateCounter AcceptTime("/accept_time");

////////////////////////////////////////////////////////////////////////////////

class TTcpBusServerBase
    : public IPollable
{
public:
    TTcpBusServerBase(
        TTcpBusServerConfigPtr config,
        IPollerPtr poller,
        IMessageHandlerPtr handler,
        ETcpInterfaceType interfaceType)
        : Config_(std::move(config))
        , Poller_(std::move(poller))
        , Handler_(std::move(handler))
        , InterfaceType_(interfaceType)
        , CheckExecutor_(New<TPeriodicExecutor>(
            GetSyncInvoker(),
            BIND(&TTcpBusServerBase::OnCheck, MakeWeak(this)),
            CheckPeriod))
    {
        YCHECK(Config_);
        YCHECK(Poller_);
        YCHECK(Handler_);

        if (Config_->Port) {
            Logger.AddTag("ServerPort: %v", *Config_->Port);
        }
        if (Config_->UnixDomainName) {
            Logger.AddTag("UnixDomainName: %v", *Config_->UnixDomainName);
        }
        Logger.AddTag("InterfaceType: %v", InterfaceType_);

        CheckExecutor_->Start();
    }

    void Start()
    {
        OpenServerSocket();
        Poller_->Register(this);
        RearmPoller();
    }

    TFuture<void> Stop()
    {
        UnarmPoller();
        return Poller_->Unregister(this);
    }

    // IPollable implementation.
    virtual const TString& GetLoggingId() const override
    {
        return Logger.GetContext();
    }

    virtual void OnEvent(EPollControl /*control*/) override
    {
        OnAccept();
        RearmPoller();
    }

    virtual void OnShutdown() override
    {
        {
            auto guard = Guard(ControlSpinLock_);
            CloseServerSocket();
        }

        decltype(Connections_) connections;
        {
            TWriterGuard guard(ConnectionsSpinLock_);
            std::swap(connections, Connections_);
        }

        for (const auto& connection : connections) {
            connection->Terminate(TError(
                NRpc::EErrorCode::TransportError,
                "Bus server terminated"));
        }
    }

protected:
    const TTcpBusServerConfigPtr Config_;
    const IPollerPtr Poller_;
    const IMessageHandlerPtr Handler_;
    const ETcpInterfaceType InterfaceType_;

    const TPeriodicExecutorPtr CheckExecutor_;

    TSpinLock ControlSpinLock_;
    int ServerSocket_ = INVALID_SOCKET;

    TReaderWriterSpinLock ConnectionsSpinLock_;
    THashSet<TTcpConnectionPtr> Connections_;

    NLogging::TLogger Logger = BusLogger;


    virtual void CreateServerSocket() = 0;

    virtual void InitClientSocket(SOCKET clientSocket)
    {
        if (Config_->EnableNoDelay) {
            SetSocketNoDelay(clientSocket);
        }

        SetSocketKeepAlive(clientSocket);
    }


    void OnConnectionTerminated(const TTcpConnectionPtr& connection, const TError& /*error*/)
    {
        TWriterGuard guard(ConnectionsSpinLock_);
        // NB: Connection could be missing, see OnShutdown.
        Connections_.erase(connection);
    }

    void OpenServerSocket()
    {
        auto guard = Guard(ControlSpinLock_);

        LOG_DEBUG("Opening server socket");

        CreateServerSocket();

        try {
            ListenSocket(ServerSocket_, Config_->MaxBacklogSize);
        } catch (const std::exception& ex) {
            CloseServerSocket();
            throw;
        }

        LOG_DEBUG("Server socket opened");
    }

    void CloseServerSocket()
    {
        if (ServerSocket_ != INVALID_SOCKET) {
            close(ServerSocket_);
            ServerSocket_ = INVALID_SOCKET;
            LOG_DEBUG("Server socket closed");
        }
    }

    void OnAccept()
    {
        while (true) {
            TNetworkAddress clientAddress;
            SOCKET clientSocket;
            PROFILE_AGGREGATED_TIMING (AcceptTime) {
                try {
                    clientSocket = AcceptSocket(ServerSocket_, &clientAddress);
                    if (clientSocket == INVALID_SOCKET) {
                        break;
                    }
                } catch (const std::exception& ex) {
                    LOG_WARNING(ex, "Error accepting client connection");
                    break;
                }
            }

            auto connectionId = TConnectionId::Create();

            auto connectionCount = TTcpDispatcher::TImpl::Get()->GetCounters(InterfaceType_)->ServerConnections.load();
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

            auto address = ToString(clientAddress);
            auto endpointDescription = address;
            auto endpointAttributes = ConvertToAttributes(BuildYsonStringFluently()
                .BeginMap()
                    .Item("address").Value(address)
                .EndMap());

            auto connection = New<TTcpConnection>(
                Config_,
                EConnectionType::Server,
                InterfaceType_,
                connectionId,
                clientSocket,
                endpointDescription,
                *endpointAttributes,
                address,
                Null,
                Handler_,
                TTcpDispatcher::TImpl::Get()->GetXferPoller());

            {
                TWriterGuard guard(ConnectionsSpinLock_);
                YCHECK(Connections_.insert(connection).second);
            }

            connection->SubscribeTerminated(BIND(
                &TTcpBusServerBase::OnConnectionTerminated,
                MakeWeak(this),
                connection));

            connection->Start();
        }
    }

    void BindSocket(const TNetworkAddress& address, const TString& errorMessage)
    {
        for (int attempt = 1; attempt <= Config_->BindRetryCount; ++attempt) {
            try {
                ::NYT::BindSocket(ServerSocket_, address);
                return;
            } catch (const TErrorException& ex) {
                if (attempt == Config_->BindRetryCount) {
                    CloseServerSocket();

                    THROW_ERROR_EXCEPTION(NRpc::EErrorCode::TransportError, errorMessage)
                        << ex;
                } else {
                    LOG_WARNING(ex, "Error binding socket, starting %v retry", attempt + 1);
                    Sleep(Config_->BindRetryBackoff);
                }
            }
        }
    }

    void UnarmPoller()
    {
        auto guard = Guard(ControlSpinLock_);
        if (ServerSocket_ == INVALID_SOCKET) {
            return;
        }
        Poller_->Unarm(ServerSocket_);
    }

    void RearmPoller()
    {
        auto guard = Guard(ControlSpinLock_);
        if (ServerSocket_ == INVALID_SOCKET) {
            return;
        }
        Poller_->Arm(ServerSocket_, this, EPollControl::Read);
    }

    void OnCheck()
    {
        TReaderGuard guard(ConnectionsSpinLock_);
        for (const auto& connection : Connections_) {
            connection->Check();
        }
    }
};

class TRemoteTcpBusServer
    : public TTcpBusServerBase
{
public:
    TRemoteTcpBusServer(
        TTcpBusServerConfigPtr config,
        IPollerPtr poller,
        IMessageHandlerPtr handler)
        : TTcpBusServerBase(
            std::move(config),
            std::move(poller),
            std::move(handler),
            ETcpInterfaceType::Remote)
    { }

private:
    virtual void CreateServerSocket() override
    {
        ServerSocket_ = CreateTcpServerSocket();

        TNetworkAddress serverAddress = TNetworkAddress::CreateIPv6Any(Config_->Port.Get());
        BindSocket(serverAddress, Format("Failed to bind a server socket to port %v", Config_->Port));
    }

    virtual void InitClientSocket(SOCKET clientSocket) override
    {
        TTcpBusServerBase::InitClientSocket(clientSocket);

        SetSocketPriority(clientSocket, Config_->Priority);
    }
};

class TLocalTcpBusServer
    : public TTcpBusServerBase
{
public:
    TLocalTcpBusServer(
        TTcpBusServerConfigPtr config,
        IPollerPtr poller,
        IMessageHandlerPtr handler)
        : TTcpBusServerBase(
            std::move(config),
            std::move(poller),
            std::move(handler),
            ETcpInterfaceType::Local)
    { }

private:
    virtual void CreateServerSocket() override
    {
        ServerSocket_ = CreateUnixServerSocket();

        {
            TNetworkAddress netAddress;
            if (Config_->UnixDomainName) {
                netAddress = TNetworkAddress::CreateUnixDomainAddress(Config_->UnixDomainName.Get());
            } else {
                netAddress = GetLocalBusAddress(Config_->Port.Get());
            }
            BindSocket(netAddress, "Failed to bind a local server socket");
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
    {
        YCHECK(Config_);
    }

    ~TTcpBusServerProxy()
    {
        Stop();
    }

    virtual void Start(IMessageHandlerPtr handler)
    {
        auto server = New<TServer>(
            Config_,
            TTcpDispatcher::TImpl::Get()->GetAcceptorPoller(),
            std::move(handler));

        {
            auto guard = Guard(SpinLock_);
            YCHECK(!Server_);
            Server_ = server;
        }

        server->Start();
    }

    virtual TFuture<void> Stop()
    {
        TIntrusivePtr<TServer> server;
        {
            auto guard = Guard(SpinLock_);
            if (!Server_) {
                return VoidFuture;
            }
            std::swap(server, Server_);
        }
        return server->Stop();
    }

private:
    const TTcpBusServerConfigPtr Config_;

    TSpinLock SpinLock_;
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
        for (const auto& server : Servers_) {
            server->Start(handler);
        }
    }

    virtual TFuture<void> Stop() override
    {
        std::vector<TFuture<void>> asyncResults;
        for (const auto& server : Servers_) {
            asyncResults.push_back(server->Stop());
        }
        return Combine(asyncResults);
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
    servers.push_back(New<TTcpBusServerProxy<TLocalTcpBusServer>>(config));
#endif
    return New<TCompositeBusServer>(servers);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NYT

