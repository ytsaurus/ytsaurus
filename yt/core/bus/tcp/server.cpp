#include "server.h"
#include "config.h"
#include "server.h"
#include "connection.h"
#include "dispatcher_impl.h"

#include <yt/core/bus/bus.h>
#include <yt/core/bus/server.h>

#include <yt/core/logging/log.h>

#include <yt/core/net/address.h>

#include <yt/core/net/socket.h>
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
using namespace NNet;

////////////////////////////////////////////////////////////////////////////////

static const auto& Profiler = BusProfiler;

static constexpr auto CheckPeriod = TDuration::Seconds(15);

static NProfiling::TAggregateGauge AcceptTime("/accept_time");

////////////////////////////////////////////////////////////////////////////////

class TTcpBusServerBase
    : public IPollable
{
public:
    TTcpBusServerBase(
        TTcpBusServerConfigPtr config,
        IPollerPtr poller,
        IMessageHandlerPtr handler)
        : Config_(std::move(config))
        , Poller_(std::move(poller))
        , Handler_(std::move(handler))
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

        for (const auto& network : Config_->Networks) {
            for (const auto& prefix : network.second) {
                Networks_.emplace_back(prefix, network.first);
            }
        }

        // Putting more specific networks first in match order.
        std::sort(Networks_.begin(), Networks_.end(), [] (auto&& lhs, auto&& rhs) {
            return lhs.first.GetMaskSize() > rhs.first.GetMaskSize();
        });

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

    const TPeriodicExecutorPtr CheckExecutor_;

    TSpinLock ControlSpinLock_;
    int ServerSocket_ = INVALID_SOCKET;

    TReaderWriterSpinLock ConnectionsSpinLock_;
    THashSet<TTcpConnectionPtr> Connections_;

    NLogging::TLogger Logger = BusLogger;

    std::vector<std::pair<TIP6Network, TString>> Networks_;

    virtual void CreateServerSocket() = 0;

    virtual void InitClientSocket(SOCKET clientSocket) = 0;

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

            auto clientNetwork = GetNetworkNameForAddress(clientAddress);

            auto connectionId = TConnectionId::Create();

            auto connectionCount = TTcpDispatcher::TImpl::Get()->GetCounters(clientNetwork)->ServerConnections.load();
            auto connectionLimit = Config_->MaxSimultaneousConnections;
            if (connectionCount >= connectionLimit) {
                LOG_DEBUG("Connection dropped (Address: %v, ConnectionCount: %v, ConnectionLimit: %v)",
                    ToString(clientAddress, false),
                    connectionCount,
                    connectionLimit);
                close(clientSocket);
                continue;
            } else {
                LOG_DEBUG("Connection accepted (ConnectionId: %v, Address: %v, Network: %v, ConnectionCount: %v, ConnectionLimit: %v)",
                    connectionId,
                    ToString(clientAddress, false),
                    clientNetwork,
                    connectionCount,
                    connectionLimit);
            }

            InitClientSocket(clientSocket);

            auto address = ToString(clientAddress);
            auto endpointDescription = address;
            auto endpointAttributes = ConvertToAttributes(BuildYsonStringFluently()
                .BeginMap()
                    .Item("address").Value(address)
                    .Item("network").Value(clientNetwork)
                .EndMap());

            auto connection = New<TTcpConnection>(
                Config_,
                EConnectionType::Server,
                clientNetwork,
                connectionId,
                clientSocket,
                endpointDescription,
                *endpointAttributes,
                clientAddress,
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
                NNet::BindSocket(ServerSocket_, address);
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

    const TString& GetNetworkNameForAddress(const TNetworkAddress& address)
    {
        if (address.IsUnix()) {
            return LocalNetworkName;
        }

        if (!address.IsIP6()) {
            return DefaultNetworkName;
        }

        auto ip6Address = address.ToIP6Address();
        for (const auto& network : Networks_) {
            if (network.first.Contains(ip6Address)) {
                return network.second;
            }
        }

        return DefaultNetworkName;
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
            std::move(handler))
    { }

private:
    virtual void CreateServerSocket() override
    {
        ServerSocket_ = CreateTcpServerSocket();

        auto serverAddress = TNetworkAddress::CreateIPv6Any(Config_->Port.Get());
        BindSocket(serverAddress, Format("Failed to bind a server socket to port %v", Config_->Port));
    }

    virtual void InitClientSocket(SOCKET clientSocket) override
    {
        if (Config_->EnableNoDelay) {
            if (!TrySetSocketNoDelay(clientSocket)) {
                LOG_DEBUG("Failed to set socket no delay option");
            }
        }

        if (!TrySetSocketKeepAlive(clientSocket)) {
            LOG_DEBUG("Failed to set socket keep alive option");
        }
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
            std::move(handler))
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

    virtual void InitClientSocket(SOCKET clientSocket) override
    { }
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

