#include "server.h"

#include "config.h"
#include "private.h"

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/concurrency/poller.h>
#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/net/address.h>
#include <yt/yt/core/net/connection.h>
#include <yt/yt/core/net/listener.h>

namespace NYT::NZookeeperProxy {

using namespace NConcurrency;
using namespace NNet;
using namespace NThreading;
using namespace NZookeeper;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ZookeeperProxyLogger;

////////////////////////////////////////////////////////////////////////////////

class TServer
    : public IServer
{
public:
    TServer(
        TZookeeperServerConfigPtr config,
        IPollerPtr poller,
        IPollerPtr acceptor,
        IListenerPtr listener)
        : Config_(std::move(config))
        , Poller_(std::move(poller))
        , Acceptor_(std::move(acceptor))
        , Listener_(std::move(listener))
    {
        RegisterStartSessionHandler(BIND(&TServer::DoStartSession));
    }

    void Start() override
    {
        YT_LOG_INFO("Starting Zookeeper server (Address: %v)",
            Listener_->GetAddress());

        AsyncAcceptConnection();
    }

    void RegisterHandler(ERequestType requestType, THandler handler) override
    {
        YT_VERIFY(!Handlers_[requestType]);
        Handlers_[requestType] = std::move(handler);
    }

    void RegisterStartSessionHandler(TStartSessionHandler handler) override
    {
        YT_VERIFY(!StartSessionHandler_);
        StartSessionHandler_ = std::move(handler);
    }

private:
    const TZookeeperServerConfigPtr Config_;

    const IPollerPtr Poller_;
    const IPollerPtr Acceptor_;
    const IListenerPtr Listener_;

    struct TConnectionState final
    {
        explicit TConnectionState(IConnectionPtr connection)
            : Connection(std::move(connection))
        { }

        IConnectionPtr Connection;

        //! First request in session is always a start session request.
        //! This flag indicates whether at least one request was processed
        //! in connection.
        bool FirstRequest = true;
    };
    using TConnectionStatePtr = TIntrusivePtr<TConnectionState>;

    THashMap<TConnectionId, TConnectionStatePtr> Connections_;
    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, ConnectionMapLock_);

    TEnumIndexedArray<ERequestType, THandler> Handlers_;
    TStartSessionHandler StartSessionHandler_;

    using TStartSessionHandler = TTypedHandler<TReqStartSession, TRspStartSession>;

    void AsyncAcceptConnection()
    {
        Listener_->Accept().Subscribe(
            BIND(&TServer::OnConnectionAccepted, MakeWeak(this))
                .Via(Acceptor_->GetInvoker()));
    }

    void OnConnectionAccepted(const TErrorOr<NNet::IConnectionPtr>& connectionOrError)
    {
        AsyncAcceptConnection();

        if (!connectionOrError.IsOK()) {
            YT_LOG_INFO(connectionOrError, "Error accepting connection");
            return;
        }

        const auto& connection = connectionOrError.Value();
        auto zookeeperConnection = CreateConnection(
            Config_,
            connection,
            Poller_->GetInvoker(),
            BIND(&TServer::OnRequest, MakeWeak(this)),
            BIND(&TServer::OnConnectionFailure, MakeWeak(this)));
        zookeeperConnection->Start();

        YT_LOG_DEBUG("Connection accepted "
            "(ConnectionId: %v, LocalAddress: %v, RemoteAddress: %v)",
            zookeeperConnection->GetConnectionId(),
            connection->LocalAddress(),
            connection->RemoteAddress());

        auto connectionState = New<TConnectionState>(zookeeperConnection);
        EmplaceOrCrash(
            Connections_,
            zookeeperConnection->GetConnectionId(),
            std::move(connectionState));
    }

    void OnRequest(IConnectionPtr connection, TMessage request)
    {
        try {
            GuardedOnRequest(connection, std::move(request));
        } catch (const std::exception& ex) {
            YT_LOG_DEBUG(ex, "Failed to process request "
                "(ConnectionId: %v, SessionId: %v)",
                connection->GetConnectionId(),
                connection->GetSessionId());

            OnConnectionFailure(connection, TError(ex));
        }
    }

    void GuardedOnRequest(IConnectionPtr connection, TMessage request)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        bool isRegularRequest;
        {
            auto guard = ReaderGuard(ConnectionMapLock_);
            auto connectionIt = Connections_.find(connection->GetConnectionId());
            if (connectionIt == Connections_.end()) {
                THROW_ERROR_EXCEPTION("Connection %v is not registered",
                    connection->GetConnectionId());
            }
            const auto& connectionState = connectionIt->second;
            if (connectionState->FirstRequest) {
                isRegularRequest = false;
                connectionState->FirstRequest = false;
            } else {
                isRegularRequest = true;
            }
        }

        auto response = isRegularRequest
            ? ProcessRegularRequest(request)
            : ProcessStartSessionRequest(connection, request);

        YT_UNUSED_FUTURE(connection->PostMessage(response)
            .Apply(BIND([=, this, this_ = MakeStrong(this)] (const TError& error) {
                if (!error.IsOK()) {
                    OnConnectionFailure(connection, error);
                }
            })));
    }

    TMessage ProcessRegularRequest(const TMessage& /*request*/)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        YT_ABORT();
    }

    TMessage ProcessStartSessionRequest(
        const IConnectionPtr& connection,
        const TMessage& request)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        if (request.size() != 1) {
            THROW_ERROR_EXCEPTION("Incoming message has %v, expected 1",
                request.size());
        }

        TReqStartSession req;
        auto protocolReader = CreateZookeeperProtocolReader(request[0]);
        req.Deserialize(protocolReader.get());

        auto rsp = StartSessionHandler_(req);

        AttachSessionToConnection(connection, rsp.SessionId);

        auto protocolWriter = CreateZookeeperProtocolWriter();
        rsp.Serialize(protocolWriter.get());

        return TSharedRefArray(protocolWriter->Finish());
    }

    void OnConnectionFailure(IConnectionPtr connection, TError error)
    {
        auto connectionId = connection->GetConnectionId();

        YT_LOG_INFO(error, "Zookeeper server observed connection failure, termintating connection",
            "(ConnectionId: %v)",
            connectionId);

        if (UnregisterConnection(connection->GetConnectionId())) {
            // TODO(max42): switch to Subscribe.
            YT_UNUSED_FUTURE(connection->Terminate()
                .Apply(BIND([=] (const TError& error) {
                    YT_LOG_WARNING(error, "Failed to terminate connection");
                })));
        }
    }

    bool UnregisterConnection(TConnectionId connectionId)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto guard = WriterGuard(ConnectionMapLock_);
        if (Connections_.erase(connectionId)) {
            YT_LOG_DEBUG("Connection unregistered (ConnectionId: %v)",
                connectionId);
            return true;
        }

        return false;
    }

    void AttachSessionToConnection(
        const IConnectionPtr& connection,
        TSessionId sessionId)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        connection->SetSessionId(sessionId);

        YT_LOG_DEBUG("Session attached to connection "
            "(ConnectionId: %v, SessionId: %v)",
            connection->GetConnectionId(),
            sessionId);
    }

    static TRspStartSession DoStartSession(const TReqStartSession& req)
    {
        TRspStartSession rsp;
        rsp.SessionId = 123456;
        rsp.ProtocolVersion = req.ProtocolVersion;
        rsp.Timeout = req.Timeout;

        return rsp;
    }
};

////////////////////////////////////////////////////////////////////////////////

IServerPtr CreateServer(
    TZookeeperServerConfigPtr config,
    IPollerPtr poller,
    IPollerPtr acceptor)
{
    auto address = TNetworkAddress::CreateIPv6Any(config->Port);
    for (int retryIndex = 0;; ++retryIndex) {
        try {
            auto listener = CreateListener(address, poller, acceptor, config->MaxBacklogSize);
            return New<TServer>(config, poller, acceptor, listener);
        } catch (const std::exception& ex) {
            if (retryIndex + 1 == config->BindRetryCount) {
                throw;
            } else {
                YT_LOG_ERROR(ex, "Zookeeper server bind failed");
                Sleep(config->BindRetryBackoff);
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NZookeeperProxy
