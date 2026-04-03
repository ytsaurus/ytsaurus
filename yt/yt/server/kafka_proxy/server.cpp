#include "server.h"

#include "config.h"
#include "connection.h"
#include "private.h"
#include "request_handler.h"

#include <yt/yt/server/kafka_proxy/records/kafka_message.record.h>

#include <yt/yt/ytlib/hive/cluster_directory.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/client_cache.h>
#include <yt/yt/ytlib/api/native/transaction.h>

#include <yt/yt/ytlib/security_client/permission_cache.h>

#include <yt/yt/client/kafka/protocol.h>
#include <yt/yt/client/kafka/error.h>

#include <yt/yt/client/queue_client/consumer_client.h>

#include <yt/yt/client/table_client/record_helpers.h>

#include <yt/yt/client/tablet_client/table_mount_cache.h>

#include <yt/yt/client/transaction_client/helpers.h>

#include <yt/yt/library/auth_server/authentication_manager.h>
#include <yt/yt/library/auth_server/credentials.h>
#include <yt/yt/library/auth_server/token_authenticator.h>

#include <yt/yt/library/re2/re2.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/bus/server.h>

#include <yt/yt/core/concurrency/poller.h>
#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/net/address.h>
#include <yt/yt/core/net/connection.h>
#include <yt/yt/core/net/listener.h>
#include <yt/yt/core/net/local_address.h>

#include <util/string/split.h>

namespace NYT::NKafkaProxy {

using namespace NApi;
using namespace NAuth;
using namespace NConcurrency;
using namespace NKafka;
using namespace NNet;
using namespace NObjectClient;
using namespace NTableClient;
using namespace NThreading;
using namespace NTransactionClient;
using namespace NQueueClient;
using namespace NYPath;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = KafkaProxyLogger;

////////////////////////////////////////////////////////////////////////////////

class TServer
    : public IServer
{
public:
    TServer(
        TProxyBootstrapConfigPtr config,
        IPollerPtr poller,
        IPollerPtr acceptor,
        IListenerPtr listener,
        IRequestHandlerPtr requestHandler)
        : Config_(std::move(config))
        , Poller_(std::move(poller))
        , Acceptor_(std::move(acceptor))
        , Listener_(std::move(listener))
        , RequestHandler_(std::move(requestHandler))
    { }

    void Start() override
    {
        YT_VERIFY(!Started_);

        AsyncAcceptConnection();

        Started_ = true;

        YT_LOG_INFO("Kafka server started");
    }

private:
    const TProxyBootstrapConfigPtr Config_;

    const NNative::TClientCachePtr AuthenticatedClientCache_;

    const IPollerPtr Poller_;
    const IPollerPtr Acceptor_;
    const IListenerPtr Listener_;
    const IRequestHandlerPtr RequestHandler_;

    std::atomic<bool> Started_ = false;

    struct TKafkaConnection final
    {
        explicit TKafkaConnection(IConnectionPtr connection)
            : Connection(std::move(connection))
        { }

        IConnectionPtr Connection;

        TConnectionStatePtr Info = New<TConnectionState>();
    };
    using TKafkaConnectionPtr = TIntrusivePtr<TKafkaConnection>;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, ConnectionMapLock_);
    THashMap<TConnectionId, TKafkaConnectionPtr> Connections_;

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
        auto kafkaConnection = CreateConnection(
            Config_,
            connection,
            Poller_->GetInvoker(),
            BIND(&TServer::OnRequest, MakeWeak(this)),
            BIND(&TServer::OnConnectionFailure, MakeWeak(this)));
        kafkaConnection->Start();

        YT_LOG_DEBUG("Connection accepted "
            "(ConnectionId: %v, LocalAddress: %v, RemoteAddress: %v)",
            kafkaConnection->GetConnectionId(),
            connection->GetLocalAddress(),
            connection->GetRemoteAddress());

        auto connectionState = New<TKafkaConnection>(kafkaConnection);
        auto guard = WriterGuard(ConnectionMapLock_);
        EmplaceOrCrash(
            Connections_,
            kafkaConnection->GetConnectionId(),
            std::move(connectionState));
    }

    void OnRequest(IConnectionPtr connection, TMessage request)
    {
        try {
            GuardedOnRequest(connection, std::move(request));
        } catch (const std::exception& ex) {
            YT_LOG_ERROR(ex, "Failed to process request "
                "(ConnectionId: %v)",
                connection->GetConnectionId());

            OnConnectionFailure(connection, TError(ex));
        }
    }

    TKafkaConnectionPtr GetConnectionState(TConnectionId connectionId) const
    {
        auto guard = ReaderGuard(ConnectionMapLock_);
        auto connectionIt = Connections_.find(connectionId);
        if (connectionIt == Connections_.end()) {
            THROW_ERROR_EXCEPTION("Connection %v is not registered",
                connectionId);
        }
        return connectionIt->second;
    }

    void GuardedOnRequest(IConnectionPtr connection, TMessage request)
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        auto response = RequestHandler_->Handle(
            GetConnectionState(connection->GetConnectionId())->Info,
            request,
            Logger().WithTag("ConnectionId: %v", connection->GetConnectionId()));

        YT_UNUSED_FUTURE(connection->PostMessage(response)
            .Apply(BIND([=, this, this_ = MakeStrong(this)] (const TError& error) {
                if (!error.IsOK()) {
                    OnConnectionFailure(connection, error);
                }
            })));
    }

    void OnConnectionFailure(IConnectionPtr connection, TError error)
    {
        auto connectionId = connection->GetConnectionId();

        YT_LOG_INFO(
            error,
            "Kafka proxy server observed connection failure, terminating connection "
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
        YT_ASSERT_THREAD_AFFINITY_ANY();

        auto guard = WriterGuard(ConnectionMapLock_);
        if (Connections_.erase(connectionId)) {
            YT_LOG_DEBUG("Connection unregistered (ConnectionId: %v)",
                connectionId);
            return true;
        }

        return false;
    }
};

////////////////////////////////////////////////////////////////////////////////

IServerPtr CreateServer(
    TProxyBootstrapConfigPtr config,
    IPollerPtr poller,
    IPollerPtr acceptor,
    IRequestHandlerPtr requestHandler)
{
    auto address = TNetworkAddress::CreateIPv6Any(config->Port);
    for (int retryIndex = 0;; ++retryIndex) {
        try {
            auto listener = CreateListener(address, poller, acceptor, config->MaxBacklogSize);
            return New<TServer>(
                std::move(config),
                std::move(poller),
                std::move(acceptor),
                std::move(listener),
                std::move(requestHandler));
        } catch (const std::exception& ex) {
            if (retryIndex + 1 == config->BindRetryCount) {
                throw;
            } else {
                YT_LOG_ERROR(ex, "Kafka proxy server bind failed");
                Sleep(config->BindRetryBackoff);
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NKafkaProxy
