#include "server.h"

#include "private.h"

#include <yt/yt/core/concurrency/poller.h>
#include <yt/yt/core/concurrency/thread_pool_poller.h>

#include <yt/yt/core/net/connection.h>
#include <yt/yt/core/net/listener.h>

namespace NYT::NZookeeper {

using namespace NConcurrency;
using namespace NNet;

static const auto& Logger = ZookeeperLogger;

////////////////////////////////////////////////////////////////////////////////

class TServer
    : public IServer
{
public:
    TServer(
        TNetworkAddress address,
        IPollerPtr poller,
        IPollerPtr acceptor)
        : Address_(std::move(address))
        , Listener_(CreateListener(Address_, std::move(poller), acceptor))
        , Acceptor_(std::move(acceptor))
    { }

    void Start() override
    {
        YT_LOG_INFO("Starting Zookeeper server (Address: %v)", Address_);

        AsyncAcceptConnection();
    }

    DEFINE_SIGNAL_OVERRIDE(void(const NNet::IConnectionPtr&), ConnectionAccepted);

private:
    const NNet::TNetworkAddress Address_;
    const NNet::IListenerPtr Listener_;
    const IPollerPtr Acceptor_;

    void AsyncAcceptConnection()
    {
        try {
            Listener_->Accept().Subscribe(
                BIND(&TServer::OnConnectionAccepted, MakeWeak(this))
                    .Via(Acceptor_->GetInvoker()));
        } catch (const std::exception& ex) {
            BIND(&TServer::OnConnectionAccepted, MakeWeak(this), TError(ex))
                .Via(Acceptor_->GetInvoker())
                .Run();
        }
    }

    void OnConnectionAccepted(const TErrorOr<NNet::IConnectionPtr>& connectionOrError)
    {
        AsyncAcceptConnection();

        if (!connectionOrError.IsOK()) {
            YT_LOG_INFO(connectionOrError, "Error accepting connection");
            return;
        }

        const auto& connection = connectionOrError.Value();

        YT_LOG_DEBUG("Connection accepted (LocalAddress: %v, RemoteAddress: %v)",
            connection->LocalAddress(),
            connection->RemoteAddress());

        ConnectionAccepted_.Fire(connection);
    }
};

////////////////////////////////////////////////////////////////////////////////

IServerPtr CreateServer(const IPollerPtr& poller, int port)
{
    auto address = TNetworkAddress::CreateIPv6Any(port);

    return New<TServer>(std::move(address), /*poller*/ poller, /*acceptor*/ poller);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NZookeeper
