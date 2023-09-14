#include "router.h"

#include "bootstrap.h"
#include "config.h"
#include "connection.h"
#include "dynamic_config_manager.h"
#include "private.h"

#include <yt/yt/core/concurrency/poller.h>
#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/net/address.h>
#include <yt/yt/core/net/dialer.h>
#include <yt/yt/core/net/listener.h>

namespace NYT::NTcpProxy {

using namespace NConcurrency;
using namespace NNet;
using namespace NYPath;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TcpProxyLogger;

////////////////////////////////////////////////////////////////////////////////

struct TEndpoint
{
    TString Host;
    ui16 Port;
};

bool operator==(const TEndpoint& lhs, const TEndpoint& rhs)
{
    return std::make_pair(lhs.Host, lhs.Port) == std::make_pair(rhs.Host, rhs.Port);
}

////////////////////////////////////////////////////////////////////////////////

class TRouter
    : public IRouter
{
public:
    TRouter(IBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
        , Config_(Bootstrap_->GetConfig()->Router)
        , Poller_(Bootstrap_->GetPoller())
        , Acceptor_(Bootstrap_->GetAcceptor())
        , Dialer_(CreateDialer(Config_->Dialer, Poller_, Logger))
    { }

    void Start() override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        RoutingTableUpdateExecutor_ = New<TPeriodicExecutor>(
            Bootstrap_->GetControlInvoker(),
            BIND(&TRouter::UpdateRoutingTable, MakeWeak(this)),
            GetDynamicConfig()->RoutingTableUpdatePeriod);
        RoutingTableUpdateExecutor_->Start();

        const auto& dynamicConfigManager = Bootstrap_->GetDynamicConfigManager();
        dynamicConfigManager->SubscribeConfigChanged(BIND(&TRouter::OnDynamicConfigChanged, MakeWeak(this)));
    }

private:
    using TEndpoints = std::vector<TEndpoint>;
    using TRoutingTable = THashMap<ui16, TEndpoints>;

private:
    const IBootstrap* const Bootstrap_;

    const TRouterConfigPtr Config_;

    const IPollerPtr Poller_;
    const IPollerPtr Acceptor_;

    const IDialerPtr Dialer_;

    // TODO(gritukan): Get rid of locks on critical path.
    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, Lock_);

    struct TPortState
    {
        IListenerPtr Listener;

        // NB: Addresses are not resolved intentionally to handle
        // DNS records update properly.
        TEndpoints Endpoints;
    };
    THashMap<ui16, TPortState> PortToState_;

    TPeriodicExecutorPtr RoutingTableUpdateExecutor_;

    void Reconfigure(const TRoutingTable& newRoutingTable)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto guard = WriterGuard(Lock_);

        std::vector<ui16> relevantPorts;
        relevantPorts.reserve(newRoutingTable.size() + PortToState_.size());
        for (const auto& [port, listener] : PortToState_) {
            relevantPorts.push_back(port);
        }
        for (const auto& [port, destinations] : newRoutingTable) {
            relevantPorts.push_back(port);
        }
        SortUnique(relevantPorts);

        for (auto port : relevantPorts) {
            auto stateIt = PortToState_.find(port);
            auto destinationsIt = newRoutingTable.find(port);

            if (destinationsIt == newRoutingTable.end()) {
                YT_VERIFY(stateIt != PortToState_.end());

                YT_LOG_INFO("Stopped listening port (Port: %v, Endpoints: %v)",
                    port,
                    GetLoggingString(stateIt->second.Endpoints));

                const auto& listener = stateIt->second.Listener;
                listener->Shutdown();

                EraseOrCrash(PortToState_, port);
            } else if (stateIt == PortToState_.end()) {
                TPortState state{
                    .Endpoints = destinationsIt->second,
                };
                try {
                    // TODO(gritukan): Syscall with write lock acquired is not that great idea.
                    state.Listener = CreateListener(
                        TNetworkAddress::CreateIPv6Any(port),
                        Poller_,
                        Acceptor_,
                        Config_->MaxListenerBacklogSize);
                } catch (const std::exception& ex) {
                    YT_LOG_WARNING(ex, "Failed to create listener (Port: %v)",
                        port);
                }

                YT_LOG_INFO("Started listening port (Port: %v, Endpoints: %v)",
                    port,
                    GetLoggingString(state.Endpoints));

                EmplaceOrCrash(PortToState_, port, state);
                RearmListener(state.Listener, port);
            } else if (stateIt->second.Endpoints != destinationsIt->second) {
                YT_LOG_INFO("Updated destinations list (Port: %v, Endpoints: %v -> %v)",
                    port,
                    GetLoggingString(stateIt->second.Endpoints),
                    GetLoggingString(destinationsIt->second));

                stateIt->second.Endpoints = destinationsIt->second;
            }
        }
    }

    void OnConnectionAccepted(
        ui16 port,
        IConnectionPtr connection)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        TEndpoint endpoint;
        {
            auto guard = ReaderGuard(Lock_);

            auto stateIt = PortToState_.find(port);

            // Port is not being proxied, just drop the connection.
            if (stateIt == PortToState_.end()) {
                return;
            }

            RearmListener(stateIt->second.Listener, port);

            const auto& endpoints = stateIt->second.Endpoints;

            // Destination list is empty, just drop the connection.
            if (endpoints.empty()) {
                return;
            }

            // Select random endpoint.
            endpoint = endpoints[RandomNumber<ui32>() % endpoints.size()];
        }

        // Typically does not actually wait.
        auto address = WaitForFast(TAddressResolver::Get()->Resolve(endpoint.Host))
            .ValueOrThrow();
        HandleConnection(
            std::move(connection),
            TNetworkAddress(address, endpoint.Port),
            Dialer_,
            Acceptor_->GetInvoker());
    }

    void RearmListener(
        const IListenerPtr& listener,
        ui16 port)
    {
        listener->Accept()
            .Apply(BIND(&TRouter::OnConnectionAccepted, MakeWeak(this), port)
            .AsyncVia(Acceptor_->GetInvoker()));
    }

    void UpdateRoutingTable()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        YT_LOG_INFO("Updating routing table");

        try {
            GuardedUpdateRoutingTable();
        } catch (const std::exception& ex) {
            YT_LOG_WARNING(ex, "Failed to update routing table");
        }

        YT_LOG_INFO("Routing table updated");
    }

    void GuardedUpdateRoutingTable()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto client = Bootstrap_->GetRootClient();
        auto routesPath = TYPath(TcpProxiesRoutesPath) + "/" + Bootstrap_->GetConfig()->Role;

        NApi::TListNodeOptions options{
            .Attributes = TAttributeFilter({"endpoints"}),
        };
        auto listResult = WaitFor(client->ListNode(routesPath, options))
            .ValueOrThrow();

        TRoutingTable routingTable;
        for (const auto& item : ConvertTo<IListNodePtr>(listResult)->GetChildren()) {
            auto key = item->GetValue<TString>();
            ui16 port;
            if (!TryFromString(key, port)) {
                THROW_ERROR_EXCEPTION("Unrecognized port %Qv in routing table", key);
            }

            TEndpoints endpoints;
            for (const auto& address : item->Attributes().Get<std::vector<TString>>("endpoints")) {
                TStringBuf host;
                int port;
                ParseServiceAddress(address, &host, &port);
                endpoints.push_back(TEndpoint{
                    .Host = TString(host),
                    .Port = ui16(port),
                });
            }

            EmplaceOrCrash(routingTable, port, endpoints);
        }

        Reconfigure(routingTable);
    }

    TRouterDynamicConfigPtr GetDynamicConfig()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return Bootstrap_->GetDynamicConfigManager()->GetConfig()->Router;
    }

    void OnDynamicConfigChanged(
        const TTcpProxyDynamicConfigPtr& /*oldConfig*/,
        const TTcpProxyDynamicConfigPtr& /*newConfig*/)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        RoutingTableUpdateExecutor_->SetPeriod(GetDynamicConfig()->RoutingTableUpdatePeriod);
    }

    static TString GetLoggingString(const TEndpoints& endpoints)
    {
        TStringBuilder builder;
        builder.AppendString("{");
        bool first = true;
        for (const auto& endpoint : endpoints) {
            if (first) {
                first = false;
            } else {
                builder.AppendString(", ");
            }
            builder.AppendString(endpoint.Host);
            builder.AppendString(":");
            builder.AppendString(ToString(endpoint.Port));
        }
        builder.AppendString("}");

        return builder.Flush();
    }
};

////////////////////////////////////////////////////////////////////////////////

IRouterPtr CreateRouter(IBootstrap* bootstrap)
{
    return New<TRouter>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTcpProxy
