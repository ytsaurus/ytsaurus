#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/throughput_throttler.h>

#include <yt/yt/ytlib/distributed_throttler/public.h>
#include <yt/yt/ytlib/distributed_throttler/distributed_throttler.h>
#include <yt/yt/ytlib/distributed_throttler/config.h>

#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/discovery_client/config.h>
#include <yt/yt/ytlib/discovery_client/discovery_client.h>
#include <yt/yt/ytlib/discovery_client/member_client.h>

#include <yt/yt/server/lib/discovery_server/public.h>
#include <yt/yt/server/lib/discovery_server/config.h>
#include <yt/yt/server/lib/discovery_server/discovery_server.h>

#include <yt/yt/server/lib/discovery_server/unittests/mock/connection.h>

#include <yt/yt/core/rpc/local_channel.h>
#include <yt/yt/core/rpc/local_server.h>
#include <yt/yt/core/rpc/server.h>
#include <yt/yt/core/rpc/static_channel_factory.h>

#include <yt/yt/core/profiling/timing.h>

#include <vector>

namespace NYT::NDistributedThrottler {

using namespace NConcurrency;
using namespace NRpc;
using namespace NDiscoveryClient;
using namespace NDiscoveryServer;
using namespace NApi;
using namespace NApi::NNative;
using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

class TDiscoveryServersHolder
    : public TRefCounted
{
public:
    void Initialize()
    {
        ChannelFactory_ = New<TStaticChannelFactory>();
        for (const auto& address : Addresses_) {
            RpcServers_.push_back(CreateLocalServer());
            ChannelFactory_->Add(address, CreateLocalChannel(RpcServers_.back()));
            RpcServers_.back()->Start();
        }

        auto serverConfig = New<TDiscoveryServerConfig>();
        serverConfig->ServerAddresses = Addresses_;
        serverConfig->AttributesUpdatePeriod = TDuration::Seconds(2);

        for (size_t i = 0; i < Addresses_.size(); ++i) {
            DiscoveryServers_.push_back(CreateDiscoveryServer(serverConfig, i));
            DiscoveryServers_.back()->Initialize();
        }
    }

    void Finalize()
    {
        for (size_t i = 0; i < Addresses_.size(); ++i) {
            DiscoveryServers_[i]->Finalize();
            YT_UNUSED_FUTURE(RpcServers_[i]->Stop());
        }
    }

    const std::vector<TString>& GetDiscoveryServersAddresses()
    {
        return Addresses_;
    }

    const TStaticChannelFactoryPtr& GetChannelFactory()
    {
        return ChannelFactory_;
    }

private:
    std::vector<TString> Addresses_ = {"peer1", "peer2", "peer3", "peer4", "peer5"};
    std::vector<IDiscoveryServerPtr> DiscoveryServers_;
    std::vector<IServerPtr> RpcServers_;

    std::vector<TActionQueuePtr> ActionQueues_;
    TStaticChannelFactoryPtr ChannelFactory_;

    IDiscoveryServerPtr CreateDiscoveryServer(const TDiscoveryServerConfigPtr& serverConfig, int index)
    {
        auto serverActionQueue = New<TActionQueue>(Format("DiscoveryServer %v", index));
        auto gossipActionQueue = New<TActionQueue>(Format("Gossip %v", index));

        auto server = NDiscoveryServer::CreateDiscoveryServer(
            RpcServers_[index],
            Addresses_[index],
            serverConfig,
            ChannelFactory_,
            serverActionQueue->GetInvoker(),
            gossipActionQueue->GetInvoker(),
            /*authenticator*/ nullptr);

        ActionQueues_.push_back(serverActionQueue);
        ActionQueues_.push_back(gossipActionQueue);

        return server;
    }
};

class TState
{
public:
    TState()
        : DiscoveryServers_(New<TDiscoveryServersHolder>())
        , Config_(New<TDistributedThrottlerConfig>())
        , Address_("ThrottlerService")
        , RpcServer_(CreateLocalServer())
    {
        DiscoveryServers_->Initialize();
        auto channelFactory = DiscoveryServers_->GetChannelFactory();

        auto connectionConfig = New<TDiscoveryConnectionConfig>();
        auto addresses = DiscoveryServers_->GetDiscoveryServersAddresses();
        connectionConfig->Addresses = addresses;

        Connection_ = New<TMockDistributedThrottlerConnection>(connectionConfig);

        Config_->Mode = EDistributedThrottlerMode::Adaptive;

        channelFactory->Add(Address_, CreateLocalChannel(RpcServer_));
    }

    void ReconfigureFactory(int factoryIndex, int seconds)
    {
        Config_->MemberClient->HeartbeatPeriod = TDuration::Seconds(seconds);
        ThrottlerFactories_[factoryIndex].Factory->Reconfigure(Config_);
    }

    void AddThrottlerFactory()
    {
        auto memberActionQueue = New<TActionQueue>(Format("MemberClient %v", ThrottlerFactoryIndex_));
        ActionQueues_.push_back(memberActionQueue);

        auto throttlerFactory = CreateDistributedThrottlerFactory(
            Config_,
            DiscoveryServers_->GetChannelFactory(),
            Connection_,
            memberActionQueue->GetInvoker(),
            "/group",
            Format("Factory %v", ThrottlerFactoryIndex_),
            RpcServer_,
            Address_,
            DiscoveryServerLogger,
            /*authenticator*/ nullptr);

        ThrottlerFactories_.emplace_back(throttlerFactory);

        {
            auto guard = WriterGuard(UsageLock_);
            DesiredUsage_.emplace_back();
            for (int i = PoppedThrottlerCount_; i < ThrottlerIndex_; ++i) {
                DesiredUsage_.back().push_back(GetDefaultThrottlerUsage(ThrottlerFactoryIndex_));
            }
        }

        for (int i = PoppedThrottlerCount_; i < ThrottlerIndex_; ++i) {
            CreateThrottler(ThrottlerFactoryIndex_);
        }
        ++ThrottlerFactoryIndex_;
    }

    void AddThrottler(TThroughputThrottlerConfigPtr config)
    {
        auto throttlerActionQueue = New<TActionQueue>(Format("Throttler %v", ThrottlerIndex_));
        ActionQueues_.push_back(throttlerActionQueue);
        ThrottlerConfigs_.push_back(std::move(config));

        {
            auto guard = WriterGuard(UsageLock_);

            YT_VERIFY(static_cast<int>(DesiredUsage_.size()) == ThrottlerFactoryIndex_);

            for (int i = PoppedThrottlerFactoryCount_; i < ThrottlerFactoryIndex_; ++i) {
                DesiredUsage_[i].push_back(GetDefaultThrottlerUsage(i));
            }
        }

        ++ThrottlerIndex_;

        for (int i = PoppedThrottlerFactoryCount_; i < ThrottlerFactoryIndex_; ++i) {
            CreateThrottler(i);
        }
    }

    void UpdateThrottler(int factoryIndex, int throttlerIndex, i64 desiredUsage)
    {
        YT_VERIFY(factoryIndex < ThrottlerFactoryIndex_);
        YT_VERIFY(throttlerIndex < ThrottlerIndex_);

        auto guard = WriterGuard(UsageLock_);
        DesiredUsage_[factoryIndex][throttlerIndex] = desiredUsage;
    }

    void PopThrottler()
    {
        ++PoppedThrottlerCount_;
        for (auto& throttlerFactory : ThrottlerFactories_) {
            throttlerFactory.Throttlers.pop_front();
        }
    }

    void PopFactory()
    {
        ++PoppedThrottlerFactoryCount_;
        ThrottlerFactories_.pop_front();
    }

    void Reconfigure(int throttlerIndex, double newLimit)
    {
        ThrottlerConfigs_[throttlerIndex]->Limit = newLimit > 0 ? newLimit : std::optional<double>(std::nullopt);

        auto realthrottlerIndex = throttlerIndex - PoppedThrottlerCount_;
        YT_VERIFY(realthrottlerIndex >= 0);

        for (const auto& factory : ThrottlerFactories_) {
            YT_VERIFY(realthrottlerIndex < static_cast<int>(factory.Throttlers.size()));
            factory.Throttlers[realthrottlerIndex]->Reconfigure(ThrottlerConfigs_[throttlerIndex]);
        }
    }

    void Print()
    {
        auto guard = ReaderGuard(UsageLock_);
        for (int i = PoppedThrottlerFactoryCount_; i < ThrottlerFactoryIndex_; ++i) {
            for (int j = PoppedThrottlerCount_; j < ThrottlerIndex_; ++j) {
                Cout << "factoryIndex: " << i << " throttlerIndex: " << j << " usage: " << DesiredUsage_[i][j] << Endl;
            }
        }
    }

private:
    const TIntrusivePtr<TDiscoveryServersHolder> DiscoveryServers_;
    const TDistributedThrottlerConfigPtr Config_;
    const TString Address_;
    const IServerPtr RpcServer_;

    NNative::IConnectionPtr Connection_;

    int ThrottlerFactoryIndex_ = 0;
    int PoppedThrottlerFactoryCount_ = 0;

    int ThrottlerIndex_ = 0;
    int PoppedThrottlerCount_ = 0;

    std::vector<TActionQueuePtr> ActionQueues_;

    struct TThrottlerFactory
    {
        explicit TThrottlerFactory(IDistributedThrottlerFactoryPtr factory)
            : Factory(std::move(factory))
        { }

        IDistributedThrottlerFactoryPtr Factory;
        std::deque<IReconfigurableThroughputThrottlerPtr> Throttlers;
    };

    std::deque<TThrottlerFactory> ThrottlerFactories_;

    std::vector<TThroughputThrottlerConfigPtr> ThrottlerConfigs_;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, UsageLock_);
    std::vector<std::vector<i64>> DesiredUsage_;

    i64 GetDefaultThrottlerUsage(int factoryIndex)
    {
        return (factoryIndex + 1) * 10;
    }

    void CreateThrottler(int factoryIndex)
    {
        auto realFactoryIndex = factoryIndex - PoppedThrottlerFactoryCount_;
        YT_VERIFY(realFactoryIndex < static_cast<int>(ThrottlerFactories_.size()));
        auto& throttlersFactory = ThrottlerFactories_[realFactoryIndex];

        int throttlerIndex = throttlersFactory.Throttlers.size() + PoppedThrottlerCount_;
        YT_VERIFY(throttlerIndex < ThrottlerIndex_);

        auto throttleRequestActionQueue = New<TActionQueue>(Format("ThrottleRequest %v %v", ThrottlerFactoryIndex_, throttlerIndex));
        ActionQueues_.push_back(throttleRequestActionQueue);

        YT_VERIFY(throttlerIndex < static_cast<int>(ThrottlerConfigs_.size()));
        auto throttler = throttlersFactory.Factory->GetOrCreateThrottler(Format("Throttler %v", throttlerIndex), ThrottlerConfigs_[throttlerIndex]);
        throttlersFactory.Throttlers.push_back(throttler);

        throttleRequestActionQueue->GetInvoker()->Invoke(
            BIND([this, factoryIndex = factoryIndex, throttlerIndex = throttlerIndex, weakThrottler = MakeWeak(throttler)] {
                while (true) {
                    auto throttler = weakThrottler.Lock();
                    if (!throttler) {
                        return;
                    }
                    TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(100));

                    double count = 0;
                    {
                        auto guard = ReaderGuard(UsageLock_);
                        count = DesiredUsage_[factoryIndex][throttlerIndex] * 1.0 / 10;
                    }
                    Y_UNUSED(WaitFor(throttler->Throttle(count)));
                }
            }));
    }
};

void RunThrottlers()
{
    TState state;

    for (int i = 0; i < 2; ++i) {
        state.AddThrottlerFactory();
    }

    for (int i = 0; i < 1; ++i) {
        state.AddThrottler(TThroughputThrottlerConfig::Create(1000));
    }

    state.Print();

    while (true) {
        char action;
        Cin >> action;
        switch (action) {
            case 'u': {
                int factoryIndex, throttlerIndex;
                i64 desiredUsage;
                Cin >> factoryIndex >> throttlerIndex >> desiredUsage;
                state.UpdateThrottler(factoryIndex, throttlerIndex, desiredUsage);
                break;
            }
            case 'f': {
                state.AddThrottlerFactory();
                break;
            }
            case 'a': {
                double limit;
                Cin >> limit;
                state.AddThrottler(TThroughputThrottlerConfig::Create(limit));
                break;
            }
            case 'p': {
                state.PopThrottler();
                break;
            }
            case 'd': {
                state.PopFactory();
                break;
            }
            case 'r': {
                double limit;
                int throttlerIndex;
                Cin >> throttlerIndex >> limit;
                state.Reconfigure(throttlerIndex, limit);
                break;
            }
            case 'c': {
                int factoryIndex, seconds;
                Cin >> factoryIndex >> seconds;
                state.ReconfigureFactory(factoryIndex, seconds);
                break;
            }
            default:
                break;
        }
        state.Print();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDistributedThrottler

int main()
{
    try {
        NYT::NDistributedThrottler::RunThrottlers();
        return 0;
    } catch (const std::exception& ex) {
        Cerr << ToString(NYT::TError(ex)) << Endl;
        return 1;
    }
}
