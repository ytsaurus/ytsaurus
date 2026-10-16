#include "discovery.h"

#include "discovery_base.h"
#include "discovery_client.h"
#include "member_client.h"

namespace NYT::NDiscoveryClient {

using namespace NConcurrency;
using namespace NProfiling;
using namespace NRpc;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TDiscovery
    : public TDiscoveryBase
{
public:
    TDiscovery(
        TDiscoveryConfigPtr config,
        TDiscoveryConnectionConfigPtr discoveryConnection,
        IChannelFactoryPtr channelFactory,
        IInvokerPtr invoker,
        std::vector<std::string> extraAttributes,
        NLogging::TLogger logger,
        TProfiler profiler)
        : TDiscoveryBase(config, invoker, logger)
        , Config_(std::move(config))
        , DiscoveryConnection_(std::move(discoveryConnection))
        , ChannelFactory_(std::move(channelFactory))
        , DiscoveryClient_(CreateDiscoveryClient(DiscoveryConnection_, Config_, ChannelFactory_))
        , ParticipantCount_(profiler.Gauge("/participant_count"))
    {
        ListOptions_.AttributeKeys = std::move(extraAttributes);
    }

    TFuture<void> Enter(const std::string& name, IAttributeDictionaryPtr attributes) override
    {
        {
            auto guard = WriterGuard(Lock_);
            MemberClient_ = CreateMemberClient(
                DiscoveryConnection_,
                Config_,
                ChannelFactory_,
                Invoker_,
                name,
                Config_->GroupId);

            auto* memberAttributes = MemberClient_->GetAttributes();
            for (const auto& [key, value] : attributes->ListPairs()) {
                memberAttributes->Set(key, value);
            }
            NameAndAttributes_ = {name, attributes};
        }
        return MemberClient_->Start();
    }

    TFuture<void> Leave() override
    {
        {
            auto guard = WriterGuard(Lock_);
            NameAndAttributes_.reset();
        }
        return MemberClient_->Stop();
    }

private:
    TDiscoveryConfigPtr Config_;
    TDiscoveryConnectionConfigPtr DiscoveryConnection_;
    IChannelFactoryPtr ChannelFactory_;
    TListMembersOptions ListOptions_;

    IDiscoveryClientPtr DiscoveryClient_;
    IMemberClientPtr MemberClient_;

    TGauge ParticipantCount_;

    void DoUpdateList() override
    {
        WaitForFast(DiscoveryClient_->GetReadyEvent()
            .WithTimeout(Config_->DiscoveryReadinessTimeout))
            .ThrowOnError();
        auto list = WaitFor(DiscoveryClient_->ListMembers(Config_->GroupId, ListOptions_))
            .ValueOrThrow();

        THashMap<std::string, IAttributeDictionaryPtr> newList;
        for (const auto& memberInfo : list) {
            newList[memberInfo.Id] = memberInfo.Attributes->Clone();
        }
        {
            auto guard = WriterGuard(Lock_);
            swap(List_, newList);
            LastUpdate_ = TInstant::Now();
            ParticipantCount_.Update(List_.size());
        }
        YT_TLOG_DEBUG("List of participants updated")
            .With("Alive", list.size());
    }
};

////////////////////////////////////////////////////////////////////////////////

IDiscoveryPtr CreateDiscovery(
    TDiscoveryConfigPtr config,
    TDiscoveryConnectionConfigPtr discoveryConnection,
    IChannelFactoryPtr channelFactory,
    IInvokerPtr invoker,
    std::vector<std::string> extraAttributes,
    NLogging::TLogger logger,
    TProfiler profiler)
{
    return New<TDiscovery>(
        std::move(config),
        std::move(discoveryConnection),
        std::move(channelFactory),
        std::move(invoker),
        std::move(extraAttributes),
        std::move(logger),
        std::move(profiler));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDiscoveryClient
