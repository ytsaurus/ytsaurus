#include "discovery_base.h"
#include "discovery_v2.h"

#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/discovery_client/discovery_client.h>
#include <yt/yt/ytlib/discovery_client/member_client.h>

namespace NYT::NClickHouseServer {

using namespace NConcurrency;
using namespace NDiscoveryClient;
using namespace NRpc;
using namespace NYTree;
using namespace NApi::NNative;

////////////////////////////////////////////////////////////////////////////////

class TDiscoveryV2
    : public TDiscoveryBase
{
public:
    TDiscoveryV2(
        TDiscoveryV2ConfigPtr config,
        IConnectionPtr connection,
        NRpc::IChannelFactoryPtr channelFactory,
        IInvokerPtr invoker,
        std::vector<TString> extraAttributes,
        NLogging::TLogger logger)
        : TDiscoveryBase(config, invoker, logger)
        , Config_(std::move(config))
        , Connection_(std::move(connection))
        , ChannelFactory_(std::move(channelFactory))
        , DiscoveryClient_(Connection_->CreateDiscoveryClient(Config_, ChannelFactory_))
    {
        ListOptions_.AttributeKeys = extraAttributes;
    }

    TFuture<void> Enter(TString name, NYTree::IAttributeDictionaryPtr attributes) override
    {
        {
            auto guard = WriterGuard(Lock_);
            // TODO: Make sure there is discovery connection at this point.
            MemberClient_ = Connection_->CreateMemberClient(
                Config_,
                ChannelFactory_,
                Invoker_,
                name,
                Config_->GroupId);

            auto* memberAttributes = MemberClient_->GetAttributes();
            for (const auto& [key, value]: attributes->ListPairs()) {
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

    int Version() const override
    {
        return 2;
    }

private:
    TDiscoveryV2ConfigPtr Config_;
    IConnectionPtr Connection_;
    NRpc::IChannelFactoryPtr ChannelFactory_;
    TListMembersOptions ListOptions_;

    IDiscoveryClientPtr DiscoveryClient_;
    IMemberClientPtr MemberClient_;

    void DoUpdateList() override
    {
        WaitForFast(DiscoveryClient_->GetReadyEvent())
            .ThrowOnError();
        auto list = WaitFor(DiscoveryClient_->ListMembers(Config_->GroupId, ListOptions_))
            .ValueOrThrow();

        THashMap<TString, NYTree::IAttributeDictionaryPtr> newList;
        for (const auto& memberInfo: list) {
            newList[memberInfo.Id] = memberInfo.Attributes->Clone();
        }
        {
            auto guard = WriterGuard(Lock_);
            swap(List_, newList);
            LastUpdate_ = TInstant::Now();
        }
        YT_LOG_DEBUG("List of participants updated (Alive: %v)", list.size());
    }
};

////////////////////////////////////////////////////////////////////////////////

IDiscoveryPtr CreateDiscoveryV2(
    TDiscoveryV2ConfigPtr config,
    IConnectionPtr connection,
    NRpc::IChannelFactoryPtr channelFactory,
    IInvokerPtr invoker,
    std::vector<TString> extraAttributes,
    NLogging::TLogger logger)
{
    return New<TDiscoveryV2>(
        std::move(config),
        std::move(connection),
        std::move(channelFactory),
        std::move(invoker),
        std::move(extraAttributes),
        std::move(logger));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
