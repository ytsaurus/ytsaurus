#include "spyt_discovery.h"

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/discovery_client/discovery_client.h>
#include <yt/yt/ytlib/hive/cluster_directory.h>

namespace NYT::NQueryTracker {

using namespace NDiscoveryClient;
using namespace NRpc;
using namespace NYTree;
using namespace NConcurrency;

class TSpytDiscoveryV1
    : public ISpytDiscovery
{
public:
    TSpytDiscoveryV1(NApi::IClientPtr queryClient, TYPath discoveryPath)
        : QueryClient_(std::move(queryClient))
        , DiscoveryPath_(std::move(discoveryPath))
    { }

    std::optional<TString> GetVersion() const
    {
        return GetModuleValue("version");
    }

    std::optional<TString> GetLivyUrl() const
    {
        return GetModuleValue("livy");
    }

    std::optional<TString> GetMasterWebUIUrl() const
    {
        return GetModuleValue("webui");
    }

private:
    const NApi::IClientPtr QueryClient_;
    const TYPath DiscoveryPath_;

    std::vector<TString> GetModuleValues(const TString& modulePath) const
    {
        auto rawResult = WaitFor(QueryClient_->ListNode(modulePath))
            .ValueOrThrow();
        return ConvertTo<std::vector<TString>>(rawResult);
    }

    std::optional<TString> GetModuleValue(const TString& moduleName) const
    {
        auto modulePath = Format("%v/discovery/%v", DiscoveryPath_, NYPath::ToYPathLiteral(moduleName));
        auto moduleExists = WaitFor(QueryClient_->NodeExists(modulePath))
            .ValueOrThrow();
        if (!moduleExists) {
            return std::nullopt;
        }
        auto listResult = GetModuleValues(modulePath);
        if (listResult.size() > 1) {
            THROW_ERROR_EXCEPTION(
                "Invalid discovery directory for %v: at most 1 value expected, found %v",
                moduleName,
                listResult.size());
        }
        return listResult.size() == 1 ? std::optional(listResult[0]) : std::nullopt;
    }
};

class TSpytDiscoveryV2
    : public ISpytDiscovery
{
public:
    TSpytDiscoveryV2(
        NApi::NNative::IConnectionPtr connection,
        TString discoveryGroup,
        IChannelFactoryPtr channelFactory,
        NLogging::TLogger logger)
        : Connection_(std::move(connection))
        , DiscoveryGroup_(Format("/spyt/%v", discoveryGroup))
        , ChannelFactory_(std::move(channelFactory))
        , Logger(std::move(logger))
    {
        ListOptions_.AttributeKeys = {"url", "version"};

        auto discoveryConfig = New<TDiscoveryClientConfig>();
        discoveryConfig->ReadQuorum = 1;
        DiscoveryClient_ = Connection_->CreateDiscoveryClient(discoveryConfig, ChannelFactory_);
    }

    std::optional<TString> GetVersion() const
    {
        return GetAttribute("version");
    }

    std::optional<TString> GetLivyUrl() const
    {
        return GetAttribute("url");
    }

    std::optional<TString> GetMasterWebUIUrl() const
    {
        return {};
    }

private:
    const NApi::NNative::IConnectionPtr Connection_;
    const TString DiscoveryGroup_;
    const IChannelFactoryPtr ChannelFactory_;
    const NLogging::TLogger Logger;

    TListMembersOptions ListOptions_;
    IDiscoveryClientPtr DiscoveryClient_;

    TMemberInfo GetLivyMember() const
    {
        WaitForFast(DiscoveryClient_->GetReadyEvent()
            .WithTimeout(TDuration::Seconds(30)))
            .ThrowOnError();
        auto memeberList = WaitFor(DiscoveryClient_->ListMembers(DiscoveryGroup_, ListOptions_))
            .ValueOrThrow();
        YT_LOG_DEBUG("Discovery group was found (Name: %v, Size: %v)", DiscoveryGroup_, memeberList.size());
        for (const auto& memberInfo : memeberList) {
            if (memberInfo.Id == "livy") {
                return memberInfo;
            }
        }
        THROW_ERROR_EXCEPTION("Livy was not found in the discovery group (Group: %v)", DiscoveryGroup_);
    }

    std::optional<TString> GetAttribute(const TString& attr) const
    {
        return GetLivyMember().Attributes->Find<TString>(attr);
    }
};

ISpytDiscoveryPtr CreateDiscoveryV1(
    NApi::IClientPtr queryClient,
    TYPath discoveryPath)
{
    return New<TSpytDiscoveryV1>(
        queryClient,
        discoveryPath);
}

ISpytDiscoveryPtr CreateDiscoveryV2(
    NApi::NNative::IConnectionPtr connection,
    TString discoveryGroup,
    IChannelFactoryPtr channelFactory,
    NLogging::TLogger logger)
{
    return New<TSpytDiscoveryV2>(
        std::move(connection),
        std::move(discoveryGroup),
        std::move(channelFactory),
        std::move(logger));
}

}
