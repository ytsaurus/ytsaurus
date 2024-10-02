#include "common.h"

#include <yt/yt/orm/client/native/discovery_client.h>

namespace NYT::NOrm::NClient::NNative {
namespace {

using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

static NTests::ISimulatorPtr Simulator;

////////////////////////////////////////////////////////////////////////////////

class TSimulator
    : public NTests::ISimulator
{
public:
    TSimulator(std::vector<TMasterInfo> peers)
        : Peers_(std::move(peers))
    {
        for (const auto& peer : peers) {
            ActivePeers_.emplace(peer.GrpcAddress, true);
        }
    }

    void Shutdown(std::string_view peer) override
    {
        auto guard = NThreading::WriterGuard(SpinLock_);
        ActivePeers_[peer] = false;
    }

    void Restart(std::string_view peer) override
    {
        auto guard = NThreading::WriterGuard(SpinLock_);
        ActivePeers_[peer] = true;
    }

    bool IsAvailable(std::string_view peer) override
    {
        auto guard = NThreading::ReaderGuard(SpinLock_);
        if (Mode_ != NTests::ENodeShutdownSimulationMode::FailUserRequests) {
            return true;
        }

        if (auto* alive = MapFindPtr(ActivePeers_, peer)) {
            return *alive;
        }

        return true;
    }

    void SetSimulationMode(NTests::ENodeShutdownSimulationMode mode) override
    {
        auto guard = NThreading::WriterGuard(SpinLock_);
        Mode_ = mode;
    }

    std::optional<std::vector<TMasterInfo>> GetMasters(std::string_view address) override
    {
        auto guard = NThreading::ReaderGuard(SpinLock_);
        if (auto* alive = MapFindPtr(ActivePeers_, address)) {
            if (!*alive) {
                return std::nullopt;
            }
        }

        return Peers_;
    }

private:
    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, SpinLock_);
    std::vector<TMasterInfo> Peers_;
    THashMap<std::string, bool, THash<std::string>, TEqualTo<>> ActivePeers_;
    NTests::ENodeShutdownSimulationMode Mode_ = NTests::ENodeShutdownSimulationMode::IgnoreDiscoverRequests;
};

////////////////////////////////////////////////////////////////////////////////

class TFakeChannel
    : public IChannel
{
public:
    explicit TFakeChannel(const std::string& address)
        : Address_(address)
    { }

    const std::string& GetEndpointDescription() const override
    {
        return Address_;
    }

    const NYTree::IAttributeDictionary& GetEndpointAttributes() const override
    {
        YT_UNIMPLEMENTED();
    }

    IClientRequestControlPtr Send(
        IClientRequestPtr /*request*/,
        IClientResponseHandlerPtr responseHandler,
        const TSendOptions& /*options*/) override
    {
        if (Simulator->IsAvailable(Address_)) {
            responseHandler->HandleResponse(/*message*/ {}, Address_);
        } else {
            responseHandler->HandleError(TError{NRpc::EErrorCode::Unavailable, "Peer shutdown is being simulated"});
        }

        return nullptr;
    }

    void Terminate(const TError& /*error*/) override
    {
        YT_UNIMPLEMENTED();
    }

    int GetInflightRequestCount() override
    {
        return 0;
    }

    const IMemoryUsageTrackerPtr& GetChannelMemoryTracker() override
    {
        return MemoryUsageTracker_;
    }

    DEFINE_SIGNAL_OVERRIDE(void(const TError&), Terminated);

private:
    const IMemoryUsageTrackerPtr MemoryUsageTracker_ = GetNullMemoryUsageTracker();
    std::string Address_;
};

////////////////////////////////////////////////////////////////////////////////

class TFakeChannelFactory
    : public IChannelFactory
{
public:
    IChannelPtr CreateChannel(const std::string& address) override
    {
        return New<TFakeChannel>(address);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TFakeRequest
    : public TClientRequest
{
public:
    TFakeRequest(IChannelPtr channel)
        : TClientRequest(
            channel,
            TServiceDescriptor{"service"},
            TMethodDescriptor{"method"})
    { }

    TSharedRefArray SerializeHeaderless() const override
    {
        YT_UNIMPLEMENTED();
    }

    size_t GetHash() const override
    {
        return Hash_;
    }

private:
    size_t Hash_ = RandomNumber<size_t>();
};

////////////////////////////////////////////////////////////////////////////////

class TFakeResponseHandler
    : public IClientResponseHandler
{
public:
    void HandleAcknowledgement() override
    { }

    virtual void HandleResponse(TSharedRefArray /*message*/, const std::string& address) override
    {
        Result_.Set(address);
    }

    virtual void HandleError(TError error) override
    {
        Result_.Set(std::move(error));
    }

    virtual void HandleStreamingPayload(const TStreamingPayload& /*payload*/) override
    {
        YT_UNIMPLEMENTED();
    }

    virtual void HandleStreamingFeedback(const TStreamingFeedback& /*feedback*/) override
    {
        YT_UNIMPLEMENTED();
    }

    TFuture<std::string> GetResult()
    {
        return Result_.ToFuture();
    }

private:
    const TPromise<std::string> Result_ = NewPromise<std::string>();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

TFuture<TGetMastersResult> TDiscoveryServiceTraits<NTests::TFakeDiscoveryServiceProxy>::GetMasters(
    const NRpc::IChannelPtr& channel,
    const NLogging::TLogger& /*logger*/,
    TDuration /*timeout*/)
{
    YT_VERIFY(Simulator);
    auto masters = Simulator->GetMasters(NTests::GetAddress(channel));
    if (!masters) {
        return NewPromise<TGetMastersResult>().ToFuture();
    }

    return MakeFuture(TGetMastersResult{
        .MasterInfos = *masters,
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

namespace NTests {

////////////////////////////////////////////////////////////////////////////////

ISimulatorPtr InitializeSimulator(std::vector<TMasterInfo> peers)
{
    Simulator = New<TSimulator>(std::move(peers));
    return Simulator;
}

IChannelFactoryPtr MakeFakeChannelFactory()
{
    return New<TFakeChannelFactory>();
}

std::string GetAddress(const IChannelPtr& channel)
{
    return channel->GetEndpointDescription();
}

TFuture<std::string> PingGetAddress(const IChannelPtr& channel)
{
    auto request = New<TFakeRequest>(channel);
    auto handler = New<TFakeResponseHandler>();
    channel->Send(request, handler, /*options*/ {});
    return handler->GetResult();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTests
} // namespace NYT::NOrm::NClient::NNative
