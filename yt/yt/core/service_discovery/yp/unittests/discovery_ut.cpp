#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/service_discovery/yp/config.h>
#include <yt/yt/core/service_discovery/yp/service_discovery.h>
#include <yt/yt/core/service_discovery/yp/service_discovery_service_proxy.h>

#include <infra/yp_service_discovery/api/api.pb.h>

#include <yt/yt/core/concurrency/thread_pool.h>
#include <yt/yt/core/misc/finally.h>
#include <yt/yt/core/rpc/grpc/config.h>
#include <yt/yt/core/rpc/grpc/server.h>
#include <yt/yt/core/rpc/service_detail.h>

#include <library/cpp/testing/common/network.h>

namespace NYT::NServiceDiscovery::NYP {
namespace {

using namespace NConcurrency;
using namespace NLogging;
using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

struct TEndpointsClusterSnapshot
{
    THashMap<TString, int> EndpointCount;
};

using TEndpointsClusterSnapshotPtr = std::unique_ptr<TEndpointsClusterSnapshot>;

class TServiceDiscoveryService
    : public TServiceBase
{
public:
    explicit TServiceDiscoveryService(IInvokerPtr invoker)
        : TServiceBase(
            std::move(invoker),
            TServiceDiscoveryServiceProxy::GetDescriptor(),
            NLogging::TLogger("TestYPSD"))
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ResolveEndpoints));
    }

    void SetClusterSnapshot(
        const TString& cluster,
        TEndpointsClusterSnapshotPtr clusterSnapshot,
        ui64 snapshotTimestamp)
    {
        auto guard = Guard(SnapshotsLock_);
        SnapshotPerCluster_[cluster] = std::move(clusterSnapshot);
        SnapshotsTimestamp_ = snapshotTimestamp;
    }

    void SetClusterSnapshot(
        const TString& cluster,
        TEndpointsClusterSnapshotPtr clusterSnapshot)
    {
        auto guard = Guard(SnapshotsLock_);
        SnapshotPerCluster_[cluster] = std::move(clusterSnapshot);
        SnapshotsTimestamp_ += 1;
    }

    void SetFlaky(bool flaky)
    {
        Flaky_.store(flaky);
    }

    void SetDisabled(bool disabled)
    {
        Disabled_.store(disabled);
    }

private:
    THashMap<TString, TEndpointsClusterSnapshotPtr> SnapshotPerCluster_;
    ui64 SnapshotsTimestamp_ = 0;
    TSpinLock SnapshotsLock_;

    std::atomic<bool> Flaky_ = false;
    std::atomic<bool> Disabled_ = false;
    std::atomic<int> RequestNumber_ = 0;

    DECLARE_RPC_SERVICE_METHOD(::NYP::NServiceDiscovery::NApi, ResolveEndpoints)
    {
        context->SetRequestInfo();

        EXPECT_FALSE(request->client_name().empty());
        EXPECT_FALSE(request->ruid().empty());

        if (Disabled_.load()) {
            THROW_ERROR_EXCEPTION("Service discovery is disabled");
        }

        int requestNumber = RequestNumber_.fetch_add(1);

        if (requestNumber % 2 == 0 && Flaky_.load()) {
            THROW_ERROR_EXCEPTION(NRpc::EErrorCode::TransportError,
                "Flaky service discovery response");
        }

        response->set_timestamp(1);
        auto* protoEndpointSet = response->mutable_endpoint_set();
        protoEndpointSet->set_endpoint_set_id(request->endpoint_set_id());

        int endpointCount = -1;
        {
            auto guard = Guard(SnapshotsLock_);
            response->set_timestamp(SnapshotsTimestamp_);
            auto itSnapshot = SnapshotPerCluster_.find(request->cluster_name());
            if (itSnapshot != SnapshotPerCluster_.end()) {
                auto* snapshot = itSnapshot->second.get();
                auto it = snapshot->EndpointCount.find(request->endpoint_set_id());
                if (it != snapshot->EndpointCount.end()) {
                    endpointCount = it->second;
                    YT_VERIFY(endpointCount >= 0);
                }
            }
        }

        if (endpointCount < 0) {
            response->set_resolve_status(::NYP::NServiceDiscovery::NApi::EResolveStatus::NOT_EXISTS);
        } else {
            response->set_resolve_status(::NYP::NServiceDiscovery::NApi::EResolveStatus::OK);
            for (int index = 0; index < endpointCount; ++index) {
                auto* protoEndpoint = protoEndpointSet->add_endpoints();
                protoEndpoint->set_id(request->endpoint_set_id() + ToString(index));
            }
        }

        response->set_host("test");

        context->Reply();
    }
};

using TServiceDiscoveryServicePtr = TIntrusivePtr<TServiceDiscoveryService>;

////////////////////////////////////////////////////////////////////////////////

class TYPServiceDiscoveryTest
    : public ::testing::Test
{
public:
    virtual void SetUp() override final
    {
        Port_ = NTesting::GetFreePort();
        Address_ = Format("localhost:%v", Port_);

        WorkerPool_ = CreateThreadPool(4, "TestYPSDWorker");
        Server_ = CreateServer();
        Service_ = New<TServiceDiscoveryService>(WorkerPool_->GetInvoker());

        Server_->RegisterService(Service_);

        Server_->Start();
    }

    virtual void TearDown() override final
    {
        Server_->Stop().Get().ThrowOnError();
        Server_.Reset();

        Service_->Stop().Get().ThrowOnError();
        Service_.Reset();

        WorkerPool_->Shutdown();
        WorkerPool_.Reset();
    }

    TServiceDiscoveryConfigPtr CreateTestServiceDiscoveryConfig(bool enableRetries = true) const
    {
        auto config = New<TServiceDiscoveryConfig>();
        config->Fqdn = "localhost";
        config->GrpcPort = Port_;
        config->Client = "yt";
        if (!enableRetries) {
            config->RetryAttempts = 1;
        }
        config->Postprocess();
        return config;
    }

    const TServiceDiscoveryServicePtr& GetService() const
    {
        return Service_;
    }

private:
    NTesting::TPortHolder Port_;
    TString Address_;

    NConcurrency::IThreadPoolPtr WorkerPool_;
    IServerPtr Server_;
    TServiceDiscoveryServicePtr Service_;

    IServerPtr CreateServer() const
    {
        auto serverAddressConfig = New<NGrpc::TServerAddressConfig>();
        serverAddressConfig->Address = Address_;
        serverAddressConfig->Postprocess();

        auto serverConfig = New<NGrpc::TServerConfig>();
        serverConfig->Addresses.push_back(std::move(serverAddressConfig));
        serverConfig->Postprocess();

        return NGrpc::CreateServer(std::move(serverConfig));
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TYPServiceDiscoveryTest, Simple)
{
    {
        auto snapshot = std::make_unique<TEndpointsClusterSnapshot>();
        snapshot->EndpointCount.emplace("first-set", 5);
        GetService()->SetClusterSnapshot("sas", std::move(snapshot));
    }
    {
        auto snapshot = std::make_unique<TEndpointsClusterSnapshot>();
        snapshot->EndpointCount.emplace("second-set", 1);
        GetService()->SetClusterSnapshot("man", std::move(snapshot));
    }

    auto validateEndpointSet = [] (
        const TEndpointSet& endpointSet,
        const TString& endpointSetId,
        size_t expectedEndpointCount)
    {
        EXPECT_EQ(endpointSetId, endpointSet.Id);
        EXPECT_EQ(expectedEndpointCount, endpointSet.Endpoints.size());
        for (const auto& endpoint : endpointSet.Endpoints) {
            EXPECT_FALSE(endpoint.Id.empty());
        }
    };

    auto sd = CreateServiceDiscovery(CreateTestServiceDiscoveryConfig());
    {
        auto endpointSet = WaitFor(sd->ResolveEndpoints("sas", "first-set"))
            .ValueOrThrow();
        validateEndpointSet(endpointSet, "first-set", 5);
    }
    {
        auto endpointSetOrError = WaitFor(sd->ResolveEndpoints("sas", "second-set"));
        EXPECT_THROW_WITH_ERROR_CODE(endpointSetOrError.ValueOrThrow(), NServiceDiscovery::EErrorCode::EndpointSetDoesNotExist);
    }
    {
        auto endpointSet = WaitFor(sd->ResolveEndpoints("man", "second-set"))
            .ValueOrThrow();
        validateEndpointSet(endpointSet, "second-set", 1);
    }
}

TEST_F(TYPServiceDiscoveryTest, Retries)
{
    GetService()->SetFlaky(true);
    auto guard = Finally([&] { GetService()->SetFlaky(false); });

    {
        auto snapshot = std::make_unique<TEndpointsClusterSnapshot>();
        snapshot->EndpointCount.emplace("first-set", 5);
        GetService()->SetClusterSnapshot("sas", std::move(snapshot));
    }

    for (auto enableRetries : {false, true}) {
        auto runTest = [&] {
            auto sd = CreateServiceDiscovery(CreateTestServiceDiscoveryConfig(enableRetries));
            for (int i = 0; i < 10; ++i) {
                auto endpointSet = WaitFor(sd->ResolveEndpoints("sas", "first-set"))
                    .ValueOrThrow();
                EXPECT_EQ("first-set", endpointSet.Id);
            }
        };
        if (enableRetries) {
            runTest();
        } else {
            EXPECT_THROW(runTest(), TErrorException);
        }
    }
}

TEST_F(TYPServiceDiscoveryTest, TimestampMonotonicity)
{
    auto setSnapshot = [this] (ui32 endpointCount, ui64 timestamp) {
        auto snapshot = std::make_unique<TEndpointsClusterSnapshot>();
        snapshot->EndpointCount.emplace("first-set", endpointCount);
        GetService()->SetClusterSnapshot(
            "sas",
            std::move(snapshot),
            timestamp);
    };

    auto sdConfig = CreateTestServiceDiscoveryConfig();
    auto waitDuration = (*sdConfig->RefreshTime) * 5;
    auto sd = CreateServiceDiscovery(sdConfig);

    auto getEndpointSetSize = [&] {
        return WaitFor(sd->ResolveEndpoints("sas", "first-set"))
            .ValueOrThrow()
            .Endpoints
            .size();
    };

    // Initialize.
    setSnapshot(5, 2);
    EXPECT_EQ(5u, getEndpointSetSize());

    // Update.
    setSnapshot(3, 3);
    TDelayedExecutor::WaitForDuration(waitDuration);
    EXPECT_EQ(3u, getEndpointSetSize());

    // Rollback.
    setSnapshot(4, 1);
    TDelayedExecutor::WaitForDuration(waitDuration);
    EXPECT_EQ(3u, getEndpointSetSize());
}

TEST_F(TYPServiceDiscoveryTest, Caching)
{
    auto setSnapshot = [this] (ui32 endpointCount) {
        auto snapshot = std::make_unique<TEndpointsClusterSnapshot>();
        snapshot->EndpointCount.emplace("first-set", endpointCount);
        GetService()->SetClusterSnapshot("sas", std::move(snapshot));
    };

    auto sdConfig = CreateTestServiceDiscoveryConfig(/* enableRetries */ true);
    auto sd = CreateServiceDiscovery(sdConfig);

    auto getEndpointSetSize = [&] {
        return WaitFor(sd->ResolveEndpoints("sas", "first-set"))
            .ValueOrThrow()
            .Endpoints
            .size();
    };

    setSnapshot(5);
    EXPECT_EQ(5u, getEndpointSetSize());

    setSnapshot(3);
    for (int i = 0; i < 3; ++i) {
        EXPECT_EQ(5u, getEndpointSetSize());
    }

    TDelayedExecutor::WaitForDuration(2 * (*sdConfig->RefreshTime));
    EXPECT_EQ(3u, getEndpointSetSize());
}

TEST_F(TYPServiceDiscoveryTest, UsingLastSuccessfulResponse)
{
    auto guard = Finally([&] { GetService()->SetDisabled(false); });

    auto sd = CreateServiceDiscovery(CreateTestServiceDiscoveryConfig());

    auto getEndpointSetSize = [&] {
        return WaitFor(sd->ResolveEndpoints("sas", "first-set"))
            .ValueOrThrow()
            .Endpoints
            .size();
    };

    {
        auto snapshot = std::make_unique<TEndpointsClusterSnapshot>();
        snapshot->EndpointCount.emplace("first-set", 3);
        GetService()->SetClusterSnapshot("sas", std::move(snapshot));
    }

    GetService()->SetDisabled(true);
    EXPECT_THROW(getEndpointSetSize(), TErrorException);
    GetService()->SetDisabled(false);

    // Wait for cached value eviction.
    WaitForPredicate([&] {
        try {
            return 3 == getEndpointSetSize();
        } catch (const std::exception& ex) {
            return false;
        }
    });

    GetService()->SetDisabled(true);
    for (int i = 0; i < 10; ++i) {
        EXPECT_EQ(3u, getEndpointSetSize());
        TDelayedExecutor::WaitForDuration(TDuration::Seconds(3));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYP::NServiceDiscovery::NYP
