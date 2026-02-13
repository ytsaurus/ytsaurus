#include <yt/yt/library/gpu/config.h>
#include <yt/yt/library/gpu/gpu_info_provider.h>

#include <yt/yt/core/concurrency/thread_pool.h>

#include <yt/yt/core/rpc/server.h>
#include <yt/yt/core/rpc/service_detail.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/rpc/grpc/config.h>
#include <yt/yt/core/rpc/grpc/channel.h>
#include <yt/yt/core/rpc/grpc/server.h>
#include <yt/yt/core/rpc/grpc/proto/grpc.pb.h>

#include <yt/yt/core/yson/string.h>

#include <yt/yt/core/ytree/convert.h>

#include <library/cpp/testing/common/network.h>

#include <yt/yt/gpuagent/api/api.pb.h>

namespace NYT::NGpu {
namespace {

using namespace NYson;
using namespace NYTree;
using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

static const std::string ServiceName = "GpuAgent";

////////////////////////////////////////////////////////////////////////////////

using TReqListGpuDevices = NYT::NGpuAgent::NProto::ListGpuDevicesRequest;
using TRspListGpuDevices = NYT::NGpuAgent::NProto::ListGpuDevicesResponse;

class TMockGpuAgentService
    : public NRpc::TServiceBase
{
public:
    explicit TMockGpuAgentService(IInvokerPtr invoker)
        : TServiceBase(
            invoker,
            NRpc::TServiceDescriptor(ServiceName),
            NLogging::TLogger("TMockGpuagentService"))
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ListGpuDevices));
    }

    DECLARE_RPC_SERVICE_METHOD(NYT::NGpu, ListGpuDevices)
    {
        static std::atomic<int> callCount;

        if (++callCount % 2 == 0) {
            context->Reply(TError(NRpc::EErrorCode::TransportError, "Flaky channel"));
            return;
        }

        {
            auto* dev = response->add_devices();
            dev->set_uuid("dev1");
            dev->set_power_limit(123);
            dev->set_memory_total(123_MB);
            dev->set_minor(117);
            dev->set_gpu_utilization_rate(50);
            dev->set_memory_utilization_rate(25);
            dev->set_memory_used(100_MB);
            dev->set_power_usage(100);
            dev->set_clock_sm(30.0);
            dev->add_slowdowns(NYT::NGpuAgent::NProto::SlowdownType::SWThermal);
            dev->add_slowdowns(NYT::NGpuAgent::NProto::SlowdownType::HWThermal);
        }

        {
            auto* dev = response->add_devices();
            dev->set_uuid("dev2");
            dev->set_power_limit(234);
            dev->set_memory_total(234_MB);
            dev->set_minor(225);
            dev->set_gpu_utilization_rate(75);
            dev->set_memory_utilization_rate(50);
            dev->set_memory_used(200_MB);
            dev->set_power_usage(200);
            dev->set_clock_sm(35.0);
            dev->add_slowdowns(NYT::NGpuAgent::NProto::SlowdownType::HW);
            dev->add_slowdowns(NYT::NGpuAgent::NProto::SlowdownType::HWPowerBrake);
        }

        context->Reply();
    }
};

class TGpuAgentGpuInfoProviderTest
    : public ::testing::Test
{
public:
    void SetUp() final
    {
        Port_ = NTesting::GetFreePort();
        Address_ = Format("localhost:%v", Port_);

        Server_ = CreateServer(Port_);
        WorkerPool_ = NConcurrency::CreateThreadPool(4, "Worker");
        GpuAgentService_ = New<TMockGpuAgentService>(WorkerPool_->GetInvoker());
        Server_->RegisterService(GpuAgentService_);
        Server_->Start();
    }

    void TearDown() final
    {
        Server_->Stop().Get().ThrowOnError();
        Server_.Reset();
    }

    IServerPtr CreateServer(ui16 port)
    {
        auto serverAddressConfig = New<NGrpc::TServerAddressConfig>();
        auto address = Format("localhost:%v", port);
        serverAddressConfig->Address = address;
        auto serverConfig = New<NGrpc::TServerConfig>();
        serverConfig->Addresses.push_back(serverAddressConfig);
        return NGrpc::CreateServer(serverConfig);
    }

protected:
    NTesting::TPortHolder Port_;
    std::string Address_;

    NConcurrency::IThreadPoolPtr WorkerPool_;
    IServicePtr GpuAgentService_;
    IServerPtr Server_;
};

TEST_F(TGpuAgentGpuInfoProviderTest, SimpleGpuInfo)
{
    auto config = TGpuInfoProviderConfig(EGpuInfoProviderType::GpuAgent);
    auto gpuAgentConfig = config.TryGetConcrete<EGpuInfoProviderType::GpuAgent>();
    gpuAgentConfig->Address = Address_;
    gpuAgentConfig->ServiceName = ServiceName;
    gpuAgentConfig->Channel->RetryBackoffTime = TDuration::MilliSeconds(500);

    auto provider = CreateGpuInfoProvider(config);

    // Two iterations to test retries.
    for (int iteration = 0; iteration < 1; ++iteration) {
        auto gpuInfos = provider->GetGpuInfos(TDuration::Max());

        {
            const auto& gpuInfo = gpuInfos[0];
            EXPECT_EQ(gpuInfo.Index, 117);
            EXPECT_EQ(gpuInfo.Name, "dev1");
            EXPECT_EQ(gpuInfo.UtilizationGpuRate, 0.50);
            EXPECT_EQ(gpuInfo.UtilizationMemoryRate, 0.25);
            EXPECT_EQ(gpuInfo.MemoryUsed, static_cast<i64>(100_MB));
            EXPECT_EQ(gpuInfo.MemoryTotal, static_cast<i64>(123_MB));
            EXPECT_EQ(gpuInfo.PowerDraw, 100);
            EXPECT_EQ(gpuInfo.PowerLimit, 123);
            EXPECT_EQ(gpuInfo.ClocksSM, 30);
            EXPECT_FALSE(gpuInfo.Slowdowns[ESlowdownType::HW]);
            EXPECT_FALSE(gpuInfo.Slowdowns[ESlowdownType::HWPowerBrake]);
            EXPECT_TRUE(gpuInfo.Slowdowns[ESlowdownType::HWThermal]);
            EXPECT_TRUE(gpuInfo.Slowdowns[ESlowdownType::SWThermal]);
        }

        {
            const auto& gpuInfo = gpuInfos[1];
            EXPECT_EQ(gpuInfo.Index, 225);
            EXPECT_EQ(gpuInfo.Name, "dev2");
            EXPECT_EQ(gpuInfo.UtilizationGpuRate, 0.75);
            EXPECT_EQ(gpuInfo.UtilizationMemoryRate, 0.50);
            EXPECT_EQ(gpuInfo.MemoryUsed, static_cast<i64>(200_MB));
            EXPECT_EQ(gpuInfo.MemoryTotal, static_cast<i64>(234_MB));
            EXPECT_EQ(gpuInfo.PowerDraw, 200);
            EXPECT_EQ(gpuInfo.PowerLimit, 234);
            EXPECT_EQ(gpuInfo.ClocksSM, 35);
            EXPECT_TRUE(gpuInfo.Slowdowns[ESlowdownType::HW]);
            EXPECT_TRUE(gpuInfo.Slowdowns[ESlowdownType::HWPowerBrake]);
            EXPECT_FALSE(gpuInfo.Slowdowns[ESlowdownType::HWThermal]);
            EXPECT_FALSE(gpuInfo.Slowdowns[ESlowdownType::SWThermal]);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NGpu
