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

#include <infra/rsm/nvgpumanager/api/nvgpu.pb.h>

namespace NYT::NGpu {
namespace {

using namespace NYson;
using namespace NYTree;
using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

static const TString ServiceName = "NvGpuManager";

////////////////////////////////////////////////////////////////////////////////

using TReqListGpuDevices = nvgpu::ListDevicesRequest;
using TRspListGpuDevices = nvgpu::ListResponse;

using TReqListRdmaDevices = nvgpu::Empty;
using TRspListRdmaDevices = nvgpu::RdmaDevicesListResponse;

class TMockNvGpuManagerService
    : public NRpc::TServiceBase
{
public:
    explicit TMockNvGpuManagerService(IInvokerPtr invoker)
        : TServiceBase(
            invoker,
            NRpc::TServiceDescriptor(ServiceName),
            NLogging::TLogger("TMockNvGpuManagerService"),
            NRpc::NullRealmId)
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ListGpuDevices));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ListRdmaDevices));
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
            auto* spec = dev->mutable_spec()->mutable_nvidia();
            spec->set_uuid("dev1");
            spec->set_power(123);
            spec->set_memory_size_mb(123);
            spec->set_minor(117);
            auto* status = dev->mutable_status()->mutable_nvidia();
            status->set_gpu_utilization(50);
            status->set_memory_utilization(25);
            status->set_memory_used_mb(100);
            status->set_power(100);
            status->set_sm_utilization(20.0);
            status->set_sm_occupancy(10.0);
            status->set_nvlink_rx_bytes_per_second(1000.0);
            status->set_nvlink_tx_bytes_per_second(5000.0);
            status->set_pcie_rx_bytes_per_second(100.0);
            status->set_pcie_tx_bytes_per_second(500.0);
            auto* stuck = status->mutable_stuck();
            stuck->set_status(false);
        }

        response->add_devices();

        {
            auto* dev = response->add_devices();
            auto* spec = dev->mutable_spec()->mutable_nvidia();
            spec->set_uuid("dev2");
            spec->set_power(234);
            spec->set_memory_size_mb(234);
            spec->set_minor(225);
            auto* status = dev->mutable_status()->mutable_nvidia();
            status->set_gpu_utilization(75);
            status->set_memory_utilization(50);
            status->set_memory_used_mb(200);
            status->set_power(200);
            status->set_sm_utilization(25.0);
            status->set_sm_occupancy(10.0);
            status->set_nvlink_rx_bytes_per_second(4000.0);
            status->set_nvlink_tx_bytes_per_second(2000.0);
            status->set_pcie_rx_bytes_per_second(0.0);
            status->set_pcie_tx_bytes_per_second(300.0);
            auto* stuck = status->mutable_stuck();
            stuck->set_status(true);
        }

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NYT::NGpu, ListRdmaDevices)
    {
        {
            auto* dev = response->add_devices();
            auto* spec = dev->mutable_spec();
            spec->set_name("dev1");
            auto* status = dev->mutable_status();
            status->set_portrcvbytespersecond(50);
            status->set_portxmitbytespersecond(100);
        }

        response->add_devices();

        {
            auto* dev = response->add_devices();
            auto* spec = dev->mutable_spec();
            spec->set_name("dev2");
            auto* status = dev->mutable_status();
            status->set_portrcvbytespersecond(150);
            status->set_portxmitbytespersecond(200);
        }

        context->Reply();
    }
};

class TNvManagerGpuInfoProviderTest
    : public ::testing::Test
{
public:
    void SetUp() final
    {
        Port_ = NTesting::GetFreePort();
        Address_ = Format("localhost:%v", Port_);

        Server_ = CreateServer(Port_);
        WorkerPool_ = NConcurrency::CreateThreadPool(4, "Worker");
        NvGpuManagerService_ = New<TMockNvGpuManagerService>(WorkerPool_->GetInvoker());
        Server_->RegisterService(NvGpuManagerService_);
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
    TString Address_;

    NConcurrency::IThreadPoolPtr WorkerPool_;
    IServicePtr NvGpuManagerService_;
    IServerPtr Server_;
};

TEST_F(TNvManagerGpuInfoProviderTest, SimpleGpuInfo)
{
    auto config = New<TGpuInfoSourceConfig>();
    config->NvGpuManagerServiceAddress = Address_;
    config->NvGpuManagerServiceName = ServiceName;
    config->NvGpuManagerChannel->RetryBackoffTime = TDuration::MilliSeconds(500);
    config->Type = EGpuInfoSourceType::NvGpuManager;
    config->GpuIndexesFromNvidiaSmi = false;

    auto provider = CreateGpuInfoProvider(config);

    // Two iterations to test retries.
    for (int iteration = 0; iteration < 2; ++iteration) {
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
            EXPECT_EQ(gpuInfo.SMUtilizationRate, 0.2);
            EXPECT_EQ(gpuInfo.SMOccupancyRate, 0.1);
            EXPECT_EQ(gpuInfo.NvlinkRxByteRate, 1000.0);
            EXPECT_EQ(gpuInfo.NvlinkTxByteRate, 5000.0);
            EXPECT_EQ(gpuInfo.PcieRxByteRate, 100.0);
            EXPECT_EQ(gpuInfo.PcieTxByteRate, 500.0);
            EXPECT_FALSE(gpuInfo.Stuck.Status);
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
            EXPECT_EQ(gpuInfo.SMUtilizationRate, 0.25);
            EXPECT_EQ(gpuInfo.SMOccupancyRate, 0.1);
            EXPECT_EQ(gpuInfo.NvlinkRxByteRate, 4000.0);
            EXPECT_EQ(gpuInfo.NvlinkTxByteRate, 2000.0);
            EXPECT_EQ(gpuInfo.PcieRxByteRate, 0.0);
            EXPECT_EQ(gpuInfo.PcieTxByteRate, 300.0);
            EXPECT_TRUE(gpuInfo.Stuck.Status);
        }
    }
}

TEST_F(TNvManagerGpuInfoProviderTest, SimpleRdmaDeviceInfo)
{
    auto config = New<TGpuInfoSourceConfig>();
    config->NvGpuManagerServiceAddress = Address_;
    config->NvGpuManagerServiceName = ServiceName;
    config->Type = EGpuInfoSourceType::NvGpuManager;
    config->GpuIndexesFromNvidiaSmi = false;

    auto provider = CreateGpuInfoProvider(config);
    auto rdmaDeviceInfos = provider->GetRdmaDeviceInfos(TDuration::Max());

    {
        const auto& rdmaDeviceInfo = rdmaDeviceInfos[0];
        EXPECT_EQ(rdmaDeviceInfo.Name, "dev1");
        EXPECT_EQ(rdmaDeviceInfo.RxByteRate, 50);
        EXPECT_EQ(rdmaDeviceInfo.TxByteRate, 100);
    }

    {
        const auto& rdmaDeviceInfo = rdmaDeviceInfos[1];
        EXPECT_EQ(rdmaDeviceInfo.Name, "dev2");
        EXPECT_EQ(rdmaDeviceInfo.RxByteRate, 150);
        EXPECT_EQ(rdmaDeviceInfo.TxByteRate, 200);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NGpu
