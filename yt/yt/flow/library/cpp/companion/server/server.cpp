#include "server.h"

#include "companion_service.h"
#include "monitoring.h"

#include "private.h"

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/concurrency/thread_pool.h>

#include <yt/yt/core/rpc/grpc/config.h>
#include <yt/yt/core/rpc/grpc/server.h>

#include <yt/yt/core/rpc/server.h>

#include <yt/yt/core/ytree/convert.h>

#include <util/system/info.h>

namespace NYT::NFlow::NCompanionServer {

using namespace NConcurrency;

constinit const auto Logger = CompanionServerLogger;

////////////////////////////////////////////////////////////////////////////////

namespace {

NRpc::NGrpc::TServerConfigPtr BuildGrpcServerConfig(int port)
{
    auto addressConfig = New<NRpc::NGrpc::TServerAddressConfig>();
    addressConfig->Address = Format("0.0.0.0:%v", port);

    auto serverConfig = New<NRpc::NGrpc::TServerConfig>();
    serverConfig->Addresses.push_back(std::move(addressConfig));
    // Match worker-side gRPC message-size limits.
    static constexpr i64 MaxMessageLength = std::numeric_limits<i32>::max();
    serverConfig->GrpcArguments["grpc.max_send_message_length"] =
        NYTree::ConvertToNode(MaxMessageLength);
    serverConfig->GrpcArguments["grpc.max_receive_message_length"] =
        NYTree::ConvertToNode(MaxMessageLength);
    return serverConfig;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TCompanionServer::TCompanionServer(
    NCompanion::TCompanionExecutionConfigPtr config,
    TPipeline pipeline,
    NProfiling::TSolomonRegistryPtr registry)
    : Config_(std::move(config))
    , Monitoring_(New<TCompanionMonitoring>(Config_, registry))
{
    ThreadPool_ = CreateThreadPool(
        static_cast<int>(NSystemInfo::CachedNumberOfCpus()),
        "Companion");
    RpcServer_ = NRpc::NGrpc::CreateServer(BuildGrpcServerConfig(Config_->Port));
    RpcServer_->RegisterService(CreateCompanionService(
        std::move(pipeline),
        ThreadPool_->GetInvoker(),
        std::move(registry)));
}

void TCompanionServer::Start()
{
    YT_TLOG_INFO("Starting companion server")
        .With("Port", Config_->Port);
    RpcServer_->Start();
    Monitoring_->Start();
}

void TCompanionServer::Stop()
{
    YT_TLOG_INFO("Stopping companion server");
    // NB: Stop is called from the plain main thread at shutdown, not from a fiber.
    RpcServer_->Stop().BlockingGet().ThrowOnError();
    ThreadPool_->Shutdown();
    Monitoring_->Stop();
}

const TCompanionMonitoringPtr& TCompanionServer::GetMonitoring() const
{
    return Monitoring_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanionServer
