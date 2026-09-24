#include "flow_execute.h"

#include <yt/yt/flow/library/cpp/client/authentication.h>
#include <yt/yt/flow/library/cpp/client/controller/controller_service_proxy.h>
#include <yt/yt/flow/library/cpp/client/public.h>
#include <yt/yt/flow/library/cpp/common/control_table.h>
#include <yt/yt/flow/library/cpp/misc/node_info.h>
#include <yt/yt/flow/library/cpp/native_client/public.h>

#include <yt/yt/library/auth/credentials_injecting_channel.h>

#include <yt/yt/client/api/client.h>

#include <yt/yt/core/bus/tcp/config.h>

#include <yt/yt/core/compression/codec.h>

#include <yt/yt/core/crypto/config.h>

#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/rpc/bus/channel.h>
#include <yt/yt/core/rpc/caching_channel_factory.h>

#include <yt/yt/core/ypath/helpers.h>

#include <yt/yt/core/yson/protobuf_helpers.h>

#include <yt/yt/core/ytree/convert.h>

#include <util/generic/algorithm.h>

#include <library/cpp/yt/threading/spin_lock.h>

namespace NYT::NFlow {

using namespace NApi;
using namespace NController;
using namespace NRpc;
using namespace NYPath;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void TDirectControllerCommandsConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enabled", &TThis::Enabled)
        .Default(false);
    // The same as the rpc_timeout of the RPC proxy connection, which serves these commands
    // on the proxy path.
    registrar.Parameter("rpc_timeout", &TThis::RpcTimeout)
        .Default(TDuration::Seconds(30));
}

////////////////////////////////////////////////////////////////////////////////

namespace {

const NLogging::TLogger Logger("FlowClient");

//! The commands the runner sends while releasing a pipeline; only these bypass the RPC proxy.
const THashSet<std::string> DirectCommands{
    "get-pipeline-state",
    "set-target-pipeline-state",
    "set-pipeline-specs",
    "set-flow-core-target",
};

TFuture<TFlowExecuteResult> DirectFlowExecute(
    const IClientPtr& client,
    const IDirectControllerChannelsPtr& channels,
    const TYPath& pipelinePath,
    const std::string& command,
    const TYsonString& argument,
    const NApi::TFlowExecuteOptions& options)
{
    if (!DirectCommands.contains(command)) {
        return MakeFuture<TFlowExecuteResult>(
            TError("Command %Qv cannot be sent to the pipeline controller directly", command)
                .With("direct_commands", DirectCommands));
    }

    return TControlTable::Read(client, YPathJoin(pipelinePath, FlowControlTableName), LeaderControllerKey)
        .Apply(BIND([client, channels, pipelinePath, command, argument, options] (const std::optional<TYsonString>& leaderInfo) {
            // The controller publishes its node info to the leader row of the flow_control table.
            auto leader = leaderInfo ? ConvertTo<TNodeInfoPtr>(*leaderInfo) : New<TNodeInfo>();
            const auto& address = leader->RpcAddress;
            if (address.empty()) {
                THROW_ERROR_EXCEPTION(
                    "Cannot discover pipeline controller of %v because no leader is published to the %v table. "
                    "Probably pipeline controller has never been successfully started or has been unable to publish itself",
                    pipelinePath,
                    FlowControlTableName);
            }

            // The credentials of the client go to the controller: it authenticates the caller by them.
            auto channel = NAuth::CreateCredentialsInjectingChannel(
                channels->GetChannel(*leader),
                client->GetOptions());

            TControllerServiceProxy proxy(std::move(channel));

            auto req = proxy.FlowExecute();
            MarkDirectRequest(&req->Header());
            if (options.Timeout) {
                req->SetTimeout(options.Timeout);
            }
            req->set_command(command);
            if (argument) {
                ToProto(req->mutable_argument(), argument);
            }
            // The controller derives the user from the credentials; the field is not needed.

            return req->Invoke()
                .Apply(BIND([pipelinePath, command, address, certificateSha256 = leader->CertificateSha256] (
                    const TControllerServiceProxy::TErrorOrRspFlowExecutePtr& rspOrError) {
                    if (rspOrError.GetCode() == NRpc::EErrorCode::TransportError) {
                        THROW_ERROR_EXCEPTION(
                            "Cannot connect to pipeline controller leader directly. "
                            "Probably controller is stopped or it is failing")
                            .With("flow_execute_command", command)
                            .With("pipeline_path", pipelinePath)
                            .With("pipeline_controller_leader_address", address)
                            .With("pipeline_controller_leader_certificate_sha256", certificateSha256.value_or(""))
                            .With(rspOrError);
                    }
                    const auto& rsp = rspOrError.ValueOrThrow();
                    return TFlowExecuteResult{
                        .Result = rsp->has_result() ? TYsonString(rsp->result()) : TYsonString(),
                    };
                }));
        }));
}

////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////

//! A leader that publishes its incarnation certificate is reached over TLS with that certificate
//! pinned; a leader that publishes none, with the default bus config.
class TDirectControllerChannels
    : public IDirectControllerChannels
{
public:
    IChannelPtr GetChannel(const TNodeInfo& leader) override
    {
        if (!leader.CertificatePem) {
            // No certificate is published by a controller of an older binary or by one whose bus server
            // TLS is configured explicitly: the default bus config connects to it without verification.
            return PlainChannelFactory_->CreateChannel(leader.RpcAddress);
        }

        {
            auto guard = Guard(Lock_);
            if (PinnedCertificatePem_ == *leader.CertificatePem) {
                return PinnedChannelFactory_->CreateChannel(leader.RpcAddress);
            }
        }

        // The published certificate is the only trust anchor: nobody but the incarnation that published
        // it holds its private key. It is a self-signed leaf, so the address is not checked against it.
        auto certificateAuthority = New<NCrypto::TPemBlobConfig>();
        certificateAuthority->Value = *leader.CertificatePem;
        auto config = New<NYT::NBus::NTcp::TBusConfig>();
        config->EncryptionMode = NYT::NBus::EEncryptionMode::Required;
        config->VerificationMode = NYT::NBus::EVerificationMode::Ca;
        config->CertificateAuthority = std::move(certificateAuthority);
        auto channelFactory = CreateCachingChannelFactory(NRpc::NBus::CreateTcpBusChannelFactory(std::move(config)));

        IChannelFactoryPtr previousChannelFactory;
        {
            auto guard = Guard(Lock_);
            PinnedCertificatePem_ = *leader.CertificatePem;
            previousChannelFactory = std::exchange(PinnedChannelFactory_, channelFactory);
        }
        // Null when this is the first pinned certificate.
        std::optional<bool> previousChannelFactoryReleased;
        if (previousChannelFactory) {
            TWeakPtr<IChannelFactory> weakPreviousChannelFactory = previousChannelFactory;
            previousChannelFactory.Reset();
            previousChannelFactoryReleased = weakPreviousChannelFactory.IsExpired();
        }
        YT_TLOG_DEBUG("Pinned the certificate of a leader controller incarnation")
            .With("Address", leader.RpcAddress)
            .With("CertificateSha256", leader.CertificateSha256.value_or(""))
            .With("PreviousChannelFactoryReleased", previousChannelFactoryReleased);
        return channelFactory->CreateChannel(leader.RpcAddress);
    }

private:
    const IChannelFactoryPtr PlainChannelFactory_ = CreateCachingChannelFactory(
        NRpc::NBus::CreateTcpBusChannelFactory(New<NYT::NBus::NTcp::TBusConfig>()));

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    //! The certificate of the last leader and the channels pinned to it. A leader restart rotates
    //! the certificate, and the channels to the previous incarnation are dropped with their factory.
    std::string PinnedCertificatePem_;
    IChannelFactoryPtr PinnedChannelFactory_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

void TGetFlowViewArg::Register(TRegistrar registrar)
{
    registrar.Parameter("path", &TThis::Path)
        .Default("");
    registrar.Parameter("cache", &TThis::Cache)
        .Default(false);
    registrar.UnrecognizedStrategy(EUnrecognizedStrategy::Throw);
}

void TGetFlowViewV2Arg::Register(TRegistrar /*registrar*/)
{
    // Inherits path/cache (and the unrecognized-field strategy) from TGetFlowViewArg::Register.
}

void TGetFlowViewV2Result::Register(TRegistrar registrar)
{
    registrar.Parameter("codec", &TThis::Codec);
    registrar.Parameter("data", &TThis::Data);
}

TGetFlowViewResult DecompressFlowView(const TGetFlowViewV2Result& compressed)
{
    auto decompressed = NCompression::GetCodec(compressed.Codec)->Decompress(TSharedRef::FromString(compressed.Data));
    return NYson::TYsonString(decompressed);
}

IDirectControllerChannelsPtr CreateDirectControllerChannels()
{
    return New<TDirectControllerChannels>();
}

////////////////////////////////////////////////////////////////////////////////

TFlowExecuteTarget::TFlowExecuteTarget(IClientPtr client, TDirectControllerCommandsConfigPtr directControllerCommands)
    : Client(std::move(client))
    , DirectControllerCommands(std::move(directControllerCommands))
    , Channels(IsDirect() ? CreateDirectControllerChannels() : nullptr)
{ }

bool TFlowExecuteTarget::IsDirect() const
{
    return DirectControllerCommands && DirectControllerCommands->Enabled;
}

////////////////////////////////////////////////////////////////////////////////

NYson::TYsonString FlowExecute(
    const TFlowExecuteTarget& target,
    const NYPath::TYPath& pipelinePath,
    const std::string& command,
    const NYson::TYsonString& argument,
    const NApi::TFlowExecuteOptions& options)
{
    if (!target.IsDirect()) {
        return NConcurrency::WaitFor(target.Client->FlowExecute(pipelinePath, command, argument, options))
            .ValueOrThrow()
            .Result;
    }

    auto directOptions = options;
    if (!directOptions.Timeout) {
        directOptions.Timeout = target.DirectControllerCommands->RpcTimeout;
    }
    return NConcurrency::WaitFor(DirectFlowExecute(
        target.Client,
        target.Channels,
        pipelinePath,
        command,
        argument,
        directOptions))
        .ValueOrThrow()
        .Result;
}

////////////////////////////////////////////////////////////////////////////////

TGetFlowViewResult GetFlowView(
    const NApi::IClientPtr& client,
    const NYPath::TYPath& pipelinePath,
    const TGetFlowViewArg& arg)
{
    auto flowExecute = [&] (const std::string& command, const NYson::TYsonString& argument) {
        return NConcurrency::WaitFor(client->FlowExecute(pipelinePath, command, argument))
            .ValueOrThrow()
            .Result;
    };

    // The "list" command advertises the controller's supported commands; prefer the compressed v2 when present.
    auto commands = ConvertTo<std::vector<std::string>>(flowExecute("list", NYson::TYsonString(TStringBuf("#"))));
    auto argument = NYson::ConvertToYsonString(arg);
    if (IsIn(commands, "get-flow-view-v2")) {
        return DecompressFlowView(ConvertTo<TGetFlowViewV2Result>(flowExecute("get-flow-view-v2", argument)));
    }
    return flowExecute("get-flow-view", argument);
}

////////////////////////////////////////////////////////////////////////////////

void TGetPipelineDynamicSpecArg::Register(TRegistrar registrar)
{
    registrar.Parameter("path", &TThis::Path)
        .Default("");
    registrar.UnrecognizedStrategy(EUnrecognizedStrategy::Throw);
}

void TGetPipelineDynamicSpecResult::Register(TRegistrar registrar)
{
    registrar.Parameter("spec", &TThis::Spec);
    registrar.Parameter("version", &TThis::Version);
}

////////////////////////////////////////////////////////////////////////////////

void TSetPipelineDynamicSpecArg::Register(TRegistrar registrar)
{
    registrar.Parameter("spec", &TThis::Spec);
    registrar.Parameter("path", &TThis::Path)
        .Default("");
    registrar.Parameter("expected_version", &TThis::ExpectedVersion)
        .Default();
    registrar.UnrecognizedStrategy(EUnrecognizedStrategy::Throw);
}

void TSetPipelineDynamicSpecResult::Register(TRegistrar registrar)
{
    registrar.Parameter("version", &TThis::Version);
}

////////////////////////////////////////////////////////////////////////////////

void TGetPipelineSpecArg::Register(TRegistrar registrar)
{
    registrar.Parameter("path", &TThis::Path)
        .Default("");
    registrar.UnrecognizedStrategy(EUnrecognizedStrategy::Throw);
}

void TGetPipelineSpecResult::Register(TRegistrar registrar)
{
    registrar.Parameter("spec", &TThis::Spec);
    registrar.Parameter("version", &TThis::Version);
}

////////////////////////////////////////////////////////////////////////////////

void TSetPipelineSpecArg::Register(TRegistrar registrar)
{
    registrar.Parameter("spec", &TThis::Spec);
    registrar.Parameter("expected_version", &TThis::ExpectedVersion)
        .Default();
    registrar.Parameter("force", &TThis::Force)
        .Default(false);
    registrar.UnrecognizedStrategy(EUnrecognizedStrategy::Throw);
}

void TSetPipelineSpecResult::Register(TRegistrar registrar)
{
    registrar.Parameter("version", &TThis::Version);
}

////////////////////////////////////////////////////////////////////////////////

void TSetPipelineSpecsArg::Register(TRegistrar registrar)
{
    registrar.Parameter("spec", &TThis::Spec)
        .Default();
    registrar.Parameter("dynamic_spec", &TThis::DynamicSpec)
        .Default();
    registrar.Parameter("expected_spec_version", &TThis::ExpectedSpecVersion)
        .Default();
    registrar.Parameter("expected_dynamic_spec_version", &TThis::ExpectedDynamicSpecVersion)
        .Default();
    registrar.Parameter("allow_spec_update_on_pause", &TThis::AllowSpecUpdateOnPause)
        .Default(false);
    registrar.Parameter("validate_strict", &TThis::ValidateStrict)
        .Default(false);
    registrar.Parameter("force", &TThis::Force)
        .Default(false);
    registrar.Postprocessor([] (TThis* arg) {
        if (arg->Force) {
            arg->AllowSpecUpdateOnPause = true;
            arg->ValidateStrict = false;
        }
    });
    registrar.UnrecognizedStrategy(EUnrecognizedStrategy::Throw);
}

void TSetPipelineSpecsResult::Register(TRegistrar registrar)
{
    registrar.Parameter("spec_version", &TThis::SpecVersion);
    registrar.Parameter("dynamic_spec_version", &TThis::DynamicSpecVersion);
}

////////////////////////////////////////////////////////////////////////////////

void TGetPipelineStateArg::Register(TRegistrar registrar)
{
    registrar.UnrecognizedStrategy(EUnrecognizedStrategy::Throw);
}

void TGetPipelineStateResult::Register(TRegistrar registrar)
{
    registrar.Parameter("pipeline_state", &TThis::PipelineState);
}

////////////////////////////////////////////////////////////////////////////////

void TSetTargetPipelineStateArg::Register(TRegistrar registrar)
{
    registrar.Parameter("target_pipeline_state", &TThis::TargetPipelineState);
    registrar.UnrecognizedStrategy(EUnrecognizedStrategy::Throw);
}

void TSetTargetPipelineStateResult::Register(TRegistrar /*registrar*/)
{ }

////////////////////////////////////////////////////////////////////////////////

void TGetControllerOrchidArg::Register(TRegistrar registrar)
{
    registrar.Parameter("path", &TThis::Path)
        .Default("");
    registrar.UnrecognizedStrategy(EUnrecognizedStrategy::Throw);
}

void TGetControllerOrchidResult::Register(TRegistrar registrar)
{
    registrar.Parameter("value", &TThis::Value)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TGetFlowCoreTargetArg::Register(TRegistrar registrar)
{
    registrar.UnrecognizedStrategy(EUnrecognizedStrategy::Throw);
}

void TGetFlowCoreTargetResult::Register(TRegistrar registrar)
{
    registrar.Parameter("flow_core_target", &TThis::FlowCoreTarget);
    registrar.Parameter("version", &TThis::Version);
}

////////////////////////////////////////////////////////////////////////////////

void TSetFlowCoreTargetArg::Register(TRegistrar registrar)
{
    registrar.Parameter("flow_core_target", &TThis::FlowCoreTarget);
    registrar.Parameter("allow_update_on_pause", &TThis::AllowUpdateOnPause)
        .Default(false);
    registrar.Parameter("expected_version", &TThis::ExpectedVersion)
        .Default();
    registrar.UnrecognizedStrategy(EUnrecognizedStrategy::Throw);
}

void TSetFlowCoreTargetResult::Register(TRegistrar registrar)
{
    registrar.Parameter("version", &TThis::Version);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
