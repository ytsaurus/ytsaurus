#include "config.h"

#include <yt/yt/core/crypto/config.h>
#include <yt/yt/core/rpc/grpc/config.h>

#include <library/cpp/resource/resource.h>

#include <contrib/libs/grpc/include/grpc/impl/grpc_types.h>

namespace NYT::NOrm::NClient::NNative {
namespace {

////////////////////////////////////////////////////////////////////////////////

void PatchSecurityOptions(bool secure, const NRpc::NGrpc::TChannelConfigTemplatePtr& channelConfig)
{
    if (secure) {
        if (!channelConfig->Credentials) {
            channelConfig->Credentials = New<NRpc::NGrpc::TChannelCredentialsConfig>();
        }
        auto& credentials = channelConfig->Credentials;
        if (!credentials->PemRootCerts) {
            credentials->PemRootCerts = New<NCrypto::TPemBlobConfig>();
            credentials->PemRootCerts->Value = NResource::Find("yandex_internal.pem");
        }
    } else {
        if (channelConfig->Credentials) {
            THROW_ERROR_EXCEPTION("Value of \"secure\" is false, but \"grpc_channel/credentials\" are given");
        }
    }
}

void SetGrpcDefaults(THashMap<TString, NYTree::INodePtr>& grpcArguments)
{
    grpcArguments.try_emplace(GRPC_ARG_HTTP2_MAX_PINGS_WITHOUT_DATA, NYTree::ConvertToNode(0));
    grpcArguments.try_emplace(GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS, NYTree::ConvertToNode(1));
    grpcArguments.try_emplace(GRPC_ARG_KEEPALIVE_TIME_MS, NYTree::ConvertToNode(60'000));
    grpcArguments.try_emplace(GRPC_ARG_KEEPALIVE_TIMEOUT_MS, NYTree::ConvertToNode(15'000));
    grpcArguments.try_emplace(GRPC_ARG_MAX_RECEIVE_MESSAGE_LENGTH, NYTree::ConvertToNode(128_MB));
    grpcArguments.try_emplace(GRPC_ARG_MAX_SEND_MESSAGE_LENGTH, NYTree::ConvertToNode(128_MB));
    grpcArguments.try_emplace(GRPC_ARG_MAX_METADATA_SIZE, NYTree::ConvertToNode(16_KB));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

TAdaptiveBatchSizeOptions TAdaptiveBatchSizeOptionsConfig::ToPlainStruct() const
{
    return *this;
}

void TAdaptiveBatchSizeOptionsConfig::Register(TRegistrar registrar)
{
    // NB! Defaults and validations are configured in TAdaptiveBatchSizeOptions.
    registrar.BaseClassParameter("increasing_additive", &TThis::IncreasingAdditive)
        .Optional(/*init*/ false);
    registrar.BaseClassParameter("decreasing_divisor", &TThis::DecreasingDivisor)
        .Optional(/*init*/ false);
    registrar.BaseClassParameter("max_batch_size", &TThis::MaxBatchSize)
        .Optional(/*init*/ false);
    registrar.BaseClassParameter("min_batch_size", &TThis::MinBatchSize)
        .Optional(/*init*/ false);
    registrar.BaseClassParameter("max_consecutive_retry_count", &TThis::MaxConsecutiveRetryCount)
        .Optional(/*init*/ false);

    registrar.Postprocessor(&TThis::Validate);
}

////////////////////////////////////////////////////////////////////////////////

void TConnectionConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("discovery_address", &TThis::DiscoveryAddress)
        .Optional();
    registrar.Parameter("discovery_ip6_address", &TThis::DiscoveryIP6Address)
        .Optional();
    registrar.Parameter("addresses", &TThis::Addresses)
        .Default();

    registrar.Parameter("endpoints", &TThis::Endpoints)
        .Optional();

    registrar.Parameter("secure", &TThis::Secure)
        .Default(true);

    registrar.Parameter("grpc_channel_template", &TThis::GrpcChannelTemplate)
        .Optional();
    registrar.Parameter("grpc_channel", &TThis::GrpcChannel)
        .DefaultNew();

    registrar.Parameter("dynamic_channel_pool", &TThis::DynamicChannelPool)
        .DefaultNew();
    registrar.Parameter("channel_ttl", &TThis::ChannelTtl)
        .Default(TDuration::Seconds(30));

    registrar.Parameter("request_timeout", &TThis::RequestTimeout)
        .Default(TDuration::Seconds(30));

    registrar.Postprocessor([] (TThis* config) {
        PatchSecurityOptions(config->Secure, config->GrpcChannel);

        // Using direct copy instead of alias to support loads
        // with `Throw` or `ThrowRecursive` unrecognized strategies.
        if (!config->GrpcChannelTemplate) {
            config->GrpcChannelTemplate = NYTree::ConvertTo<NRpc::NGrpc::TChannelConfigTemplatePtr>(
                config->GrpcChannel);
            YT_VERIFY(config->GrpcChannelTemplate);
        } else {
            PatchSecurityOptions(config->Secure, config->GrpcChannelTemplate);
        }

        SetGrpcDefaults(config->GrpcChannelTemplate->GrpcArguments);

        if (config->DiscoveryIP6Address) {
            THROW_ERROR_EXCEPTION_UNLESS(config->DiscoveryAddress, "Discovery address must be provided");
        }
    });
}

void TConnectionConfig::SetDefaultDomain(
    TRegistrar registrar,
    TStringBuf defaultDomain,
    ui16 defaultGrpcPort)
{
    if (defaultDomain.empty()) {
        return;
    }

    registrar.Postprocessor([defaultDomain, defaultGrpcPort] (TThis* config) {
        if (!config->DiscoveryAddress) {
            return;
        }

        auto address = *config->DiscoveryAddress;
        YT_VERIFY(!address.Empty());
        if (!address.Contains('.') && !address.Contains(':') && address != "localhost") {
            address = Format("%v%v:%v", address, defaultDomain, defaultGrpcPort);
        }

        config->DiscoveryAddress = address;
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NClient::NNative
