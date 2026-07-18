#include "authenticator.h"
#include "internal_urls.h"
#include "private.h"

#include <yt/yt/flow/library/cpp/client/authentication.h>
#include <yt/yt/flow/library/cpp/client/public.h>

#include <yt/yt/library/tvm/service/config.h>
#include <yt/yt/library/tvm/service/tvm_service.h>
#include <yt/yt/library/tvm/tvm_base.h>

#include <yt/yt/library/signature/common/crypto.h>
#include <yt/yt/library/signature/validation/config.h>
#include <yt/yt/library/signature/validation/cypress_key_reader.h>
#include <yt/yt/library/signature/validation/signature_validator.h>

#include <yt/yt/client/cache/cache.h>
#include <yt/yt/client/cache/config.h>
#include <yt/yt/client/cache/rpc.h>
#include <yt/yt/client/signature/signature.h>
#include <yt/yt/client/signature/validator.h>

#include <yt/yt_proto/yt/client/misc/proto/signature.pb.h>
#include <yt/yt_proto/yt/core/rpc/proto/rpc.pb.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/scheduler_api.h>
#include <yt/yt/core/crypto/crypto.h>
#include <yt/yt/core/http/client.h>
#include <yt/yt/core/http/helpers.h>
#include <yt/yt/core/json/json_parser.h>
#include <yt/yt/core/logging/log.h>
#include <yt/yt/core/rpc/authenticator.h>
#include <yt/yt/core/rpc/channel_detail.h>
#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/core/yson/string.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>

#include <util/string/split.h>
#include <util/system/compiler.h>
#include <util/system/env.h>

namespace NYT::NFlow {

using namespace NApi;
using namespace NAuth;
using namespace NClient::NCache;
using namespace NConcurrency;
using namespace NCrypto;
using namespace NLogging;
using namespace NProfiling;
using namespace NYPath;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = FlowAuthenticatorLogger;

constexpr auto AuthInfoRequestTimeout = TDuration::Seconds(5);

//! Authentication result user and realms reported for requests forwarded by the RPC proxy.
constexpr std::string_view ProxySignatureUser = "yt-proxy";
constexpr std::string_view ProxySignatureRealm = "yt-proxy-signature";
constexpr std::string_view ProxySignatureBypassRealm = "yt-proxy-signature-bypass";

constexpr auto& TvmInfoUrlPrefix = NInternalUrls::TvmInfoUrlPrefix;

std::string FormatTvmInfoUrl(NAuth::TTvmId tvmId)
{
    if (TvmInfoUrlPrefix.empty()) {
        return {};
    }
    return Format("%v%v/info", TvmInfoUrlPrefix, tvmId);
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_REFCOUNTED_TYPE(IPipelineAuthenticator)

////////////////////////////////////////////////////////////////////////////////

class THmacTicketAuth
    : public TRefCounted
{
public:
    static constexpr TStringBuf MetadataKey = "ytflow-hmac";

    explicit THmacTicketAuth(std::string staticSecret)
        : StaticSecret_(std::move(staticSecret))
    { }

    std::string IssueTicket()
    {
        auto guard = Guard(Lock_);
        if (TicketInstant_ == TInstant::Zero() || TInstant::Now() - TicketInstant_ > TicketLifetime_ / 2) {
            TicketInstant_ = TInstant::Now();
            Ticket_ = Format("%v:%v:%v", TicketPrefix_, TicketInstant_.Seconds(), ComputeHmac(TicketInstant_));
        }
        return Ticket_;
    }

    std::string RemoveTicketSignature(const std::string& ticket)
    {
        auto [ticketInstant, hmac] = PreparseTicket(ticket);
        return Format("%v:%v", TicketPrefix_, ticketInstant.Seconds());
    }

    // Throws on validation failure.
    void ValidateTicket(const std::string& ticket)
    {
        auto [ticketInstant, hmac] = PreparseTicket(ticket);
        auto secondsDelta = std::abs(TInstant::Now().SecondsFloat() - ticketInstant.SecondsFloat());
        if (secondsDelta > TicketLifetime_.Seconds()) {
            THROW_ERROR_EXCEPTION("Ticket expired (TicketTimestamp: %v, DeltaSeconds: %v, LifeTimeSeconds: %v)",
                ticketInstant,
                secondsDelta,
                TicketLifetime_.Seconds());
        }
        if (ComputeHmac(ticketInstant) != hmac) {
            THROW_ERROR_EXCEPTION("Wrong ticket hmac");
        }
    }

private:
    static constexpr TStringBuf TicketPrefix_ = "ytflow_hmac_v0";
    static constexpr TDuration TicketLifetime_ = TDuration::Minutes(10);

    const std::string StaticSecret_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    TInstant TicketInstant_ = TInstant::Zero();
    std::string Ticket_;

private:
    std::string ComputeHmac(TInstant ticketInstant)
    {
        TSha256Hasher hasher;
        hasher.Append(TicketPrefix_);
        hasher.Append(ToString(ticketInstant.Seconds()));
        hasher.Append(StaticSecret_);
        return hasher.GetHexDigestLowerCase();
    }

    std::pair<TInstant, std::string> PreparseTicket(const std::string& ticket)
    {
        std::string prefix;
        ui64 instantSeconds = 0;
        std::string hmac;
        StringSplitter(ticket).Split(':').CollectInto(&prefix, &instantSeconds, &hmac);
        if (prefix != TicketPrefix_) {
            constexpr int maxPrefixPrintLength = 8;
            if (prefix.size() > maxPrefixPrintLength) {
                prefix = prefix.substr(0, maxPrefixPrintLength) + "..."; // Avoid printing full secret, if it is bad.
            }
            THROW_ERROR_EXCEPTION("Wrong ticket prefix (Got: %v, Expected: %v)", prefix, TicketPrefix_);
        }
        return {TInstant::Seconds(instantSeconds), hmac};
    }
};

using THmacTicketAuthPtr = TIntrusivePtr<THmacTicketAuth>;

////////////////////////////////////////////////////////////////////////////////

class THmacTicketInjectingChannel
    : public NRpc::TChannelWrapper
{
public:
    THmacTicketInjectingChannel(NRpc::IChannelPtr underlying, THmacTicketAuthPtr hmacTicketAuth)
        : NRpc::TChannelWrapper(std::move(underlying))
        , HmacTicketAuth_(std::move(hmacTicketAuth))
    { }

    NRpc::IClientRequestControlPtr Send(
        NRpc::IClientRequestPtr request,
        NRpc::IClientResponseHandlerPtr responseHandler,
        const NRpc::TSendOptions& options) override
    {
        auto* ext = request->Header().MutableExtension(
            NRpc::NProto::TCustomMetadataExt::custom_metadata_ext);
        (*ext->mutable_entries())[std::string(THmacTicketAuth::MetadataKey)] = HmacTicketAuth_->IssueTicket();
        return TChannelWrapper::Send(std::move(request), std::move(responseHandler), options);
    }

private:
    const THmacTicketAuthPtr HmacTicketAuth_;
};

class THmacTicketInjectingChannelFactory
    : public NRpc::IChannelFactory
{
public:
    THmacTicketInjectingChannelFactory(NRpc::IChannelFactoryPtr underlying, THmacTicketAuthPtr hmacTicketAuth)
        : Underlying_(std::move(underlying))
        , HmacTicketAuth_(std::move(hmacTicketAuth))
    { }

    NRpc::IChannelPtr CreateChannel(const std::string& address) override
    {
        return New<THmacTicketInjectingChannel>(Underlying_->CreateChannel(address), HmacTicketAuth_);
    }

private:
    const NRpc::IChannelFactoryPtr Underlying_;
    const THmacTicketAuthPtr HmacTicketAuth_;
};

////////////////////////////////////////////////////////////////////////////////

class THmacTicketAuthenticator
    : public NRpc::IAuthenticator
{
public:
    explicit THmacTicketAuthenticator(THmacTicketAuthPtr hmacTicketAuth)
        : HmacTicketAuth_(std::move(hmacTicketAuth))
    {
        YT_VERIFY(HmacTicketAuth_);
    }

    bool CanAuthenticate(const NRpc::TAuthenticationContext& context) override
    {
        if (!context.Header->HasExtension(NRpc::NProto::TCustomMetadataExt::custom_metadata_ext)) {
            return false;
        }
        const auto& ext = context.Header->GetExtension(NRpc::NProto::TCustomMetadataExt::custom_metadata_ext);
        return ext.entries().contains(std::string(THmacTicketAuth::MetadataKey));
    }

    TFuture<NRpc::TAuthenticationResult> AsyncAuthenticate(
        const NRpc::TAuthenticationContext& context) override
    {
        std::string ticketWithoutSignature = "unknown";

        try {
            const auto& ext = context.Header->GetExtension(NRpc::NProto::TCustomMetadataExt::custom_metadata_ext);
            const auto& ticket = ext.entries().at(std::string(THmacTicketAuth::MetadataKey));

            ticketWithoutSignature = HmacTicketAuth_->RemoveTicketSignature(ticket);
            YT_TLOG_DEBUG("Validating hmac ticket")
                .With("TicketWithoutSignature", ticketWithoutSignature);

            HmacTicketAuth_->ValidateTicket(ticket);
            YT_TLOG_DEBUG("Hmac ticket authentication successful")
                .With("TicketWithoutSignature", ticketWithoutSignature);

            NRpc::TAuthenticationResult rpcResult;
            rpcResult.User = "root"; // It is important for admin service.
            rpcResult.Realm = "ytflow_hmac";
            return MakeFuture(rpcResult);
        } catch (const std::exception& ex) {
            TError error(ex);
            YT_TLOG_DEBUG("Parsing hmac ticket failed")
                .With("TicketWithoutSignature", ticketWithoutSignature)
                .With(error);
            return MakeFuture<NRpc::TAuthenticationResult>(error);
        }
    }

private:
    const THmacTicketAuthPtr HmacTicketAuth_;
};

class TProxySignatureAuthenticator
    : public NRpc::IAuthenticator
{
public:
    TProxySignatureAuthenticator(
        NSignature::ISignatureValidatorPtr validator,
        NObjectClient::TObjectId pipelineObjectId,
        std::string controllerAddress,
        bool required)
        : Validator_(std::move(validator))
        , ExpectedPipelineObjectId_(pipelineObjectId)
        , ExpectedControllerAddress_(std::move(controllerAddress))
        , Required_(required)
    {
        YT_VERIFY(Validator_);
    }

    bool CanAuthenticate(const NRpc::TAuthenticationContext& /*context*/) override
    {
        // Always claim to be able to handle the request: when the signature is
        // missing or invalid, AsyncAuthenticate decides based on Required_
        // whether to fail or to log-and-pass.
        return true;
    }

    TFuture<NRpc::TAuthenticationResult> AsyncAuthenticate(
        const NRpc::TAuthenticationContext& context) override
    {
        try {
            auto signature = ExtractSignatureOrThrow(context);
            return Validator_->Validate(signature)
                .Apply(BIND([
                    this,
                    this_ = MakeStrong(this),
                    signature
                ] (bool isValid) -> NRpc::TAuthenticationResult {
                    if (!isValid) {
                        return OnFailure("Proxy signature cryptographic validation failed");
                    }
                    try {
                        ValidateRequestMetadataOrThrow(signature->Payload());
                    } catch (const std::exception& ex) {
                        return OnFailure(Format("Proxy request metadata validation failed: %v", ex.what()));
                    }
                    NRpc::TAuthenticationResult result;
                    result.User = std::string(ProxySignatureUser);
                    result.Realm = std::string(ProxySignatureRealm);
                    return result;
                }));
        } catch (const std::exception& ex) {
            try {
                return MakeFuture(OnFailure(Format("Proxy signature extraction failed: %v", ex.what())));
            } catch (const std::exception& innerEx) {
                return MakeFuture<NRpc::TAuthenticationResult>(TError(innerEx));
            }
        }
    }

private:
    const NSignature::ISignatureValidatorPtr Validator_;
    const NObjectClient::TObjectId ExpectedPipelineObjectId_;
    const std::string ExpectedControllerAddress_;
    const bool Required_;

    //! Reads the request metadata signature from the request header.
    NSignature::TSignaturePtr ExtractSignatureOrThrow(
        const NRpc::TAuthenticationContext& context)
    {
        if (!context.Header->HasExtension(NRpc::NProto::TCustomMetadataExt::custom_metadata_ext)) {
            THROW_ERROR_EXCEPTION("Missing TCustomMetadataExt in request header");
        }
        const auto& ext = context.Header->GetExtension(
            NRpc::NProto::TCustomMetadataExt::custom_metadata_ext);

        auto signatureIt = ext.entries().find(std::string(ControllerRequestMetadataSignatureKey));
        if (signatureIt == ext.entries().end()) {
            THROW_ERROR_EXCEPTION("Missing %Qv entry in TCustomMetadataExt",
                ControllerRequestMetadataSignatureKey);
        }

        NSignature::NProto::TSignature protoSignature;
        if (!protoSignature.ParseFromString(signatureIt->second)) {
            THROW_ERROR_EXCEPTION("Failed to parse TSignature proto from %Qv metadata",
                ControllerRequestMetadataSignatureKey);
        }
        NSignature::TSignaturePtr signature;
        FromProto(&signature, protoSignature);
        return signature;
    }

    //! Re-parses the signed metadata and confirms it addresses this pipeline and controller.
    void ValidateRequestMetadataOrThrow(const std::string& serializedMetadata)
    {
        auto metadata = ConvertTo<TIntrusivePtr<TControllerRequestMetadata>>(
            NYson::TYsonStringBuf(serializedMetadata));

        if (metadata->Method != ControllerRequestMetadataMethod) {
            THROW_ERROR_EXCEPTION("Proxy request metadata method mismatch (Expected: %v, Actual: %v)",
                ControllerRequestMetadataMethod,
                metadata->Method);
        }
        if (metadata->PipelineObjectId != ExpectedPipelineObjectId_) {
            THROW_ERROR_EXCEPTION("Proxy request metadata pipeline object id mismatch (Expected: %v, Actual: %v)",
                ExpectedPipelineObjectId_,
                metadata->PipelineObjectId);
        }
        if (metadata->ControllerAddress != ExpectedControllerAddress_) {
            THROW_ERROR_EXCEPTION("Proxy request metadata controller address mismatch (Expected: %v, Actual: %v)",
                ExpectedControllerAddress_,
                metadata->ControllerAddress);
        }
    }

    //! Either throws (when proxy signature is required) or returns a bypass result
    //! marking the request as unauthenticated by upstream.
    NRpc::TAuthenticationResult OnFailure(const std::string& reason)
    {
        if (!Required_) {
            // During rollout: do not block the request, but explicitly mark it as
            // unauthenticated by upstream proxy signature.
            YT_TLOG_TRACE("Proxy signature authentication bypassed")
                .With("Reason", reason);
            NRpc::TAuthenticationResult result;
            result.User = std::string(ProxySignatureUser);
            result.Realm = std::string(ProxySignatureBypassRealm);
            return result;
        }
        YT_TLOG_ERROR("Proxy signature authentication failed")
            .With("Reason", reason);
        THROW_ERROR_EXCEPTION(
            NRpc::EErrorCode::AuthenticationError,
            "Proxy signature authentication failed");
    }
};

////////////////////////////////////////////////////////////////////////////////

class TPipelineAuthenticator
    : public IPipelineAuthenticator
{
public:
    static inline constexpr std::string_view YTTvmAlias = "yt";
    static inline constexpr TTvmId YTTvmId{2031010};

public:
    TPipelineAuthenticator(
        NYPath::TRichYPath pipelinePath,
        std::optional<std::string> proxyRole,
        TTvmServiceConfigPtr tvmConfig,
        NHttp::IClientPtr httpClient,
        TAuthenticatorConfigPtr config,
        TNodeInfoPtr nodeInfo,
        TClientsCacheConfigPtr clientsCacheConfig)
        : PipelinePath_(std::move(pipelinePath))
        , ProxyRole_(std::move(proxyRole))
        , HttpClient_(std::move(httpClient))
        , Config_(std::move(config))
        , NodeInfo_(std::move(nodeInfo))
        , ClientsCacheConfig_(std::move(clientsCacheConfig))
    {
        // Interpret missing TVM ID environment variable as just not configured TVM.
        tvmConfig = CloneYsonStruct(tvmConfig);
        if (tvmConfig->ClientSelfIdEnv.has_value() && GetEnv(std::string(*tvmConfig->ClientSelfIdEnv)).empty()) {
            tvmConfig->ClientSelfIdEnv = std::nullopt;
        }
        if (tvmConfig->GetClientSelfId()) {
            TvmService_ = CreateDynamicTvmService(tvmConfig, TProfiler("", "yt.flow"));
        } else {
            YT_TLOG_INFO("TVM is not configured");
            TvmService_ = nullptr;
        }

        ClientOptions_ = NApi::GetClientOptionsFromEnvStatic();
        if (ClientOptions_.Token.has_value()) {
            YT_TLOG_INFO("Using OAUTH for interacting with YT");
            HmacTicketAuth_ = New<THmacTicketAuth>(Format("%v,%v", PipelinePath_, *ClientOptions_.Token));
        } else {
            THROW_ERROR_EXCEPTION_UNLESS(TvmService_, "TVM or OAUTH authentication must be configured");
            ClientOptions_ = NApi::TClientOptions::FromServiceTicketAuth(NAuth::CreateServiceTicketAuth(TvmService_, std::string{YTTvmAlias}));
            ClientOptions_.UserTag = Format("%v:%v", PipelinePath_.GetCluster().value(), PipelinePath_.GetPath());
            YT_TLOG_INFO("Using TVM for interacting with YT")
                .With("SelfTvmId", TvmService_->TryGetSelfTvmId())
                .With("YTTvmId", tvmConfig->ClientDstMap.at(std::string{YTTvmAlias}));

            HmacTicketAuth_ = New<THmacTicketAuth>(Format("%v,%v", PipelinePath_, tvmConfig->GetClientSelfSecret().value()));
        }

        YT_VERIFY(HmacTicketAuth_);
    }

    IDynamicTvmServicePtr GetTvmService() override
    {
        return TvmService_;
    }

    TClientOptions GetClientOptions() override
    {
        return ClientOptions_;
    }

    NRpc::IChannelFactoryPtr CreateSelfCredentialsInjectingChannelFactory(NRpc::IChannelFactoryPtr underlying) override
    {
        return New<THmacTicketInjectingChannelFactory>(underlying, HmacTicketAuth_);
    }

    NRpc::IAuthenticatorPtr CreateSelfRpcAuthenticator() override
    {
        return New<THmacTicketAuthenticator>(HmacTicketAuth_);
    }

    NRpc::IAuthenticatorPtr CreateYTControllerRpcAuthenticator() override
    {
        return ProxySignatureAuthenticator_.Acquire();
    }

    void Initialize()
    {
        YT_VERIFY(NodeInfo_);

        auto client = CreateClient(BuildConnectionConfig(), ClientOptions_);

        auto pipelineObjectId = FetchPipelineObjectId(client);
        const auto& controllerAddress = NodeInfo_->RpcAddress;

        // Cryptography backend must be initialized before any signature is validated.
        auto actionQueue = New<NConcurrency::TActionQueue>("FlowCryptoInit");
        WaitFor(NSignature::InitializeCryptography(actionQueue->GetInvoker()))
            .ThrowOnError();

        auto keyReaderConfig = New<NSignature::TCypressKeyReaderConfig>();
        // Same prefix the RPC/HTTP proxies use to publish public keys.
        keyReaderConfig->Path = "//sys/public_keys/by_owner";
        // The default `ClientSideCache` is not representable in the RPC proxy
        // proto, and the flow controller always talks to YT via RPC proxy.
        keyReaderConfig->CypressReadOptions->ReadFrom = NApi::EMasterChannelKind::Cache;
        auto keyReader = New<NSignature::TCypressKeyReader>(
            std::move(keyReaderConfig),
            std::move(client));

        NSignature::ISignatureValidatorPtr validator =
            New<NSignature::TSignatureValidator>(std::move(keyReader));

        auto authenticator = New<TProxySignatureAuthenticator>(
            std::move(validator),
            pipelineObjectId,
            controllerAddress,
            Config_->RequireProxySignature);

        YT_TLOG_INFO("Proxy signature validation enabled")
            .With("PipelineObjectId", pipelineObjectId)
            .With("ControllerAddress", controllerAddress)
            .With("Required", Config_->RequireProxySignature);

        ProxySignatureAuthenticator_.Store(std::move(authenticator));
    }

    std::string GetAuthDescription() override
    {
        if (ClientOptions_.Token.has_value()) {
            return Format("OAUTH token of %v", GetUserLogin());
        }
        if (TvmService_) {
            return Format("TVM service %v", GetTvmName());
        }
        return "unknown authentication method";
    }

    NYTree::IMapNodePtr RequestHttpJson(std::string url, NHttp::THeadersPtr headers = nullptr)
    {
        if (auto* resultPtr = Cache_.Find(url)) {
            return *resultPtr;
        }
        auto rsp = WaitFor(HttpClient_->Get(TYPath(url), headers).WithTimeout(AuthInfoRequestTimeout)).ValueOrThrow();
        auto body = rsp->ReadAll();
        THROW_ERROR_EXCEPTION_IF(rsp->GetStatusCode() != NHttp::EStatusCode::OK, "Failed to perform http request: %v", body);

        TMemoryInput stream(body.Begin(), body.Size());
        auto factory = CreateEphemeralNodeFactory();
        auto builder = CreateBuilderFromFactory(factory.get());
        NJson::ParseJson(&stream, builder.get(), New<NJson::TJsonFormatConfig>());
        auto result = builder->EndTree()->AsMap();
        Cache_.FindOrEmplace(url, result);
        return result;
    }

    std::string GetTvmName()
    {
        YT_VERIFY(TvmService_);
        auto tvmId = TvmService_->TryGetSelfTvmId().value_or(TTvmId(0));
        auto url = FormatTvmInfoUrl(tvmId);
        if (url.empty()) {
            return Format("%v", tvmId);
        }
        try {
            auto name = RequestHttpJson(url)->GetChildValueOrThrow<std::string>("name");
            return Format("%v (%v)", name, tvmId);
        } catch (const std::exception& ex) {
            YT_TLOG_WARNING("Failed to get TVM name by token")
                .With(ex);
            return Format("%v", tvmId);
        }
    }

    std::string GetUserLogin()
    {
        YT_VERIFY(ClientOptions_.Token.has_value());
        try {
            auto client = CreateClient(BuildConnectionConfig(), ClientOptions_);

            TGetCurrentUserOptions options;
            options.Timeout = AuthInfoRequestTimeout;
            return WaitFor(client->GetCurrentUser(options)).ValueOrThrow().User;
        } catch (const std::exception& ex) {
            YT_TLOG_WARNING("Failed to get user login by token")
                .With(ex);
            return "unknown-user";
        }
    }

    //! Builds the connection config for the authenticator's own RPC-proxy client.
    //!
    //! Reuses the node's clients_cache connection config (explicit proxy
    //! addresses, proxy discovery toggle, address resolver, etc.) so the auth
    //! client connects exactly like the rest of the node, e.g. in an IPv4-only
    //! environment.
    NRpcProxy::TConnectionConfigPtr BuildConnectionConfig()
    {
        auto clusterUrl = PipelinePath_.GetCluster().value();
        auto connectionConfig = ClientsCacheConfig_
            ? NClient::NCache::GetConnectionConfig(ClientsCacheConfig_, clusterUrl)
            : New<NRpcProxy::TConnectionConfig>();
        connectionConfig->ClusterUrl = clusterUrl;
        if (ProxyRole_) {
            connectionConfig->ProxyRole = ProxyRole_;
        }
        return connectionConfig;
    }

    //! Reads pipeline @id from Cypress with infinite retries.
    //!
    //! Initialization cannot proceed without this id (it is bound into the
    //! signature payload), so we keep retrying until success rather than
    //! propagating a transient error upwards.
    NObjectClient::TObjectId FetchPipelineObjectId(const NApi::IClientPtr& client)
    {
        TExponentialBackoffOptions backoffOptions;
        backoffOptions.InvocationCount = std::numeric_limits<decltype(backoffOptions.InvocationCount)>::max();
        backoffOptions.MaxBackoff = TDuration::Minutes(5);
        TBackoffStrategy backoffStrategy(backoffOptions);

        while (true) {
            try {
                NApi::TGetNodeOptions options;
                options.Attributes = std::vector<std::string>{std::string(IdAttribute)};
                auto node = NYTree::ConvertTo<NYTree::INodePtr>(WaitFor(
                    client->GetNode(PipelinePath_.GetPath(), options))
                        .ValueOrThrow());
                auto objectId = node->Attributes().Get<NObjectClient::TObjectId>(IdAttribute);
                YT_TLOG_INFO("Found pipeline object id")
                    .With("PipelineObjectId", objectId);
                return objectId;
            } catch (const TErrorException& ex) {
                backoffStrategy.Next();
                YT_TLOG_ERROR("Unable to read pipeline object id")
                    .With("Attempt", backoffStrategy.GetInvocationIndex() - 1)
                    .With("RetryAfter", backoffStrategy.GetBackoff())
                    .With(ex);
                TDelayedExecutor::WaitForDuration(backoffStrategy.GetBackoff());
            }
        }
    }

private:
    NYPath::TRichYPath PipelinePath_;
    std::optional<std::string> ProxyRole_;
    NHttp::IClientPtr HttpClient_;
    const TAuthenticatorConfigPtr Config_;
    const TNodeInfoPtr NodeInfo_;
    const TClientsCacheConfigPtr ClientsCacheConfig_;
    IDynamicTvmServicePtr TvmService_;
    THmacTicketAuthPtr HmacTicketAuth_;
    TClientOptions ClientOptions_;

    NConcurrency::TSyncMap<std::string, NYTree::IMapNodePtr> Cache_;

    TAtomicIntrusivePtr<NRpc::IAuthenticator> ProxySignatureAuthenticator_;
};

IPipelineAuthenticatorPtr CreatePipelineAuthenticator(
    NYPath::TRichYPath pipelinePath,
    std::optional<std::string> proxyRole,
    TTvmServiceConfigPtr tvmConfig,
    NHttp::IClientPtr httpClient,
    TAuthenticatorConfigPtr config,
    TNodeInfoPtr nodeInfo,
    TClientsCacheConfigPtr clientsCacheConfig)
{
    auto authenticator = New<TPipelineAuthenticator>(
        std::move(pipelinePath),
        std::move(proxyRole),
        std::move(tvmConfig),
        std::move(httpClient),
        std::move(config),
        std::move(nodeInfo),
        std::move(clientsCacheConfig));
    authenticator->Initialize();
    return authenticator;
}

////////////////////////////////////////////////////////////////////////////////

void TAuthenticatorConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("require_proxy_signature", &TThis::RequireProxySignature)
        // TODO(pechatnov): Flip the default to true once proxy signatures are rolled out to all prod Flow clusters (YTFLOW-276).
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

struct TDefaultTvmAliases final
{
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock);
    bool AllRegistered = false;
    THashMap<std::string, NAuth::TTvmId> Aliases = {
        {std::string{TPipelineAuthenticator::YTTvmAlias}, TPipelineAuthenticator::YTTvmId},
        {"tracing", TTvmId(2039211)},
    };

    static TDefaultTvmAliases& Get()
    {
        static TDefaultTvmAliases instance;
        return instance;
    }
};

void RegisterDefaultTvmAlias(const std::string& tvmAlias, NAuth::TTvmId tvmId)
{
    auto guard = Guard(TDefaultTvmAliases::Get().Lock);
    YT_VERIFY(!TDefaultTvmAliases::Get().AllRegistered, "Default TVM alias can not be registered after first call of GetDefaultTvmAliases");
    EmplaceOrCrash(TDefaultTvmAliases::Get().Aliases, tvmAlias, tvmId);
}

const THashMap<std::string, NAuth::TTvmId>& GetDefaultTvmAliases()
{
    auto guard = Guard(TDefaultTvmAliases::Get().Lock);
    TDefaultTvmAliases::Get().AllRegistered = true;
    return TDefaultTvmAliases::Get().Aliases;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
