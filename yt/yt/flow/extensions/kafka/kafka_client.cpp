#include "kafka_client.h"

#include "private.h"

#include <util/system/env.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

void TKafkaClientConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("bootstrap_servers", &TThis::BootstrapServers)
        .NonEmpty();
    registrar.Parameter("cluster_name", &TThis::ClusterName)
        .Default();
    registrar.Parameter("security_protocol", &TThis::SecurityProtocol)
        .Default("PLAINTEXT");
    registrar.Parameter("sasl_mechanism", &TThis::SaslMechanism)
        .Default();
    registrar.Parameter("sasl_username", &TThis::SaslUsername)
        .Default();
    registrar.Parameter("sasl_password_env", &TThis::SaslPasswordEnv)
        .Default(std::string{KafkaSaslPasswordEnv});
    registrar.Parameter("ssl_ca_location", &TThis::SslCaLocation)
        .Default();
    registrar.Parameter("extra_config", &TThis::ExtraConfig)
        .Default();

    registrar.Postprocessor([] (TThis* config) {
        // Keys the connector owns (first-class parameters and its correctness contract); a value
        // here would silently override or be overridden. librdkafka resolves aliases to one
        // property, so both spellings of an owned key are listed.
        static const std::vector<std::string> reservedKeys{
            "bootstrap.servers",
            "metadata.broker.list", // Canonical name; bootstrap.servers is its alias.
            "security.protocol",
            "sasl.mechanism",
            "sasl.mechanisms", // Canonical name; sasl.mechanism is its alias.
            "sasl.username",
            "sasl.password",
            "ssl.ca.location",
            "group.id",
            "enable.auto.commit",
            "auto.offset.reset",
            "enable.idempotence",
            "client.id",
            // The sink resolves futures and advances persistence only from delivery reports;
            // suppressing the successful ones would leave every write pending forever.
            "delivery.report.only.error",
            // Mirrored from max_buffer_bytes by the read session.
            "queued.max.messages.kbytes",
            // A metadata request must not create the topic it asks about; see TKafkaInfoController.
            "allow.auto.create.topics",
        };
        for (const auto& key : reservedKeys) {
            THROW_ERROR_EXCEPTION_IF(config->ExtraConfig.contains(key),
                "The %Qv key %Qv is managed by the connector and cannot be overridden",
                "extra_config",
                key);
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

const std::string& GetKafkaClusterIdentity(const TKafkaClientConfigPtr& config)
{
    return config->ClusterName.empty() ? config->BootstrapServers : config->ClusterName;
}

////////////////////////////////////////////////////////////////////////////////

TKafkaClient::TKafkaClient(TResourceContextPtr context, TDynamicResourceContextPtr dynamicContext)
    : TResourceBase(std::move(context), std::move(dynamicContext))
    , ClusterIdentity_(GetKafkaClusterIdentity(GetParameters()))
    , SaslUsername_(GetParameters()->SaslUsername)
    , BaseConfig_([&] {
        const auto& parameters = GetParameters();

        std::vector<std::pair<std::string, std::string>> config;
        config.emplace_back("bootstrap.servers", parameters->BootstrapServers);
        config.emplace_back("security.protocol", parameters->SecurityProtocol);

        if (!parameters->SaslMechanism.empty()) {
            config.emplace_back("sasl.mechanism", parameters->SaslMechanism);
        }
        if (!parameters->SaslUsername.empty()) {
            config.emplace_back("sasl.username", parameters->SaslUsername);
        }
        // The password is read from the environment (v1 env-based auth), never from the spec.
        if (!parameters->SaslPasswordEnv.empty()) {
            if (auto password = GetEnv(TString(parameters->SaslPasswordEnv)); !password.empty()) {
                config.emplace_back("sasl.password", std::string(password));
            } else {
                YT_TLOG_WARNING("Kafka SASL password environment variable is empty, SASL auth may fail")
                    .With("EnvVar", parameters->SaslPasswordEnv);
            }
        }
        if (!parameters->SslCaLocation.empty()) {
            config.emplace_back("ssl.ca.location", parameters->SslCaLocation);
        }
        for (const auto& [key, value] : parameters->ExtraConfig) {
            config.emplace_back(key, value);
        }

        return config;
    }())
{ }

TFuture<void> TKafkaClient::Load(const THashMap<TResourceId, IResourcePtr>& /*dependencies*/)
{
    return OKFuture;
}

cppkafka::Configuration TKafkaClient::MakeBaseConfiguration() const
{
    cppkafka::Configuration configuration;
    for (const auto& [key, value] : BaseConfig_) {
        configuration.set(key, value);
    }
    return configuration;
}

const std::string& TKafkaClient::GetClusterIdentity() const
{
    return ClusterIdentity_;
}

const std::string& TKafkaClient::GetSaslUsername() const
{
    return SaslUsername_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
