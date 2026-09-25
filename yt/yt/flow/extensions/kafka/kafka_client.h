#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/common/registry.h>

#include <yt/yt/flow/library/cpp/resources/resource_base.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <contrib/libs/cppkafka/include/cppkafka/configuration.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

//! Connection and auth settings for the shared Kafka client resource.
//! Transport credentials live here, resolved once; per-stream addressing (topic, group id)
//! lives on the source/sink parameters instead.
struct TKafkaClientConfig
    : public NYTree::TYsonStruct
{
    //! Comma-separated broker list, passed as librdkafka `bootstrap.servers`.
    std::string BootstrapServers;

    //! librdkafka `security.protocol`: PLAINTEXT, SSL, SASL_PLAINTEXT or SASL_SSL.
    std::string SecurityProtocol;

    //! librdkafka `sasl.mechanism`: e.g. PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, OAUTHBEARER.
    std::string SaslMechanism;

    //! librdkafka `sasl.username`. The matching password is read from the environment
    //! variable named by #SaslPasswordEnv for the v1 env-based auth.
    std::string SaslUsername;
    std::string SaslPasswordEnv;

    //! librdkafka `ssl.ca.location` (path to the CA bundle), if TLS is used.
    std::string SslCaLocation;

    //! Extra librdkafka config keys applied verbatim to every consumer and producer.
    THashMap<std::string, std::string> ExtraConfig;

    REGISTER_YSON_STRUCT(TKafkaClientConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TKafkaClientConfig);

////////////////////////////////////////////////////////////////////////////////

//! Shared Kafka client resource. Holds the resolved connection/auth config and hands out
//! cppkafka::Configuration objects to the source read sessions and sink writers.
//! The constructor stays non-blocking; no network I/O happens until a consumer/producer is built.
class TKafkaClient
    : public TResourceBase
{
public:
    YT_FLOW_EXTEND_PARAMETERS(TKafkaClientConfig);

    TKafkaClient(TResourceContextPtr context, TDynamicResourceContextPtr dynamicContext);

    TFuture<void> Load(const THashMap<TResourceId, IResourcePtr>& dependencies) override;

    //! Builds a fresh Configuration carrying the shared connection/auth keys. Callers overlay
    //! their role-specific keys (group.id, enable.idempotence, ...) before constructing a handle.
    cppkafka::Configuration MakeBaseConfiguration() const;

    //! Broker list, used by source/sink controllers to build a stable stream identity.
    const std::string& GetBootstrapServers() const;

    //! SASL principal the handles authenticate as, for error messages; empty without SASL.
    const std::string& GetSaslUsername() const;

private:
    const std::string BootstrapServers_;
    const std::string SaslUsername_;
    //! Fully resolved base key/value pairs (secrets already substituted from the environment).
    std::vector<std::pair<std::string, std::string>> BaseConfig_;
};

DEFINE_REFCOUNTED_TYPE(TKafkaClient);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
