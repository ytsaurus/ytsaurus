#pragma once

#include "public.h"

#include <yt/yt/core/logging/log.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

inline const NLogging::TLogger KafkaLogger("Kafka");

//! Environment variable holding the SASL password for the v1 env-based auth.
constexpr std::string_view KafkaSaslPasswordEnv = "KAFKA_SASL_PASSWORD";

//! Pause between attempts to create a librdkafka consumer or producer.
constexpr auto KafkaHandleCreateBackoff = TDuration::Seconds(5);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
