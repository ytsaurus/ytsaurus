#pragma once

#include <yt/yt/flow/library/cpp/common/public.h>

#include <library/cpp/yt/memory/ref_counted.h>
#include <library/cpp/yt/misc/enum.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

inline const TResourceId KafkaClientDefaultResourceId = "KafkaClient";

////////////////////////////////////////////////////////////////////////////////

//! Policy for handling a Kafka message that fails to parse.
DEFINE_ENUM(EMalformedKafkaMessagePolicy,
    ((Keep)   (0))
    ((Drop)   (1))
    ((Fail)   (2))
);

DECLARE_REFCOUNTED_STRUCT(TUnparsedKafkaPayload);

DECLARE_REFCOUNTED_STRUCT(TKafkaClientConfig);
DECLARE_REFCOUNTED_CLASS(TKafkaClient);

DECLARE_REFCOUNTED_STRUCT(TKafkaTopicSpec);
DECLARE_REFCOUNTED_STRUCT(TKafkaInfoSpec);
DECLARE_REFCOUNTED_STRUCT(TKafkaInfoControllerState);
DECLARE_REFCOUNTED_CLASS(TKafkaInfoController);

DECLARE_REFCOUNTED_STRUCT(TKafkaSourceParameters);
DECLARE_REFCOUNTED_STRUCT(TDynamicKafkaSourceParameters);
DECLARE_REFCOUNTED_CLASS(TKafkaReadSession);
DECLARE_REFCOUNTED_CLASS(TKafkaSource);
DECLARE_REFCOUNTED_CLASS(TKafkaSourceController);

DECLARE_REFCOUNTED_STRUCT(TCommonKafkaSinkParameters);
DECLARE_REFCOUNTED_STRUCT(TKafkaSinkParameters);
DECLARE_REFCOUNTED_STRUCT(TDynamicKafkaSinkParameters);
DECLARE_REFCOUNTED_STRUCT(TAtLeastOnceKafkaSinkParameters);
DECLARE_REFCOUNTED_STRUCT(TDynamicAtLeastOnceKafkaSinkParameters);
DECLARE_REFCOUNTED_STRUCT(TKafkaSinkControllerParameters);
DECLARE_REFCOUNTED_STRUCT(TDynamicKafkaSinkControllerParameters);

DECLARE_REFCOUNTED_CLASS(TRetryableKafkaWriter);
DECLARE_REFCOUNTED_CLASS(TKafkaSink);
DECLARE_REFCOUNTED_CLASS(TAtLeastOnceKafkaSink);
DECLARE_REFCOUNTED_CLASS(TKafkaSinkController);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
