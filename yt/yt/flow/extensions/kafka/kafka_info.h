#pragma once

#include "public.h"

#include "kafka_client.h"

#include <yt/yt/flow/library/cpp/common/init_context.h>
#include <yt/yt/flow/library/cpp/common/state.h>
#include <yt/yt/flow/library/cpp/common/state_client.h>

#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <contrib/libs/cppkafka/include/cppkafka/producer.h>

#include <memory>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

struct TKafkaInfoControllerState
    : public NYTree::TYsonStruct
{
    //! Last known topic partition count; monotonically non-decreasing (Kafka only ever adds partitions).
    std::optional<i64> CachedPartitionCount;

    REGISTER_YSON_STRUCT(TKafkaInfoControllerState);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TKafkaInfoControllerState);

////////////////////////////////////////////////////////////////////////////////

//! Discovers the topic partition count from broker metadata on a periodic executor and caches it in
//! persisted state. Shared by the source and sink controllers to enumerate partitions.
class TKafkaInfoController
    : public TRefCounted
{
public:
    TKafkaInfoController(
        TKafkaInfoSpecPtr spec,
        TKafkaClientPtr client,
        IInvokerPtr invoker,
        const NLogging::TLogger& logger,
        IStatusProfilerPtr statusProfiler);

    void Init(IInitContextPtr initContext);
    void Sync();
    void Commit();

    //! Returns the cached partition count, or nullopt until the first successful metadata fetch.
    std::optional<i64> GetPartitionCount() const;

private:
    void TryUpdatePartitionCount();

    const NLogging::TLogger Logger;
    const TKafkaInfoSpecPtr Spec_;
    const TKafkaClientPtr Client_;
    const IInvokerPtr Invoker_;
    const IStatusErrorStatePtr UpdatePartitionCountErrorState_;

    std::unique_ptr<cppkafka::Producer> Producer_;
    //! Dedicated thread for the blocking librdkafka metadata calls, keeping them off the shared
    //! controller invoker.
    NConcurrency::TActionQueuePtr MetadataQueue_;
    TMutableStateClient<TKafkaInfoControllerState> State_;
    NConcurrency::TPeriodicExecutorPtr Executor_;
};

DEFINE_REFCOUNTED_TYPE(TKafkaInfoController);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
