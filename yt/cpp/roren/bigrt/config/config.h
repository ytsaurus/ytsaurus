#pragma once

#include <bigrt/lib/consuming_system/config/config.pb.h>
#include <bigrt/lib/processing/shard_processor/fallback/config/fallback_shard_processor_config.pb.h>
#include <bigrt/lib/processing/shard_processor/stateless/config/stateless_shard_processor_config.pb.h>
#include <bigrt/lib/supplier/config/supplier_config.pb.h>
#include <bigrt/lib/writer/swift/config/config.pb.h>
#include <bigrt/lib/writer/yt_queue/proto/config.pb.h>
#include <yt/cpp/roren/library/config_extension_mixin/user_config_mixin.h>
#include <yt/yt/core/ytree/yson_struct.h>

namespace NRoren {

DECLARE_REFCOUNTED_STRUCT(TComputationConfig);

struct TComputationConfig
    : public TUserConfigMixin<TComputationConfig>
{
    NBigRT::TStatelessShardProcessorConfig StatelessShardProcessorConfig;
    NBigRT::TConsumingSystemConfig ConsumingSystemConfig;
    std::optional<NBigRT::TFallbackShardProcessorConfig> FallbackShardProcessorConfig;
    std::optional<NBigRT::TThrottlerQuota> ThrottlerQuota;

    std::optional<NBigRT::TSupplierConfig> Input;
    THashMap<TString, NBigRT::TSupplierConfig> Inputs;

    TString TransactionKeeperGroup;

    REGISTER_YSON_STRUCT(TComputationConfig);
    static void Register(TRegistrar registrar);
};

DECLARE_REFCOUNTED_STRUCT(TDestinationConfig);

struct TDestinationConfig
    : public TUserConfigMixin<TDestinationConfig>
{
public:
    std::optional<NBigRT::TSwiftQueueWriterConfig> SwiftWriter;
    std::optional<NBigRT::TYtQueueWriterConfig> YtQueueWriter;
    uint64_t ShardCount;

    REGISTER_YSON_STRUCT(TDestinationConfig);
    static void Register(TRegistrar registrar);
};

DECLARE_REFCOUNTED_STRUCT(TBigRtConfig);

struct TBigRtConfig
    : public virtual NYT::NYTree::TYsonStruct
{
    TString MainYtCluster;
    TString MainYtPath;
    TComputationConfigPtr DefaultComputationSettings;
    THashMap<TString, TComputationConfigPtr> Computations;
    THashMap<TString, TDestinationConfigPtr> Destinations;

    // Maximum size of data that can be processed in the moment.
    uint64_t MaxInFlightBytes;

    bool EnableWaitProcessStarted;

    bool AutoLaunchBalancerInternalMaster;

    bool UseProcessorV3;
    std::optional<ui64> LegacyThreadCount;

    TDuration SharedTransactionPeriod;

    REGISTER_YSON_STRUCT(TBigRtConfig);
    static void Register(TRegistrar registrar);
};

} // namespace NRoren
