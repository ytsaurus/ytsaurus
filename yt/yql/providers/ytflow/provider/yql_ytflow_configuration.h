#pragma once

#include <library/cpp/string_utils/parse_size/parse_size.h>

#include <yql/essentials/providers/common/config/yql_dispatch.h>

#include <util/generic/ptr.h>
#include <util/generic/string.h>

#include <memory>


namespace NYql {

struct TYtflowSettings
{
public:
    using TConstPtr = std::shared_ptr<const TYtflowSettings>;

private:
    static constexpr NCommon::EConfSettingType Static = NCommon::EConfSettingType::Static;
public:
    NCommon::TConfSetting<TString, Static> Auth;

    NCommon::TConfSetting<TString, Static> Cluster;
    NCommon::TConfSetting<TString, Static> PathPrefix;
    NCommon::TConfSetting<TString, Static> PipelinePath;

    NCommon::TConfSetting<TString, Static> TabletCellBundle;
    NCommon::TConfSetting<TString, Static> Account;
    NCommon::TConfSetting<TString, Static> PrimaryMedium;

    NCommon::TConfSetting<bool, Static> GracefulUpdate;
    NCommon::TConfSetting<bool, Static> FiniteStreams;

    NCommon::TConfSetting<uint64_t, Static> ControllerCount;
    NCommon::TConfSetting<NSize::TSize, Static> ControllerMemoryLimit;
    NCommon::TConfSetting<uint64_t, Static> ControllerRpcPort;
    NCommon::TConfSetting<uint64_t, Static> ControllerMonitoringPort;

    NCommon::TConfSetting<uint64_t, Static> WorkerCount;
    NCommon::TConfSetting<NSize::TSize, Static> WorkerMemoryLimit;
    NCommon::TConfSetting<uint64_t, Static> WorkerRpcPort;
    NCommon::TConfSetting<uint64_t, Static> WorkerMonitoringPort;

    NCommon::TConfSetting<TString, Static> YtConsumerPath;
    NCommon::TConfSetting<TString, Static> YtProducerPath;
    NCommon::TConfSetting<uint64_t, Static> YtPartitionCount;
};

struct TYtflowConfiguration
    : public TYtflowSettings
    , public NCommon::TSettingDispatcher
{
    using TPtr = TIntrusivePtr<TYtflowConfiguration>;

    TYtflowConfiguration();

    template <class TProtoConfig>
    void Init(const TProtoConfig& config);
};

} // namespace NYql

#include "yql_ytflow_configuration-inl.h"
