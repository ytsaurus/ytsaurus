#pragma once

#include <yql/tools/yqlworker/config/config.h>
#include <yql/essentials/providers/common/metrics/metrics_registry.h>
#include <yql/tools/yqlworker/proto/function_registry.pb.h>
#include <util/generic/ptr.h>
#include <util/generic/noncopyable.h>
#include <util/generic/vector.h>

#include "task.h"

namespace NYql {

//////////////////////////////////////////////////////////////////////////////
// TYqlRunnableTaskFactory
//////////////////////////////////////////////////////////////////////////////
class TYqlRunnableTaskFactory: private TNonCopyable {
public:
    TYqlRunnableTaskFactory(
            const TGatewaysConfig& gatewaysConfig,
            const TStaticGatewaysConfig& staticGatewaysConfig,
            const TFileStorageConfig& fsConfig,
            const NProto::TTaskExecutorConfig& executorConfig,
            const TMaybe<NProto::TDqConfig>& dqConfig,
            IMetricsRegistryPtr metrics,
            std::function<void()> interruptionPointFunc = {});

    ~TYqlRunnableTaskFactory();

    void SetFunctionRegistryData(const NProto::TFunctionRegistryData& data);
    void SetUserAffiliation(NProto::EStaffAffiliation affiliation);
    void SetGatewaysConfig(const TGatewaysConfig& gatewaysConfig);
    void InitGateways(TTaskDataPtr taskData = nullptr, const NProto::TFunctionRegistryData* data = nullptr);
    void SetMetricsDecorator(TMetricsDecorator decorator);

    IYqlRunnableTaskPtr Create(
            NProto::ETaskAction action, TTaskDataPtr data,
            ITaskWatcherPtr watcher);

private:
    struct TImpl;
    THolder<TImpl> Impl_;
};

} // namespace NYql
