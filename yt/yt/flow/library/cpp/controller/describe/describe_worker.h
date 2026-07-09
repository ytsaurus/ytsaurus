#pragma once

#include "common.h"

namespace NYT::NFlow::NDescribe {

////////////////////////////////////////////////////////////////////////////////

struct TExtendedWorkerDescription
    : public TWorkerDescription
{
    std::vector<TMessage> Messages;

    bool Banned{};
    NYTree::IMapNodePtr Coefficients;
    std::string FlamegraphAddress;
    std::string DeployAddress;
    std::string BacktracesAddress;
    std::string JfrDownloadCommand;
    std::string MonitoringTag;

    REGISTER_YSON_STRUCT_LITE(TExtendedWorkerDescription);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

// Takes arbitrary worker with given address and incarnation id. Nullopt means that any worker match.
TExtendedWorkerDescription DescribeWorker(
    const TFlowViewPtr& flowView,
    const std::string& workerAddress);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NDescribe
