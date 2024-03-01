#include "mount_config_optimizer.h"

#include "config.h"

#include <yt/yt/server/lib/tablet_node/config.h>

namespace NYT::NLsm::NTesting {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TMountConfigOptimizer::TMountConfigOptimizer(
    TMountConfigOptimizerConfigPtr config,
    TTableMountConfigPtr baseMountConfig)
    : Config_(std::move(config))
    , BaseMountConfig_(std::move(baseMountConfig))
{
    std::vector<TOptimizer::TParameter> optimizerParams;


    for (const auto& [name, param] : Config_->Parameters) {
        TOptimizer::TParameter optimizerParam{
            .Min = param->Min,
            .Max = param->Max,
            .Weight = param->Weight,
            .IsInteger = param->Integer,
        };
        optimizerParams.push_back(optimizerParam);

        ParameterNames_.push_back(name);
    }

    Optimizer_ = std::make_unique<TOptimizer>(optimizerParams);
}

TTableMountConfigPtr TMountConfigOptimizer::Run(TFunction function)
{
    auto wrapped = [&] (const std::vector<double>& values) {
        auto mountConfig = GetPatchedConfig(values);
        if (!mountConfig) {
            return std::numeric_limits<double>::max();
        }
        return function(mountConfig);
    };

    auto optimizedValues = Optimizer_->Optimize(
        wrapped,
        Config_->NumIterations,
        Config_->NumDirections,
        Config_->InitialStep,
        Config_->StepMultiple);

    return BaseMountConfig_;
    // return GetPatchedConfig(optimizedValues);
}

TTableMountConfigPtr TMountConfigOptimizer::GetPatchedConfig(
    const std::vector<double>& values) const
{
    auto mountConfig = CloneYsonStruct(BaseMountConfig_);

    // Both doubles and i64s are stored here. i64s are typically < 2**40
    // so they should be stored precisely.
    THashMap<TString, double> effectiveValues;

    for (int index = 0; index < ssize(values); ++index) {
        const auto& name = ParameterNames_[index];
        const auto& param = Config_->Parameters[name];
        auto realValue = values[index];

        auto* nodeFactory = GetEphemeralNodeFactory();
        auto map = nodeFactory->CreateMap();

        if (param->Integer) {
            auto scalarNode = nodeFactory->CreateInt64();
            scalarNode->SetValue(std::llround(realValue));
            mountConfig->LoadParameter(name, scalarNode);
            effectiveValues[name] = std::llround(realValue);
        } else {
            auto scalarNode = nodeFactory->CreateDouble();
            scalarNode->SetValue(realValue);
            mountConfig->LoadParameter(name, scalarNode);
            effectiveValues[name] = realValue;
        }

        Cerr << "Set " << name << " to " <<
            effectiveValues[name] << Endl;

    }

    mountConfig->Postprocess();

    for (const auto& [lhsName, lhsParam] : Config_->Parameters) {
        auto lhs = effectiveValues[lhsName];

        for (const auto& rhsName : lhsParam->LessThan) {
            auto rhs = effectiveValues[rhsName];
            if (!(lhs < rhs)) {
                return nullptr;
            }
        }

        for (const auto& rhsName : lhsParam->LessThan) {
            auto rhs = effectiveValues[rhsName];
            if (!(lhs <= rhs)) {
                return nullptr;
            }
        }
    }

    return mountConfig;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLsm::NTesting
