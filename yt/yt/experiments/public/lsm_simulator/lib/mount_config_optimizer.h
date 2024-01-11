#pragma once

#include "optimizer.h"

namespace NYT::NLsm::NTesting {

////////////////////////////////////////////////////////////////////////////////

class TMountConfigOptimizer
{
public:
    using TFunction = std::function<double(TTableMountConfigPtr)>;

    TMountConfigOptimizer(
        TMountConfigOptimizerConfigPtr config,
        TTableMountConfigPtr baseMountConfig);

    TTableMountConfigPtr Run(TFunction function);

private:
    const TMountConfigOptimizerConfigPtr Config_;
    const TTableMountConfigPtr BaseMountConfig_;
    std::unique_ptr<TOptimizer> Optimizer_;
    std::vector<TString> ParameterNames_;

    TTableMountConfigPtr GetPatchedConfig(const std::vector<double>& values) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLsm::NTesting
