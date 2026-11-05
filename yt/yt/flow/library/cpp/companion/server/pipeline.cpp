#include "pipeline.h"

#include <yt/yt/core/ytree/convert.h>

#include <util/system/getpid.h>

namespace NYT::NFlow::NCompanionServer {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void TPipeline::Add(TComputationId computationId, ECompanionComputationType type)
{
    auto [it, inserted] = Computations_.emplace(std::move(computationId), type);
    THROW_ERROR_EXCEPTION_UNLESS(inserted,
        "Computation %Qv is already registered in the companion pipeline",
        it->first);
}

const THashMap<TComputationId, ECompanionComputationType>& TPipeline::GetComputations() const
{
    return Computations_;
}

bool TPipeline::HasComputation(const TComputationId& computationId) const
{
    return Computations_.contains(computationId);
}

const THashSet<std::string>& TPipeline::GetResourceClassNames() const
{
    return ResourceClassNames_;
}

NCompanion::TCompanionInfoPtr TPipeline::BuildCompanionInfo() const
{
    auto info = New<NCompanion::TCompanionInfo>();
    info->ProcessId = GetPID();
    for (const auto& [computationId, type] : Computations_) {
        auto computationInfo = New<NCompanion::TCompanionComputationInfo>();
        computationInfo->ComputationId = computationId;
        computationInfo->CompanionComputationType = type;
        EmplaceOrCrash(info->Computations, computationId, std::move(computationInfo));
    }
    return info;
}

NYson::TYsonString TPipeline::BuildCompanionInfoPayload() const
{
    return NYson::ConvertToYsonString(BuildCompanionInfo());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanionServer
