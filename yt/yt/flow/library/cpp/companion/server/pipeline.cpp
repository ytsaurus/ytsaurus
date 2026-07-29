#include "pipeline.h"

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/fluent.h>

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

NCompanion::TCompanionInfoPtr TPipeline::BuildCompanionInfo() const
{
    auto info = New<NCompanion::TCompanionInfo>();
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
    auto node = ConvertTo<IMapNodePtr>(BuildCompanionInfo());
    node->AddChild("pid", ConvertToNode(static_cast<i64>(GetPID())));
    return NYson::ConvertToYsonString(node);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanionServer
