#pragma once

#ifndef PIPELINE_INL_H_
    #error "Direct inclusion of this file is not allowed, include pipeline.h"
    // For the sake of sane code completion.
    #include "pipeline.h"
#endif

namespace NYT::NFlow::NCompanionServer {

////////////////////////////////////////////////////////////////////////////////

template <class TFunction, class TStaticParameters, class TDynamicParameters>
void TPipeline::AddTransform(TComputationId computationId)
{
    RegisterFunction<TFunction, TStaticParameters, TDynamicParameters>();
    Add(std::move(computationId), ECompanionComputationType::Transform);
}

template <class TFunction, class TStaticParameters, class TDynamicParameters>
void TPipeline::AddSource(TComputationId computationId)
{
    RegisterFunction<TFunction, TStaticParameters, TDynamicParameters>();
    Add(std::move(computationId), ECompanionComputationType::Source);
}

template <class TFunction, class TStaticParameters, class TDynamicParameters>
void TPipeline::RegisterFunction()
{
    // A function type may be declared by several computations and several
    // TPipeline instances; register each instantiation exactly once per
    // process (the registry throws on a genuine duplicate).
    static const bool registered = [] {
        TRegistry::Get()->RegisterProcessFunction<TFunction, TStaticParameters, TDynamicParameters>();
        return true;
    }();
    Y_UNUSED(registered);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanionServer
