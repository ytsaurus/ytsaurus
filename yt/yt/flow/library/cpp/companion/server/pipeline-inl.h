#pragma once

#ifndef PIPELINE_INL_H_
    #error "Direct inclusion of this file is not allowed, include pipeline.h"
    // For the sake of sane code completion.
    #include "pipeline.h"
#endif

#include <util/system/type_name.h>

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

template <class TResource>
void TPipeline::AddResource()
{
    // A resource class may be declared by several TPipeline instances;
    // register each instantiation exactly once per process (a genuine
    // duplicate, e.g. a clash with a linked YT_FLOW_DEFINE_RESOURCE, is
    // reported by the registry).
    static const bool registered = [] {
        TRegistry::Get()->RegisterResource<TResource>();
        return true;
    }();
    Y_UNUSED(registered);
    ResourceClassNames_.insert(TypeName<TResource>());
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
