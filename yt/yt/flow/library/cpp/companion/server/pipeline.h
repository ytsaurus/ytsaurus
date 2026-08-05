#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/common/registry.h>

#include <yt/yt/flow/library/cpp/companion/companion_model.h>

#include <yt/yt/core/yson/string.h>

namespace NYT::NFlow::NCompanionServer {

////////////////////////////////////////////////////////////////////////////////

//! Build-phase registry of the computations hosted by a companion binary.
//! Populate before starting the server; the server only reads it afterwards.
class TPipeline
{
public:
    //! Declares a hosted transform computation and registers |TFunction| (with
    //! its parameter schemas) in the process-function registry, replacing
    //! YT_FLOW_DEFINE_PROCESS_FUNCTION in companion binaries. The spec still
    //! selects the function per job via |processing_function|; extra functions
    //! can be registered with the macro. Throws on duplicate computation id.
    template <
        class TFunction,
        class TStaticParameters = TEmptyProcessFunctionParameters,
        class TDynamicParameters = TEmptyProcessFunctionParameters>
    void AddTransform(TComputationId computationId);

    //! Declares a hosted source computation; see #AddTransform.
    template <
        class TFunction,
        class TStaticParameters = TEmptyProcessFunctionParameters,
        class TDynamicParameters = TEmptyProcessFunctionParameters>
    void AddSource(TComputationId computationId);

    //! Declares a hosted companion resource class and registers it in the
    //! resource registry, replacing YT_FLOW_DEFINE_RESOURCE in companion
    //! binaries. Resource ids are not declared here: the "init" command names
    //! the class per resource id via |companion_resource_class| in the spec's
    //! parameters, and the resource store creates the instances.
    template <class TResource>
    void AddResource();

    const THashMap<TComputationId, ECompanionComputationType>& GetComputations() const;
    bool HasComputation(const TComputationId& computationId) const;

    //! Class names declared via #AddResource; only they may be instantiated
    //! by the "init" resource command.
    const THashSet<std::string>& GetResourceClassNames() const;

    NCompanion::TCompanionInfoPtr BuildCompanionInfo() const;
    //! Static CompanionInfo RPC payload for this process.
    NYson::TYsonString BuildCompanionInfoPayload() const;

private:
    template <class TFunction, class TStaticParameters, class TDynamicParameters>
    void RegisterFunction();

    void Add(TComputationId computationId, ECompanionComputationType type);

    THashMap<TComputationId, ECompanionComputationType> Computations_;
    THashSet<std::string> ResourceClassNames_;
};

} // namespace NYT::NFlow::NCompanionServer

#define PIPELINE_INL_H_
#include "pipeline-inl.h"
#undef PIPELINE_INL_H_
