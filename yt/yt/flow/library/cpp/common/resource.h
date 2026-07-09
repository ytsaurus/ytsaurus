#pragma once

#include "public.h"
#include "spec_validation.h"

#include <yt/yt/core/logging/log.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

struct TResourceContext
    : public TRefCounted
{
    // Resource-specific identity and ownership.
    TResourceId ResourceId;
    TResourceSpecPtr ResourceSpec;
    TWeakPtr<IResourceManager> ResourceManager;

    // Common infrastructure.
    IPipelineAuthenticatorPtr PipelineAuthenticator;
    IInvokerPtr Invoker;

    // Observability.
    NLogging::TLogger Logger;
    NProfiling::TProfiler Profiler;
    IStatusProfilerPtr StatusProfiler;
};

DEFINE_REFCOUNTED_TYPE(TResourceContext);

////////////////////////////////////////////////////////////////////////////////

struct TDynamicResourceContext
    : public TRefCounted
{
    TDynamicResourceSpecPtr DynamicResourceSpec;
};

DEFINE_REFCOUNTED_TYPE(TDynamicResourceContext);

////////////////////////////////////////////////////////////////////////////////

//! Base interface for all Flow resource implementations.
/*!
 *  Resources are components that can be loaded before computation construction and used during job execution.
 *  Resource can have dependencies on other resources and can be parameterized through their spec.
 *
 *  Typical usecase is getting access to shared and/or heavyweight external resources, such as database connections or files.
 */
struct IResource
    : public virtual TRefCounted
{
    // Provide TParameter[Ptr] aliases. It is type of spec `Parameters` field.
    // This type is used in resource registration for future parsing.
    // It may be shadowed by macros YT_FLOW_EXTEND_PARAMETERS in derived types.
    YT_FLOW_REGISTER_PARAMETERS(NYTree::TYsonStruct);

    // Provide TDynamicParameter[Ptr] aliases. It is type of dynamic spec `Parameters` field.
    // This type is used in resource registration for future parsing.
    // It may be shadowed by macros YT_FLOW_EXTEND_DYNAMIC_PARAMETERS in derived types.
    YT_FLOW_REGISTER_DYNAMIC_PARAMETERS(NYTree::TYsonStruct);

    using TValidator = TNoopSpecValidator;

    //! Loads the resource with its dependencies.
    /*!
     *  This method is called to load the resource and prepare it for use.
     *  It can access other resources it depends on through the `dependencies` parameter.
     *
     *  The Load method is called at controller and worker instances if corresponding parameters
     *  are configured at `required_resource_ids` section of computation static spec.
     *
     *  The caller guarantees not to call this method concurrently.
     *
     *  \param dependencies - Map of resource IDs to resource instances that this resource depends on.
     *  \returns A future that completes when the resource is fully loaded.
     */
    virtual TFuture<void> Load(const THashMap<TResourceId, IResourcePtr>& dependencies) = 0;

    //! Reconfigures the resource.
    /*!
     *  This method is called by TResourceManager when resource dynamic spec is updated.
     *
     *  Important! Reconfiguration of a resource is scoped to that resource only.
     *  It does NOT trigger reconfiguration of dependent resources.
     *
     *  \param dynamicSpec - The new dynamic resource spec.
     */
    virtual void Reconfigure(const TDynamicResourceContextPtr& dynamicContext) = 0;

    //! Casts this resource to a derived resource type, throwing an exception if the cast fails.
    template <class TDerivedResource>
    TIntrusivePtr<TDerivedResource> As();
    //! Attempts to cast this resource to a derived resource type, returning nullptr if the cast fails.
    template <class TDerivedResource>
    TIntrusivePtr<TDerivedResource> TryAs();

    //! Casts this resource to a derived resource type (const version), throwing an exception if the cast fails.
    template <class TDerivedResource>
    TIntrusivePtr<const TDerivedResource> As() const;
    //! Attempts to cast this resource to a derived resource type (const version), returning nullptr if the cast fails.
    template <class TDerivedResource>
    TIntrusivePtr<const TDerivedResource> TryAs() const;
};

DEFINE_REFCOUNTED_TYPE(IResource);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow

#define RESOURCE_INL_H_
#include "resource-inl.h"
#undef RESOURCE_INL_H_
