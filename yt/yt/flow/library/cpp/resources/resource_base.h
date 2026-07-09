#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/common/resource.h>
#include <yt/yt/flow/library/cpp/misc/reconfigurable.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

//! Base class for Flow resources.
//! @see IResource for details.
class TResourceBase
    : public IResource
    , public virtual TReconfigurable<TDynamicResourceContext>
{
public:
    //! Constructor.
    /*!
     *  Resource constructor called at each Controller and Worker instances but loaded only when it is required
     *  by `required_resource_ids` section of computation static spec.
     *  So constructor has to be lightweight and non-blocking.
     *
     *  Important! Do not use constructor for resource loading and blocking calls (disk, network, etc.),
     *  use the `Load` method for that purpose instead.
     *
     *  \param context - The resource context.
     *  \param spec - The resource spec defining resource class name, parameters and dependencies.
     */
    TResourceBase(TResourceContextPtr context, TDynamicResourceContextPtr dynamicContext);

    //! Gets the resource context.
    /*!
     *  \returns The resource context.
     */
    TResourceContextPtr GetContext() const;
    TDynamicResourceContextPtr GetDynamicContext() const;

    //! Gets the resource spec.
    /*!
     *  \returns The resource spec.
     */
    TResourceSpecPtr GetSpec() const;

    //! Gets the current dynamic spec.
    /*!
     *  \returns The current dynamic resource spec.
     */
    TDynamicResourceSpecPtr GetDynamicSpec() const;

    //! Loads the resource with its dependencies.
    //! @see IResource::Load for details.
    TFuture<void> Load(const THashMap<TResourceId, IResourcePtr>& dependencies) override;

    //! Reconfigures the resource.
    //! @see IResource::Reconfigure for details.
    void Reconfigure(const TDynamicResourceContextPtr& dynamicContext) final;

protected:
    //! Reports queue activity for this resource to the resource manager.
    /*!
     *  \param morePushedToQueue - Number of additional items pushed to the queue since the last call.
     *  \param moreFetchedFromQueue - Number of additional items fetched from the queue since the last call.
     */
    void FeedStatus(i64 morePushedToQueue, i64 moreFetchedFromQueue);

    //! Gets the base parameters for resource.
    /*!
     *  This method shouldn't be called directly.
     *  Use YT_FLOW_EXTEND_PARAMETERS macro for registering your own parameters and GetParameters() method to access them.
     *
     *  \returns The YSON structure containing the resource parameters.
     */
    NYTree::TYsonStructPtr GetParametersBase() const final;

    //! Gets the base dynamic parameters for resource.
    /*!
     *  This method shouldn't be called directly.
     *  Use YT_FLOW_EXTEND_DYNAMIC_PARAMETERS macro for registering your own dynamic parameters
     *  and GetDynamicParameters() method to access them.
     *
     *  \returns The YSON structure containing the resource dynamic parameters.
     */
    NYTree::TYsonStructPtr GetDynamicParametersBase() const final;

private:
    const TResourceContextPtr Context_;
    TAtomicIntrusivePtr<TDynamicResourceContext> DynamicContext_;
    const NYTree::TYsonStructPtr Parameters_;
    TAtomicIntrusivePtr<NYTree::TYsonStruct> DynamicParameters_;

protected:
    NLogging::TLogger Logger;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
