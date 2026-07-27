#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/common/resource_controller.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

//! Base class for Flow resource controllers.
//! @see IResourceController for details.
class TResourceControllerBase
    : public IResourceController
{
public:
    TResourceControllerBase(TResourceControllerContextPtr context, TDynamicResourceControllerContextPtr dynamicContext);

    //! Does nothing; controllers that persist state override this.
    void Init(IInitContextPtr initContext) override;

    TResourceControllerContextPtr GetContext() const;
    TDynamicResourceControllerContextPtr GetDynamicContext() const;

    TResourceSpecPtr GetSpec() const;
    TDynamicResourceSpecPtr GetDynamicSpec() const;

protected:
    //! Gets the base parameters for the resource controller.
    /*!
     *  This method shouldn't be called directly.
     *  Use YT_FLOW_EXTEND_PARAMETERS macro for registering your own parameters and GetParameters() method to access them.
     */
    NYTree::TYsonStructPtr GetParametersBase() const final;

    //! Gets the base dynamic parameters for the resource controller.
    /*!
     *  This method shouldn't be called directly.
     *  Use YT_FLOW_EXTEND_DYNAMIC_PARAMETERS macro for registering your own dynamic parameters
     *  and GetDynamicParameters() method to access them.
     */
    NYTree::TYsonStructPtr GetDynamicParametersBase() const final;

private:
    const TResourceControllerContextPtr Context_;
    TAtomicIntrusivePtr<TDynamicResourceControllerContext> DynamicContext_;
    const NYTree::TYsonStructPtr Parameters_;
    TAtomicIntrusivePtr<NYTree::TYsonStruct> DynamicParameters_;

protected:
    NLogging::TLogger Logger;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
