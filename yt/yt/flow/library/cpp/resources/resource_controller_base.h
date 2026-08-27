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
    static constexpr bool SupportsFileSourceDiscovery = true;

    TResourceControllerBase(TResourceControllerContextPtr context, TDynamicResourceControllerContextPtr dynamicContext);
    ~TResourceControllerBase() override;

    void Init(IInitContextPtr initContext) final;
    TResourceRevisionPtr BuildTargetRevision() final;
    void CollectStatuses(
        const THashMap<std::string, TWorkerStatusPtr>& workerStatuses,
        const TWorkerResourceStatusPtr& controllerStatus,
        std::optional<i64> publishedRevisionId) final;
    NYTree::IMapNodePtr GetView() final;

    TResourceControllerContextPtr GetContext() const;
    TDynamicResourceControllerContextPtr GetDynamicContext() const;

    TResourceSpecPtr GetSpec() const;
    TDynamicResourceSpecPtr GetDynamicSpec() const;

protected:
    virtual void DoInit(IInitContextPtr initContext);
    virtual NYTree::INodePtr DoBuildTargetRevisionSpec();
    virtual void DoCollectStatuses(
        const THashMap<std::string, TWorkerResourceStatusPtr>& workerStatuses,
        const TWorkerResourceStatusPtr& controllerStatus);
    virtual NYTree::IMapNodePtr DoGetView();

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
    class TFileSourceDiscovery;

    const TResourceControllerContextPtr Context_;
    TAtomicIntrusivePtr<TDynamicResourceControllerContext> DynamicContext_;
    const NYTree::TYsonStructPtr Parameters_;
    TAtomicIntrusivePtr<NYTree::TYsonStruct> DynamicParameters_;
    const TIntrusivePtr<TFileSourceDiscovery> FileSourceDiscovery_;

protected:
    NLogging::TLogger Logger;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
