#pragma once

#include <yt/yql/plugin/udf_meta.h>

#include <yt/yt/library/dynamic_config/dynamic_config_manager.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/client/ypath/public.h>

namespace NYT::NYqlAgent {

////////////////////////////////////////////////////////////////////////////////

//! Watches YQL UDF meta document node in Cypress and signals on changes.
/*!
 *  \note
 *  Thread affinity: any
 */
class TUdfMetaManager
    : public NDynamicConfig::TDynamicConfigManagerBase<NYqlPlugin::TUdfMeta>
{
public:
    TUdfMetaManager(
        NYPath::TYPath udfMetaPath,
        NApi::IClientPtr client,
        IInvokerPtr invoker);
};

DEFINE_REFCOUNTED_TYPE(TUdfMetaManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYqlAgent
