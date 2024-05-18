#pragma once

#include "public.h"
#include "config.h"

#include <yt/yt/library/dynamic_config/dynamic_config_manager.h>

namespace NYT::NKafkaProxy {

////////////////////////////////////////////////////////////////////////////////

//! Manages dynamic configuration of a Kafka Proxy
//! by pulling it periodically from masters.
/*!
 *  \note
 *  Thread affinity: any
 */
class TDynamicConfigManager
    : public NDynamicConfig::TDynamicConfigManagerBase<TKafkaProxyDynamicConfig>
{
public:
    explicit TDynamicConfigManager(IBootstrap* bootstrap);

private:
    const std::vector<TString> InstanceTags_;
};

DEFINE_REFCOUNTED_TYPE(TDynamicConfigManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NKafkaProxy
