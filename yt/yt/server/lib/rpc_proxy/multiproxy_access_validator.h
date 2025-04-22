#pragma once

#include "public.h"

namespace NYT::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

struct IMultiproxyAccessValidator
    : public TRefCounted
{
    virtual void ValidateMultiproxyAccess(const std::string& cluster, const std::string& method) = 0;
    virtual void Reconfigure(const TMultiproxyDynamicConfigPtr& config) = 0;
};

DEFINE_REFCOUNTED_TYPE(IMultiproxyAccessValidator)

////////////////////////////////////////////////////////////////////////////////

using TMultiproxyMethodList = std::vector<std::pair<std::string, EMultiproxyMethodKind>>;

IMultiproxyAccessValidatorPtr CreateMultiproxyAccessValidator(TMultiproxyMethodList methodList);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy
