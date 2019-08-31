#pragma once

#include "public.h"

#include <yt/core/misc/ref_counted.h>

namespace NYP::NServer::NCluster {

////////////////////////////////////////////////////////////////////////////////

struct IObjectFilterEvaluator
    : virtual public NYT::TRefCounted
{
    virtual TErrorOr<std::vector<TObject*>> Evaluate(
        EObjectType objectType,
        const std::vector<TObject*>& objects,
        const NObjects::TObjectFilter& filter) = 0;
};

DEFINE_REFCOUNTED_TYPE(IObjectFilterEvaluator);

///////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NCluster
