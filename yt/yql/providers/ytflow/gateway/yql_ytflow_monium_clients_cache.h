#pragma once

#include "yql_ytflow_monium_client.h"

#include <library/cpp/yt/memory/intrusive_ptr.h>
#include <library/cpp/yt/memory/ref_counted.h>

namespace NYql::NYtflow {

DECLARE_REFCOUNTED_CLASS(IMoniumClientsCache);

class IMoniumClientsCache
    : public NYT::TRefCounted
{
public:
    virtual IMoniumClientPtr GetClient(const TMoniumConnectionConfig& config) = 0;
};

IMoniumClientsCachePtr CreateMoniumClientsCache();

} // namespace NYql::NYtflow
