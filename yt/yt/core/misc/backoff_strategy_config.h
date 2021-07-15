#pragma once

#include "backoff_strategy.h"

#include <yt/yt/core/ytree/yson_serializable.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TSerializableExponentialBackoffOptions
    : public virtual NYTree::TYsonSerializable
    , public TExponentialBackoffOptions
{
public:
    TSerializableExponentialBackoffOptions();
};

DEFINE_REFCOUNTED_TYPE(TSerializableExponentialBackoffOptions)

////////////////////////////////////////////////////////////////////////////////

class TSerializableConstantBackoffOptions
    : public virtual NYTree::TYsonSerializable
    , public TConstantBackoffOptions
{
public:
    TSerializableConstantBackoffOptions();
};

DEFINE_REFCOUNTED_TYPE(TSerializableConstantBackoffOptions)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
