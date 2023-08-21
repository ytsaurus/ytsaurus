#pragma once

#include "public.h"

#include <library/cpp/yt/yson/consumer.h>

namespace NYT::NIncumbentClient {

////////////////////////////////////////////////////////////////////////////////

struct TIncumbentDescriptor
{
    std::vector<std::optional<TString>> Addresses;
};

using TIncumbentMap = TEnumIndexedVector<EIncumbentType, TIncumbentDescriptor>;

void Serialize(const TIncumbentDescriptor& descriptor, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

//! Returns incumbent map with all incumbents unassigned.
TIncumbentMap CreateEmptyIncumbentMap();

////////////////////////////////////////////////////////////////////////////////

struct TIncumbencyDescriptor
{
    EIncumbentType Type;
    int ShardIndex;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIncumbentClient
