#pragma once

#include "public.h"

#include <library/cpp/yt/yson/consumer.h>

#include <library/cpp/yt/misc/enum_indexed_array.h>

namespace NYT::NIncumbentClient {

////////////////////////////////////////////////////////////////////////////////

struct TIncumbentDescriptor
{
    std::vector<std::optional<TString>> Addresses;
};

using TIncumbentMap = TEnumIndexedArray<EIncumbentType, TIncumbentDescriptor>;

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
