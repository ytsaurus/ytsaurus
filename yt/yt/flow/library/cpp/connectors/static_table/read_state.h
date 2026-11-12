#pragma once

#include "source_public.h"

#include <yt/yt/flow/library/cpp/common/public.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <set>

namespace NYT::NFlow::NStaticTableConnector {

////////////////////////////////////////////////////////////////////////////////

struct TReadRange
    : public NYTree::TYsonStructLite
{
    i64 BeginRowIndex{};
    i64 EndRowIndex{};
    TSystemTimestamp PlannedTimestamp{};
    TDuration PlannedReadDuration;

    TSystemTimestamp GetPlannedEndTimestamp() const;

    REGISTER_YSON_STRUCT_LITE(TReadRange);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

struct TTableReadState
    : public NYTree::TYsonStruct
{
    i64 NextRowIndex{};
    THashMap<TRangeId, TReadRange> Ranges;
    std::multiset<TSystemTimestamp> FreeSlotTimestamps;

    REGISTER_YSON_STRUCT(TTableReadState);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTableReadState);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NStaticTableConnector
