#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/common/public.h>

#include <yt/yt/client/table_client/unversioned_row.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

struct TServiceLogEndpoint
    : public NYTree::TYsonStructLite
{
    TKey Key;
    //! Below are the examples of the cases where having Exclusive flag is indeed required (as opposed to just using Lower and UpperExclusive at all times).
    //! If we have upperbound as the end of partitionRange, it will be hash (exclusive).
    //! If we have upperbound as nextoffset from some record, it will look (reminder: offsets are (key, shift), where 0 means "before the key", 1+ means "after the key") like this:
    //! (key, 1) (since the offset for the record itself looked like (key, 0)), thus this key needs to be included into the range.
    bool Exclusive{};

    REGISTER_YSON_STRUCT_LITE(TServiceLogEndpoint);

    static void Register(TRegistrar registrar);
};

struct TServiceLogRange
    : public NYTree::TYsonStruct
{
    std::optional<TServiceLogEndpoint> Lower;
    std::optional<TServiceLogEndpoint> Upper;

    REGISTER_YSON_STRUCT(TServiceLogRange);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TServiceLogRange);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
