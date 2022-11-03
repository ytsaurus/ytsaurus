#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NQueueClient {

////////////////////////////////////////////////////////////////////////////////

class TPartitionReaderConfig
    : public NYTree::TYsonStruct
{
public:
    i64 MaxRowCount;
    i64 MaxDataWeight;

    //! If set, this value is used to compute the number of rows to read considering the given MaxDataWeight.
    std::optional<i64> DataWeightPerRowHint;

    bool UseNativeTabletNodeApi;

    REGISTER_YSON_STRUCT(TPartitionReaderConfig)

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TPartitionReaderConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueClient
