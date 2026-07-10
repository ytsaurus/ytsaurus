#pragma once

#include "public.h"

#include <yt/yt/server/lib/nbd/config.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NNbd::NDynamicTable {

////////////////////////////////////////////////////////////////////////////////

struct TDynamicTableBlockDeviceConfig
    : public TBlockDeviceConfigBase
{
    i64 Size;
    i64 BlockSize;
    i64 ReadBatchSize;
    i64 WriteBatchSize;
    NYPath::TYPath TablePath;

    REGISTER_YSON_STRUCT(TDynamicTableBlockDeviceConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicTableBlockDeviceConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd::NDynamicTable
