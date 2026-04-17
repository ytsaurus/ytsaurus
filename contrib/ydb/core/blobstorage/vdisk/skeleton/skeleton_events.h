#pragma once
#include "defs.h"
#include <contrib/ydb/core/base/blobstorage.h>

namespace NKikimr {

    //////////////////////////////////////////////////////////////////////////
    // TEvTimeToUpdateStats
    //////////////////////////////////////////////////////////////////////////
    class TEvTimeToUpdateStats : public TEventLocal<
        TEvTimeToUpdateStats,
        TEvBlobStorage::EvTimeToUpdateStats>
    {};

} // NKikimr
