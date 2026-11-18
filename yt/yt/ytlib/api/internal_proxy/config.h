#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NApi::NInternalProxy {

////////////////////////////////////////////////////////////////////////////////

struct TQueueExportRunnerConfig
    : public NYTree::TYsonStruct
{
    //! Hard limit on the number of static tables a single export call may create.
    //! It caps #NQueueClient::TExportQueueIntoStaticTableOptions::TableCountLimit and is also used when the latter is not set.
    int MaxTableCountPerCall;

    //! Maximum number of hunk chunks in a batch to fetch their attributes from master.
    int MaxHunkChunkCountPerFetch;

    REGISTER_YSON_STRUCT(TQueueExportRunnerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TQueueExportRunnerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NInternalProxy
