#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/file_storage/config.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/core/bus/tcp/public.h>

namespace NYT::NFlow::NWorker {

////////////////////////////////////////////////////////////////////////////////

// TODO(mikari): move some fields to dynamic pipeline spec.
struct TWorkerConfig
    : public virtual NYTree::TYsonStruct
{
    NBus::NTcp::TBusConfigPtr Bus;
    int MessageServiceThreads{};
    NFileStorage::TFileStorageConfigPtr FileStorage;

    REGISTER_YSON_STRUCT(TWorkerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TWorkerConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NWorker
