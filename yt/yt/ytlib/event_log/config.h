#pragma once

#include "public.h"

#include <yt/yt/library/event_log/config.h>

#include <yt/yt/ytlib/table_client/config.h>

namespace NYT::NEventLog {

////////////////////////////////////////////////////////////////////////////////

struct TStaticTableEventLogManagerConfig
    : public TEventLogManagerConfig
    , public NTableClient::TBufferedTableWriterConfig
{
    NYPath::TYPath Path;

    REGISTER_YSON_STRUCT(TStaticTableEventLogManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TStaticTableEventLogManagerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NEventLog
