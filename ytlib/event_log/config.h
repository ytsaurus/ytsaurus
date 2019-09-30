#pragma once

#include "public.h"

#include <yt/ytlib/table_client/config.h>

namespace NYT::NEventLog {

////////////////////////////////////////////////////////////////////////////////

class TEvenTLogManagerConfig
    : public NTableClient::TBufferedTableWriterConfig
{
public:
    NYPath::TYPath Path;
    TDuration PendingRowsFlushPeriod;

    TEvenTLogManagerConfig();
};

DEFINE_REFCOUNTED_TYPE(TEvenTLogManagerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NEventLog
