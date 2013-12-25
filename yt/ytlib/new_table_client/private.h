#pragma once

#include <ytlib/new_table_client/chunk_meta.pb.h>

#include <core/logging/log.h>
#include <core/profiling/profiler.h>

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

extern int FormatVersion;

extern NLog::TLogger TableReaderLogger;
extern NLog::TLogger TableWriterLogger;

struct TBlock
{
    std::vector<TSharedRef> Data;
    NProto::TBlockMeta Meta;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
