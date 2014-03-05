#pragma once

#include <core/logging/log.h>

#include <core/profiling/profiler.h>

#include <ytlib/new_table_client/chunk_meta.pb.h>

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

extern const int FormatVersion;

extern NLog::TLogger TableReaderLogger;
extern NLog::TLogger TableWriterLogger;

////////////////////////////////////////////////////////////////////////////////

struct TBlock
{
    std::vector<TSharedRef> Data;
    NProto::TBlockMeta Meta;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
