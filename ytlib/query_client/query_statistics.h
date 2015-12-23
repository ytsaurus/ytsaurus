#pragma once

#include "public.h"

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

struct TQueryStatistics
{
    i64 RowsRead = 0;
    i64 RowsWritten = 0;
    TDuration SyncTime;
    TDuration AsyncTime;
    TDuration ExecuteTime;
    TDuration ReadTime;
    TDuration WriteTime;
    TDuration CodegenTime;
    bool IncompleteInput = false;
    bool IncompleteOutput = false;

    TQueryStatistics& operator += (const TQueryStatistics& other);
};

void ToProto(NProto::TQueryStatistics* serialized, const TQueryStatistics& original);
TQueryStatistics FromProto(const NProto::TQueryStatistics& serialized);

Stroka ToString(const TQueryStatistics& stat);

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
