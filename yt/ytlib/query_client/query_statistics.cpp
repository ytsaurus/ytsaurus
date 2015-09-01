#include "stdafx.h"
#include "query_statistics.h"

#include <ytlib/query_client/query_statistics.pb.h>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

TQueryStatistics& TQueryStatistics::operator+=(const TQueryStatistics& other)
{
    RowsRead += other.RowsRead;
    RowsWritten += other.RowsWritten;
    SyncTime += other.SyncTime;
    AsyncTime += other.AsyncTime;
    ExecuteTime += other.ExecuteTime;
    ReadTime += other.ReadTime;
    WriteTime += other.WriteTime;
    CodegenTime += other.CodegenTime;
    IncompleteInput |= other.IncompleteInput;
    IncompleteOutput |= other.IncompleteOutput;
    return *this;
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TQueryStatistics* serialized, const TQueryStatistics& queryResult)
{
    serialized->set_rows_read(queryResult.RowsRead);
    serialized->set_rows_written(queryResult.RowsWritten);
    serialized->set_sync_time(queryResult.SyncTime.MicroSeconds());
    serialized->set_async_time(queryResult.AsyncTime.MicroSeconds());
    serialized->set_execute_time(queryResult.ExecuteTime.MicroSeconds());
    serialized->set_read_time(queryResult.ReadTime.MicroSeconds());
    serialized->set_write_time(queryResult.WriteTime.MicroSeconds());
    serialized->set_codegen_time(queryResult.CodegenTime.MicroSeconds());
    serialized->set_incomplete_input(queryResult.IncompleteInput);
    serialized->set_incomplete_output(queryResult.IncompleteOutput);
}

TQueryStatistics FromProto(const NProto::TQueryStatistics& serialized)
{
    TQueryStatistics result;
    result.RowsRead = serialized.rows_read();
    result.RowsWritten = serialized.rows_written();
    result.SyncTime = TDuration::MicroSeconds(serialized.sync_time());
    result.AsyncTime = TDuration::MicroSeconds(serialized.async_time());
    result.ExecuteTime = TDuration::MicroSeconds(serialized.execute_time());
    result.ReadTime = TDuration::MicroSeconds(serialized.read_time());
    result.WriteTime = TDuration::MicroSeconds(serialized.write_time());
    result.CodegenTime = TDuration::MicroSeconds(serialized.codegen_time());
    result.IncompleteInput = serialized.incomplete_input();
    result.IncompleteOutput = serialized.incomplete_output();
    return result;
}

Stroka ToString(const TQueryStatistics& stats)
{
    return Format(
        "RowsRead: %v, RowsWritten: %v, "
        "SyncTime: %v, AsyncTime: %v, ExecuteTime: %v, ReadTime: %v, WriteTime: %v, "
        "IncompleteInput: %v, IncompleteOutput: %v", 
        stats.RowsRead,
        stats.RowsWritten,
        stats.SyncTime,
        stats.AsyncTime,
        stats.ExecuteTime,
        stats.ReadTime,
        stats.WriteTime,
        stats.IncompleteInput,
        stats.IncompleteInput);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
