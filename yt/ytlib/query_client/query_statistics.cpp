#include "query_statistics.h"

#include <yt/ytlib/query_client/query_statistics.pb.h>

#include <yt/core/misc/protobuf_helpers.h>

namespace NYT {
namespace NQueryClient {

using NYT::ToProto;
using NYT::FromProto;

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
    serialized->set_sync_time(ToProto(queryResult.SyncTime));
    serialized->set_async_time(ToProto(queryResult.AsyncTime));
    serialized->set_execute_time(ToProto(queryResult.ExecuteTime));
    serialized->set_read_time(ToProto(queryResult.ReadTime));
    serialized->set_write_time(ToProto(queryResult.WriteTime));
    serialized->set_codegen_time(ToProto(queryResult.CodegenTime));
    serialized->set_incomplete_input(queryResult.IncompleteInput);
    serialized->set_incomplete_output(queryResult.IncompleteOutput);
}

TQueryStatistics FromProto(const NProto::TQueryStatistics& serialized)
{
    TQueryStatistics result;
    result.RowsRead = serialized.rows_read();
    result.RowsWritten = serialized.rows_written();
    result.SyncTime = FromProto<TDuration>(serialized.sync_time());
    result.AsyncTime = FromProto<TDuration>(serialized.async_time());
    result.ExecuteTime = FromProto<TDuration>(serialized.execute_time());
    result.ReadTime = FromProto<TDuration>(serialized.read_time());
    result.WriteTime = FromProto<TDuration>(serialized.write_time());
    result.CodegenTime = FromProto<TDuration>(serialized.codegen_time());
    result.IncompleteInput = serialized.incomplete_input();
    result.IncompleteOutput = serialized.incomplete_output();
    return result;
}

TString ToString(const TQueryStatistics& stats)
{
    return Format(
        "{"
        "RowsRead: %v, RowsWritten: %v, "
        "SyncTime: %v, AsyncTime: %v, ExecuteTime: %v, ReadTime: %v, WriteTime: %v, "
        "IncompleteInput: %v, IncompleteOutput: %v"
        "}",
        stats.RowsRead,
        stats.RowsWritten,
        stats.SyncTime,
        stats.AsyncTime,
        stats.ExecuteTime,
        stats.ReadTime,
        stats.WriteTime,
        stats.IncompleteInput,
        stats.IncompleteOutput);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
