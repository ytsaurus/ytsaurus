#include "stdafx.h"
#include "query_statistics.h"

#include <ytlib/query_client/query_statistics.pb.h>
#include <Foundation/Foundation.h>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

TQueryStatistics& TQueryStatistics::operator+=(const TQueryStatistics& other)
{
    RowsRead += other.RowsRead;
    RowsWritten += other.RowsWritten;
    SyncTime += other.SyncTime;
    AsyncTime += other.AsyncTime;
    IncompleteInput |= other.IncompleteInput;
    IncompleteOutput |= other.IncompleteOutput;
    return *this;
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TQueryStatistics* serialized, const TQueryStatistics& queryResult)
{
    serialized->set_rows_read(queryResult.RowsRead);
    serialized->set_rows_written(queryResult.RowsWritten);
    serialized->set_sync_time(queryResult.SyncTime.GetValue());
    serialized->set_async_time(queryResult.AsyncTime.GetValue());
    serialized->set_incomplete_input(queryResult.IncompleteInput);
    serialized->set_incomplete_output(queryResult.IncompleteOutput);
}

TQueryStatistics FromProto(const NProto::TQueryStatistics& serialized)
{
    TQueryStatistics result;

    result.RowsRead = serialized.rows_read();
    result.RowsWritten = serialized.rows_written();
    result.SyncTime = TDuration(serialized.sync_time());
    result.AsyncTime = TDuration(serialized.async_time());
    result.IncompleteInput = serialized.incomplete_input();
    result.IncompleteOutput = serialized.incomplete_output();

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
