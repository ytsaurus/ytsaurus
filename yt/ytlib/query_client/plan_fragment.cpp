#include "stdafx.h"
#include "plan_fragment.h"

#include "prepare_controller.h"

#include "plan_node.h"

#include <ytlib/query_client/plan_fragment.pb.h>

#include <core/misc/protobuf_helpers.h>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

TPlanFragment::TPlanFragment(
    TPlanContextPtr context,
    const TOperator* head,
    const TGuid& id)
    : Context_(std::move(context))
    , Head_(head)
    , Id_(id)
{ }

TPlanFragment TPlanFragment::Prepare(
    IPrepareCallbacks* callbacks,
    const Stroka& source,
    i64 inputRowLimit,
    i64 outputRowLimit,
    TTimestamp timestamp)
{
    return TPrepareController(
        callbacks,
        source,
        inputRowLimit,
        outputRowLimit,
        timestamp).Run();
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TPlanFragment* serialized, const TPlanFragment& fragment)
{
    ToProto(serialized->mutable_head(), fragment.GetHead());
    ToProto(serialized->mutable_id(), fragment.Id());
    serialized->set_timestamp(fragment.GetContext()->GetTimestamp());
    serialized->set_input_row_limit(fragment.GetContext()->GetInputRowLimit());
    serialized->set_output_row_limit(fragment.GetContext()->GetOutputRowLimit());
}

TPlanFragment FromProto(const NProto::TPlanFragment& serialized)
{
    auto context = New<TPlanContext>(
        serialized.timestamp(),
        serialized.input_row_limit(),
        serialized.output_row_limit());
    return TPlanFragment(
        context,
        FromProto(serialized.head(), context.Get()),
        NYT::FromProto<TGuid>(serialized.id()));
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

