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

TPlanFragment::TPlanFragment(const TPlanFragment& other)
    : Context_(other.Context_)
    , Head_(other.Head_)
    , Id_(other.Id_)
{ }

TPlanFragment::TPlanFragment(TPlanFragment&& other)
    : Context_(std::move(other.Context_))
    , Head_(std::move(other.Head_))
    , Id_(std::move(other.Id_))
{ }

TPlanFragment::~TPlanFragment()
{ }

TPlanFragment& TPlanFragment::operator=(const TPlanFragment& other)
{
    Context_ = other.Context_;
    Head_ = other.Head_;
    Id_ = other.Id_;
    return *this;
}

TPlanFragment& TPlanFragment::operator=(TPlanFragment&& other)
{
    Context_ = std::move(other.Context_);
    Head_ = std::move(other.Head_);
    Id_ = std::move(other.Id_);
    return *this;
}

TPlanFragment TPlanFragment::Prepare(
    const Stroka& source,
    TTimestamp timestamp,
    IPrepareCallbacks* callbacks)
{
    return TPrepareController(callbacks, source, std::numeric_limits<ui64>::max(), timestamp).Run();
}

TPlanFragment TPlanFragment::Prepare(
    const Stroka& source,
    TTimestamp timestamp,
    ui64 rowLimit,
    IPrepareCallbacks* callbacks)
{
    return TPrepareController(callbacks, source, rowLimit, timestamp).Run();
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TPlanFragment* serialized, const TPlanFragment& fragment)
{
    ToProto(serialized->mutable_head(), fragment.GetHead());
    ToProto(serialized->mutable_id(), fragment.Id());
    serialized->set_timestamp(fragment.GetContext()->GetTimestamp());
    serialized->set_row_limit(fragment.GetContext()->GetRowLimit());
}

TPlanFragment FromProto(const NProto::TPlanFragment& serialized)
{
    auto context = New<TPlanContext>(serialized.timestamp(), serialized.row_limit());
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
    serialized->set_incomplete(queryResult.Incomplete);
}

TQueryStatistics FromProto(const NProto::TQueryStatistics& serialized)
{
    TQueryStatistics result;

    result.RowsRead = serialized.rows_read();
    result.RowsWritten = serialized.rows_written();
    result.SyncTime = TDuration(serialized.sync_time());
    result.AsyncTime = TDuration(serialized.async_time());
    result.Incomplete = serialized.incomplete();

    return result;
}

} // namespace NQueryClient
} // namespace NYT

