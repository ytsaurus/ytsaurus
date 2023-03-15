#include "table_poller.h"

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/rowset.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <library/cpp/logger/global/global.h>

#include <util/generic/yexception.h>


namespace NRoren {

using namespace NYT;

////////////////////////////////////////////////////////////////////////////////

TTablePoller::TPoller::TPoller(
    NYT::NApi::IClientPtr client,
    const TString& tablePath,
    TDuration refreshPeriod,
    IInvokerPtr invoker,
    NPrivate::TParseRowsetFun parseRowsetFun)
    : Client_(std::move(client))
    , TablePath_(tablePath)
    , ParseRowsetFun_(std::move(parseRowsetFun))
    , RefreshPeriod_(refreshPeriod)
    , FirstRefreshPromise_(NewPromise<void>())
    , FirstRefreshFuture_(FirstRefreshPromise_.ToFuture())
{
    RefreshExecutor_ = New<NYT::NConcurrency::TPeriodicExecutor>(
        invoker,
        BIND([this, this_=::TIntrusivePtr(this)] {
            try {
                Refresh();
            } catch (const std::exception&) {
                WARNING_LOG
                    << "Exception on TablePoller::Refresh (table "
                    << TablePath_ << "):"
                    << CurrentExceptionMessage();
            }
        }),
        RefreshPeriod_);
    RefreshExecutor_->Start();
}

NYT::TFuture<std::any> TTablePoller::TPoller::AsyncGetParsedRows()
{
    return FirstRefreshFuture_.Apply(BIND([this, this_=::TIntrusivePtr(this)] {
        auto guard = Guard(ParsedRowsLock_);
        return ParsedRows_;
    }));
}

void TTablePoller::TPoller::Refresh()
{
    if (!Schema_) {
        FetchSchema();
    }

    NYT::NApi::TSelectRowsOptions options;
    options.Timeout = TDuration::Seconds(60);

    auto rowset = NYT::NConcurrency::WaitFor(
        Client_->SelectRows(
            NYT::Format("* from [%v]", this->TablePath_),
            options))
        .ValueOrThrow()
        .Rowset;

    auto keyColumnName = this->NameTable_->GetName(this->KeyColumnId_);
    auto responseKeyColumnId = rowset
        ->GetNameTable()
        ->GetIdOrThrow(keyColumnName);

    auto parsedRows = ParseRowsetFun_(rowset, responseKeyColumnId);
    {
        auto guard = Guard(ParsedRowsLock_);
        std::swap(ParsedRows_, parsedRows);
    }

    FirstRefreshPromise_.TrySet();
}

void TTablePoller::TPoller::FetchSchema()
{
    NYT::NApi::TGetNodeOptions options;
    options.Timeout = TDuration::Seconds(10);
    auto getNodeResult = Client_->GetNode(TablePath_ + "/@schema", options);
    auto schemaNode = NYT::NConcurrency::WaitFor(getNodeResult)
        .ValueOrThrow();

    Schema_ = NYT::NYTree::ConvertTo<NYT::NTableClient::TTableSchemaPtr>(schemaNode);
    NameTable_ = NYT::NTableClient::TNameTable::FromSchema(*Schema_);

    std::vector<TString> keyColumnNames;
    for (const auto& column: Schema_->Columns()) {
        if (column.SortOrder() && !column.Expression()) {
            keyColumnNames.push_back(column.Name());
        }
    }

    Y_VERIFY(keyColumnNames.size() == 1, "");
    KeyColumnId_ = NameTable_->GetIdOrThrow(keyColumnNames[0]);
}

void TTablePoller::TPoller::SetPeriod(TDuration newRefreshPeriod)
{
    if (RefreshPeriod_ > newRefreshPeriod) {
        RefreshPeriod_ = newRefreshPeriod;
        RefreshExecutor_->SetPeriod(RefreshPeriod_);
    }
}

void TTablePoller::TPoller::Finish()
{
    RefreshExecutor_->Stop();
    // NB. Reset is necessary to remove intrusive ptr cycle.
    RefreshExecutor_.Reset();
}

////////////////////////////////////////////////////////////////////////////////

TTablePoller::TTablePoller(IInvokerPtr invoker)
    : Invoker_(std::move(invoker))
{ }

TTablePoller::~TTablePoller()
{
    auto guard = Guard(PollersLock_);
    for (auto& [key, poller] : Pollers_) {
        poller->Finish();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren
