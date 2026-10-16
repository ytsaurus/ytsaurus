#include "control_table.h"

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/rowset.h>
#include <yt/yt/client/api/transaction.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/transaction_client/public.h>

namespace NYT::NFlow {

using namespace NApi;
using namespace NTableClient;
using namespace NTransactionClient;
using namespace NYPath;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TFuture<std::optional<TYsonString>> TControlTable::Read(
    const IClientPtr& client,
    const TYPath& controlTablePath,
    TStringBuf key)
{
    auto nameTable = New<TNameTable>();
    auto keyField = nameTable->GetIdOrRegisterName("key");
    auto valueField = nameTable->GetIdOrRegisterName("value");

    auto rowBuffer = New<TRowBuffer>();
    std::vector<TLegacyKey> keysToLookup;
    auto keyRow = rowBuffer->AllocateUnversioned(1);
    keyRow[0] = rowBuffer->CaptureValue(MakeUnversionedStringValue(key, keyField));
    keysToLookup.push_back(keyRow);
    auto range = MakeSharedRange(std::move(keysToLookup), std::move(rowBuffer));

    TLookupRowsOptions options;
    options.ColumnFilter = TColumnFilter({valueField});
    options.KeepMissingRows = true;
    options.Timestamp = SyncLastCommittedTimestamp;

    using TLookupResult = TLookupRowsResult<IRowset<TUnversionedRow>>;
    return client->LookupRows(controlTablePath, std::move(nameTable), range, options)
        .Apply(BIND([] (const TLookupResult& result) -> std::optional<TYsonString> {
            const auto& rows = result.Rowset->GetRows();
            if (rows.Empty() || !rows[0]) {
                return std::nullopt;
            }
            const auto& row = rows[0];
            if (row.GetCount() == 0 || row[0].Type == EValueType::Null) {
                return std::nullopt;
            }
            // Copy the bytes out of the rowset into an owning YSON string.
            return TYsonString(TString(row[0].AsStringBuf()));
        }));
}

void TControlTable::Write(
    const ITransactionPtr& transaction,
    const TYPath& controlTablePath,
    TStringBuf key,
    const TYsonString& value)
{
    auto nameTable = New<TNameTable>();
    auto keyField = nameTable->GetIdOrRegisterName("key");
    auto valueField = nameTable->GetIdOrRegisterName("value");

    auto rowBuffer = New<TRowBuffer>();
    std::vector<TRowModification> rows;
    auto row = rowBuffer->AllocateUnversioned(2);
    row[0] = rowBuffer->CaptureValue(MakeUnversionedStringValue(key, keyField));
    row[1] = rowBuffer->CaptureValue(MakeUnversionedAnyValue(value.AsStringBuf(), valueField));
    rows.push_back(NRowModifications::TWriteRow(row));

    transaction->ModifyRows(
        controlTablePath,
        std::move(nameTable),
        MakeSharedRange(std::move(rows), std::move(rowBuffer)));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
