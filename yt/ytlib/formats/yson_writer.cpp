#include "stdafx.h"
#include "yson_writer.h"
#include "config.h"

#include <core/concurrency/async_stream.h>

namespace NYT {
namespace NFormats {

using namespace NConcurrency;
using namespace NYson;
using namespace NVersionedTableClient;

////////////////////////////////////////////////////////////////////////////////

static auto PresetResult = MakeFuture(TError());

TSchemafulYsonWriter::TSchemafulYsonWriter(
    IAsyncOutputStreamPtr stream,
    TYsonFormatConfigPtr config)
    : Stream_(stream)
    , Writer_(
        &Buffer_,
        config->Format,
        EYsonType::ListFragment,
        true)
{ }

TAsyncError TSchemafulYsonWriter::Open(
    const TTableSchema& schema,
    const TNullable<TKeyColumns>& /*keyColumns*/)
{
    Schema_ = schema;
    return PresetResult;
}

TAsyncError TSchemafulYsonWriter::Close()
{
    return PresetResult;
}

bool TSchemafulYsonWriter::Write(const std::vector<TUnversionedRow>& rows)
{
    Buffer_.Clear();

    int columnCount = static_cast<int>(Schema_.Columns().size());
    for (auto row : rows) {
        YASSERT(row.GetCount() >= columnCount);
        Writer_.OnBeginMap();
        for (int index = 0; index < columnCount; ++index) {
            const auto& value = row[index];
            if (value.Type == EValueType::Null)
                continue;

            const auto& column = Schema_.Columns()[index];
            Writer_.OnKeyedItem(column.Name);

            switch (value.Type) {
                case EValueType::Integer:
                    Writer_.OnIntegerScalar(value.Data.Integer);
                    break;
                case EValueType::Double:
                    Writer_.OnDoubleScalar(value.Data.Double);
                    break;
                case EValueType::String:
                    Writer_.OnStringScalar(TStringBuf(value.Data.String, value.Length));
                    break;
                case EValueType::Any:
                    Writer_.OnRaw(TStringBuf(value.Data.String, value.Length));
                    break;
                default:
                    YUNREACHABLE();
            }
        }
        Writer_.OnEndMap();
    }

    return Stream_->Write(Buffer_.Begin(), Buffer_.Size());
}

TAsyncError TSchemafulYsonWriter::GetReadyEvent()
{
    return Stream_->GetReadyEvent();
}

////////////////////////////////////////////////////////////////////////////////


} // namespace NFormats
} // namespace NYT
