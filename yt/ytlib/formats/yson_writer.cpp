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

TFuture<void> TSchemafulYsonWriter::Open(
    const TTableSchema& schema,
    const TNullable<TKeyColumns>& /*keyColumns*/)
{
    Schema_ = schema;
    return VoidFuture;
}

TFuture<void> TSchemafulYsonWriter::Close()
{
    return VoidFuture;
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
                case EValueType::Int64:
                    Writer_.OnInt64Scalar(value.Data.Int64);
                    break;
                case EValueType::Uint64:
                    Writer_.OnUint64Scalar(value.Data.Uint64);
                    break;
                case EValueType::Double:
                    Writer_.OnDoubleScalar(value.Data.Double);
                    break;
                case EValueType::Boolean:
                    Writer_.OnBooleanScalar(value.Data.Boolean);
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

    Result_ = Stream_->Write(Buffer_.Begin(), Buffer_.Size());
    return Result_.IsSet() && Result_.Get().IsOK();
}

TFuture<void> TSchemafulYsonWriter::GetReadyEvent()
{
    return Result_;
}

////////////////////////////////////////////////////////////////////////////////


} // namespace NFormats
} // namespace NYT
