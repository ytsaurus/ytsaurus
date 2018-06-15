#include "versioned_writer.h"
#include "config.h"

#include <yt/core/concurrency/async_stream.h>

namespace NYT {
namespace NFormats {

using namespace NConcurrency;
using namespace NYson;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

TVersionedWriter::TVersionedWriter(
    NConcurrency::IAsyncOutputStreamPtr stream,
    const NTableClient::TTableSchema& schema,
    const std::function<std::unique_ptr<IFlushableYsonConsumer>(IOutputStream*)>& consumerBuilder)
    : Stream_(stream)
    , Schema_(schema)
    , Consumer_(consumerBuilder(&Buffer_))
{ }

TFuture<void> TVersionedWriter::Close()
{
    return Result_;
}

bool TVersionedWriter::Write(const TRange<TVersionedRow>& rows)
{
    Buffer_.Clear();

    auto consumeUnversionedData = [&] (const TUnversionedValue& value) {
        switch (value.Type) {
            case EValueType::Int64:
                Consumer_->OnInt64Scalar(value.Data.Int64);
                break;
            case EValueType::Uint64:
                Consumer_->OnUint64Scalar(value.Data.Uint64);
                break;
            case EValueType::Double:
                Consumer_->OnDoubleScalar(value.Data.Double);
                break;
            case EValueType::Boolean:
                Consumer_->OnBooleanScalar(value.Data.Boolean);
                break;
            case EValueType::String:
                Consumer_->OnStringScalar(TStringBuf(value.Data.String, value.Length));
                break;
            case EValueType::Null:
                Consumer_->OnEntity();
                break;
            case EValueType::Any:
                Consumer_->OnRaw(TStringBuf(value.Data.String, value.Length), EYsonType::Node);
                break;
            default:
                Y_UNREACHABLE();
        }
    };

    for (auto row : rows) {
        if (!row) {
            Consumer_->OnEntity();
            continue;
        }

        Consumer_->OnBeginAttributes();
        {
            Consumer_->OnKeyedItem("write_timestamps");
            Consumer_->OnBeginList();
            for (auto it = row.BeginWriteTimestamps(), jt = row.EndWriteTimestamps(); it != jt; ++it) {
                Consumer_->OnListItem();
                Consumer_->OnUint64Scalar(*it);
            }
            Consumer_->OnEndList();
        }
        {
            Consumer_->OnKeyedItem("delete_timestamps");
            Consumer_->OnBeginList();
            for (auto it = row.BeginDeleteTimestamps(), jt = row.EndDeleteTimestamps(); it != jt; ++it) {
                Consumer_->OnListItem();
                Consumer_->OnUint64Scalar(*it);
            }
            Consumer_->OnEndList();
        }
        Consumer_->OnEndAttributes();

        Consumer_->OnBeginMap();
        for (auto keyBeginIt = row.BeginKeys(), keyEndIt = row.EndKeys(); keyBeginIt != keyEndIt; ++keyBeginIt) {
            const auto& value = *keyBeginIt;
            const auto& column = Schema_.Columns()[value.Id];
            Consumer_->OnKeyedItem(column.Name());
            consumeUnversionedData(value);
        }
        for (auto valuesBeginIt = row.BeginValues(), valuesEndIt = row.EndValues(); valuesBeginIt != valuesEndIt; /**/) {
            auto columnBeginIt = valuesBeginIt;
            auto columnEndIt = columnBeginIt;
            while (columnEndIt < valuesEndIt && columnEndIt->Id == columnBeginIt->Id) {
                ++columnEndIt;
            }

            const auto& column = Schema_.Columns()[columnBeginIt->Id];
            Consumer_->OnKeyedItem(column.Name());
            Consumer_->OnBeginList();
            while (columnBeginIt != columnEndIt) {
                Consumer_->OnListItem();
                Consumer_->OnBeginAttributes();
                Consumer_->OnKeyedItem("timestamp");
                Consumer_->OnUint64Scalar(columnBeginIt->Timestamp);
                Consumer_->OnKeyedItem("aggregate");
                Consumer_->OnBooleanScalar(columnBeginIt->Aggregate);
                Consumer_->OnEndAttributes();
                consumeUnversionedData(*columnBeginIt);
                ++columnBeginIt;
            }
            Consumer_->OnEndList();

            valuesBeginIt = columnEndIt;
        }
        Consumer_->OnEndMap();
    }

    Consumer_->Flush();
    auto buffer = Buffer_.Flush();
    Result_ = Stream_->Write(buffer);
    return Result_.IsSet() && Result_.Get().IsOK();
}

TFuture<void> TVersionedWriter::GetReadyEvent()
{
    return Result_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
