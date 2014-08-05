#include "stdafx.h" 

#include "schemaless_writer_adapter.h"

#include <ytlib/new_table_client/name_table.h>

#include <core/misc/error.h>
#include <core/yson/consumer.h>

#include <core/actions/future.h>

namespace NYT {
namespace NFormats {

using namespace NVersionedTableClient;
using namespace NYson;

TAsyncError TSchemalessWriterAdapter::StaticError_ = MakeFuture(TError());

////////////////////////////////////////////////////////////////////////////////

TSchemalessWriterAdapter::TSchemalessWriterAdapter(
    std::unique_ptr<NYson::IYsonConsumer> consumer,
    TNameTablePtr nameTable)
    : Consumer_(std::move(consumer))
    , NameTable_(nameTable)
{ }

TAsyncError TSchemalessWriterAdapter::Open()
{
    return MakeFuture(TError());
}

bool TSchemalessWriterAdapter::Write(const std::vector<TUnversionedRow> &rows)
{
    try {
        for (auto row : rows) {
            Consumer_->OnListItem();
            Consumer_->OnBeginMap();
            for (auto* it = row.Begin(); it != row.End(); ++it) {
                auto& value = *it;
                Consumer_->OnKeyedItem(NameTable_->GetName(value.Id));
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
                    case EValueType::Any:
                        Consumer_->OnRaw(TStringBuf(value.Data.String, value.Length), EYsonType::Node);
                        break;
                    default:
                        YUNREACHABLE();
                }
            }
            Consumer_->OnEndMap();
        }
    } catch (const std::exception& ex) {
        Error_ = TError(ex);
        return false;
    }

    return true;
}

TAsyncError TSchemalessWriterAdapter::GetReadyEvent()
{
    return MakeFuture(Error_);
}

TAsyncError TSchemalessWriterAdapter::Close()
{
    return MakeFuture(Error_);
}



////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
