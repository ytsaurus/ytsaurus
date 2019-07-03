#include "yson_map_to_unversioned_value.h"

namespace NYT::NFormats {

using namespace NTableClient;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TYsonMapToUnversionedValueConverter::TYsonMapToUnversionedValueConverter(IValueConsumer* consumer)
    : Consumer_(consumer)
    , AllowUnknownColumns_(consumer->GetAllowUnknownColumns())
    , NameTable_(consumer->GetNameTable())
{
    ColumnConsumer_.SetValueConsumer(this);
}

void TYsonMapToUnversionedValueConverter::Reset()
{
    YT_VERIFY(InsideValue_ == false);
}

void TYsonMapToUnversionedValueConverter::OnStringScalar(TStringBuf value)
{
    if (Y_LIKELY(InsideValue_)) {
        ColumnConsumer_.OnStringScalar(value);
    } else {
        THROW_ERROR_EXCEPTION("YSON map expected");
    }
}

void TYsonMapToUnversionedValueConverter::OnInt64Scalar(i64 value)
{
    if (Y_LIKELY(InsideValue_)) {
        ColumnConsumer_.OnInt64Scalar(value);
    } else {
        THROW_ERROR_EXCEPTION("YSON map expected");
    }
}

void TYsonMapToUnversionedValueConverter::OnUint64Scalar(ui64 value)
{
    if (Y_LIKELY(InsideValue_)) {
        ColumnConsumer_.OnUint64Scalar(value);
    } else {
        THROW_ERROR_EXCEPTION("YSON map expected");
    }
}

void TYsonMapToUnversionedValueConverter::OnDoubleScalar(double value)
{
    if (Y_LIKELY(InsideValue_)) {
        ColumnConsumer_.OnDoubleScalar(value);
    } else {
        THROW_ERROR_EXCEPTION("YSON map expected");
    }
}

void TYsonMapToUnversionedValueConverter::OnBooleanScalar(bool value)
{
    if (Y_LIKELY(InsideValue_)) {
        ColumnConsumer_.OnBooleanScalar(value);
    } else {
        THROW_ERROR_EXCEPTION("YSON map expected");
    }
}

void TYsonMapToUnversionedValueConverter::OnEntity()
{
    if (Y_LIKELY(InsideValue_)) {
        ColumnConsumer_.OnEntity();
    } else {
        THROW_ERROR_EXCEPTION("YSON map expected");
    }
}

void TYsonMapToUnversionedValueConverter::OnBeginList()
{
    if (Y_LIKELY(InsideValue_)) {
        ColumnConsumer_.OnBeginList();
    } else {
        THROW_ERROR_EXCEPTION("YSON map expected");
    }
}

void TYsonMapToUnversionedValueConverter::OnListItem()
{
    if (Y_LIKELY(InsideValue_)) {
        ColumnConsumer_.OnListItem();
    } else {
        YT_ABORT(); // Should crash on BeginList()
    }
}

void TYsonMapToUnversionedValueConverter::OnBeginMap()
{
    if (Y_LIKELY(InsideValue_)) {
        ColumnConsumer_.OnBeginMap();
    }
}

void TYsonMapToUnversionedValueConverter::OnKeyedItem(TStringBuf name)
{
    if (Y_LIKELY(InsideValue_)) {
        ColumnConsumer_.OnKeyedItem(name);
    } else {
        InsideValue_ = true;
        if (AllowUnknownColumns_) {
            ColumnConsumer_.SetColumnIndex(NameTable_->GetIdOrRegisterName(name));
        } else {
            ColumnConsumer_.SetColumnIndex(NameTable_->GetIdOrThrow(name));
        }
    }
}

void TYsonMapToUnversionedValueConverter::OnEndMap()
{
    if (Y_LIKELY(InsideValue_)) {
        ColumnConsumer_.OnEndMap();
    }
}

void TYsonMapToUnversionedValueConverter::OnBeginAttributes()
{
    if (Y_LIKELY(InsideValue_)) {
        ColumnConsumer_.OnBeginAttributes();
    } else {
        THROW_ERROR_EXCEPTION("YSON map without attributes expected");
    }
}

void TYsonMapToUnversionedValueConverter::OnEndList()
{
    if (Y_LIKELY(InsideValue_)) {
        ColumnConsumer_.OnEndList();
    } else {
        YT_ABORT(); // Should crash on BeginList()
    }
}

void TYsonMapToUnversionedValueConverter::OnEndAttributes()
{
    if (Y_LIKELY(InsideValue_)) {
        ColumnConsumer_.OnEndAttributes();
    } else {
        YT_ABORT(); // Should crash on BeginAttributes()
    }
}

const TNameTablePtr& TYsonMapToUnversionedValueConverter::GetNameTable() const
{
    YT_ABORT();
}

bool TYsonMapToUnversionedValueConverter::GetAllowUnknownColumns() const
{
    YT_ABORT();
}

void TYsonMapToUnversionedValueConverter::OnBeginRow()
{
    YT_ABORT();
}

void TYsonMapToUnversionedValueConverter::OnValue(const TUnversionedValue& value)
{
    InsideValue_ = false;
    Consumer_->OnValue(value);
}

void TYsonMapToUnversionedValueConverter::OnEndRow()
{
    YT_ABORT();
}

const NTableClient::TTableSchema& TYsonMapToUnversionedValueConverter::GetSchema() const
{
    YT_ABORT();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats
