#include "skiff_parser.h"

#include "parser.h"

#include <yt/core/skiff/schema_match.h>
#include <yt/core/skiff/parser.h>

#include <yt/ytlib/table_client/name_table.h>
#include <yt/ytlib/table_client/table_consumer.h>
#include <yt/ytlib/table_client/value_consumer.h>

#include <yt/core/concurrency/coroutine.h>

#include <yt/core/yson/parser.h>

#include <util/generic/strbuf.h>
#include <util/stream/zerocopy.h>

namespace NYT {
namespace NFormats {

using namespace NTableClient;
using namespace NSkiff;

////////////////////////////////////////////////////////////////////////////////

class TOtherColumnsConsumer
    : public NYson::TYsonConsumerBase
    , private IValueConsumer
{
public:
    explicit TOtherColumnsConsumer(IValueConsumer* consumer)
        : Consumer_(consumer)
        , AllowUnknownColumns_(consumer->GetAllowUnknownColumns())
        , NameTable_(consumer->GetNameTable())
    {
        ColumnConsumer_.SetValueConsumer(this);
    }

    void Reset()
    {
        YCHECK(InsideValue_ == false);
    }

    virtual void OnStringScalar(const TStringBuf& value) override
    {
        if (InsideValue_) {
            ColumnConsumer_.OnStringScalar(value);
        } else {
            THROW_ERROR_EXCEPTION("\"$other_columns\" must be a map");
        }
    }

    virtual void OnInt64Scalar(i64 value) override
    {
        if (InsideValue_) {
            ColumnConsumer_.OnInt64Scalar(value);
        } else {
            THROW_ERROR_EXCEPTION("\"$other_columns\" must be a map");
        }
    }

    virtual void OnUint64Scalar(ui64 value) override
    {
        if (InsideValue_) {
            ColumnConsumer_.OnUint64Scalar(value);
        } else {
            THROW_ERROR_EXCEPTION("\"$other_columns\" must be a map");
        }
    }

    virtual void OnDoubleScalar(double value) override
    {
        if (InsideValue_) {
            ColumnConsumer_.OnDoubleScalar(value);
        } else {
            THROW_ERROR_EXCEPTION("\"$other_columns\" must be a map");
        }
    }

    virtual void OnBooleanScalar(bool value) override
    {
        if (InsideValue_) {
            ColumnConsumer_.OnBooleanScalar(value);
        } else {
            THROW_ERROR_EXCEPTION("\"$other_columns\" must be a map");
        }
    }

    virtual void OnEntity() override
    {
        if (InsideValue_) {
            ColumnConsumer_.OnEntity();
        } else {
            THROW_ERROR_EXCEPTION("\"$other_columns\" must be a map");
        }
    }

    virtual void OnBeginList() override
    {
        if (InsideValue_) {
            ColumnConsumer_.OnBeginList();
        } else {
            THROW_ERROR_EXCEPTION("\"$other_columns\" must be a map");
        }
    }

    virtual void OnListItem() override
    {
        if (InsideValue_) {
            ColumnConsumer_.OnListItem();
        } else {
            Y_UNREACHABLE(); // Should crash on BeginList()
        }
    }

    virtual void OnBeginMap() override
    {
        if (InsideValue_) {
            ColumnConsumer_.OnBeginMap();
        }
    }

    virtual void OnKeyedItem(const TStringBuf& name) override
    {
        if (InsideValue_) {
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

    virtual void OnEndMap() override
    {
        if (InsideValue_) {
            ColumnConsumer_.OnEndMap();
        }
    }

    virtual void OnBeginAttributes() override
    {
        if (InsideValue_) {
            ColumnConsumer_.OnBeginAttributes();
        } else {
            THROW_ERROR_EXCEPTION("\"$other_columns\" cannot have attributes");
        }
    }

    virtual void OnEndList() override
    {
        if (InsideValue_) {
            ColumnConsumer_.OnEndList();
        } else {
            Y_UNREACHABLE(); // Should crash on BeginList()
        }
    }

    virtual void OnEndAttributes() override
    {
        if (InsideValue_) {
            ColumnConsumer_.OnEndAttributes();
        } else {
            Y_UNREACHABLE(); // Should crash on BeginAttributes()
        }
    }

private:
    virtual const TNameTablePtr& GetNameTable() const override
    {
        Y_UNREACHABLE();
    }

    virtual bool GetAllowUnknownColumns() const override
    {
        Y_UNREACHABLE();
    }

    virtual void OnBeginRow() override
    {
        Y_UNREACHABLE();
    }

    virtual void OnValue(const TUnversionedValue& value) override
    {
        InsideValue_ = false;
        Consumer_->OnValue(value);
    }

    virtual void OnEndRow() override
    {
        Y_UNREACHABLE();
    }

private:
    IValueConsumer* const Consumer_;
    const bool AllowUnknownColumns_;
    TNameTablePtr NameTable_;
    TYsonToUnversionedValueConverter ColumnConsumer_;
    bool InsideValue_ = false;
};

////////////////////////////////////////////////////////////////////////////////

class TSkiffConsumer
{
public:
    TSkiffConsumer(IValueConsumer* valueConsumer)
        : ValueConsumer_(valueConsumer)
        , OtherColumnsConsumer_(valueConsumer)
    {
        YsonToUnversionedValueConverter_.SetValueConsumer(valueConsumer);
    }

    Y_FORCE_INLINE void OnBeginRow(ui16 schemaIndex)
    {
        if (schemaIndex > 0) {
            THROW_ERROR_EXCEPTION(
                "Unkwnown table index varint16 tag %v",
                schemaIndex);
        }
        ValueConsumer_->OnBeginRow();
    }

    Y_FORCE_INLINE void OnEndRow()
    {
        ValueConsumer_->OnEndRow();
    }

    Y_FORCE_INLINE void OnStringScalar(const TStringBuf& value, ui16 columnId)
    {
        ValueConsumer_->OnValue(MakeUnversionedStringValue(value, columnId));
    }

    Y_FORCE_INLINE void OnInt64Scalar(i64 value, ui16 columnId)
    {
        ValueConsumer_->OnValue(MakeUnversionedInt64Value(value, columnId));
    }

    Y_FORCE_INLINE void OnUint64Scalar(ui64 value, ui16 columnId)
    {
        ValueConsumer_->OnValue(MakeUnversionedUint64Value(value, columnId));
    }

    Y_FORCE_INLINE void OnDoubleScalar(double value, ui16 columnId)
    {
        ValueConsumer_->OnValue(MakeUnversionedDoubleValue(value, columnId));
    }

    Y_FORCE_INLINE void OnBooleanScalar(bool value, ui16 columnId)
    {
        ValueConsumer_->OnValue(MakeUnversionedBooleanValue(value, columnId));
    }

    Y_FORCE_INLINE void OnEntity(ui16 columnId)
    {
        ValueConsumer_->OnValue(MakeUnversionedSentinelValue(EValueType::Null, columnId));
    }

    Y_FORCE_INLINE void OnYsonString(const TStringBuf& value, ui16 columnId)
    {
        YsonToUnversionedValueConverter_.SetColumnIndex(columnId);
        ParseYsonStringBuffer(
            value,
            NYson::EYsonType::Node,
            &YsonToUnversionedValueConverter_);
    }

    Y_FORCE_INLINE void OnOtherColumns(const TStringBuf& value)
    {
        ParseYsonStringBuffer(
            value,
            NYson::EYsonType::Node,
            &OtherColumnsConsumer_);
    }

private:
    IValueConsumer* ValueConsumer_;

    TYsonToUnversionedValueConverter YsonToUnversionedValueConverter_;
    TOtherColumnsConsumer OtherColumnsConsumer_;
};

////////////////////////////////////////////////////////////////////////////////

class TSkiffPushParser
    : public IParser
{
public:
    TSkiffPushParser()
    { }

    TSkiffPushParser(
        const TSkiffSchemaPtr& skiffSchema,
        const TSkiffTableColumnIds& tableColumnIds,
        NTableClient::IValueConsumer* consumer)
    {
        Consumer_.reset(new TSkiffConsumer(consumer));
        Parser_.reset(new TSkiffMultiTableParser<TSkiffConsumer>(
            Consumer_.get(),
            {skiffSchema},
            {tableColumnIds},
            RangeIndexColumnName,
            RowIndexColumnName));
    }

    void Read(const TStringBuf& data) override
    {
        if (!data.Empty()) {
            Parser_->Read(data);
        }
    }

    void Finish() override
    {
        Parser_->Finish();
    }

private:
    std::unique_ptr<TSkiffConsumer> Consumer_;
    std::unique_ptr<TSkiffMultiTableParser<TSkiffConsumer>> Parser_;
};

////////////////////////////////////////////////////////////////////////////////

static TSkiffTableColumnIds PrepareTableDescription(
    const TSkiffTableDescription& commonDescription,
    const TNameTablePtr& nameTable)
{
    if (commonDescription.KeySwitchFieldIndex) {
        THROW_ERROR_EXCEPTION("Skiff parser does not support %Qv",
            KeySwitchColumnName);
    }
    if (commonDescription.RowIndexFieldIndex) {
        THROW_ERROR_EXCEPTION("Skiff parser does not support %Qv",
            RowIndexColumnName);
    }
    if (commonDescription.RangeIndexFieldIndex) {
        THROW_ERROR_EXCEPTION("Skiff parser does not support %Qv",
            RangeIndexColumnName);
    }

    TSkiffTableColumnIds result;

    for (const auto& field : commonDescription.DenseFieldDescriptionList) {
        auto columnId = nameTable->GetIdOrRegisterName(field.Name);
        result.DenseFieldColumnIds.push_back(columnId);
    }
    for (const auto& field : commonDescription.SparseFieldDescriptionList) {
        auto columnId = nameTable->GetIdOrRegisterName(field.Name);
        result.SparseFieldColumnIds.push_back(columnId);
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IParser> CreateParserForSkiff(
    IValueConsumer* consumer,
    TSkiffFormatConfigPtr config,
    int tableIndex)
{
    auto skiffSchemas = ParseSkiffSchemas(config->SkiffSchemaRegistry, config->TableSkiffSchemas);
    if (tableIndex >= static_cast<int>(skiffSchemas.size())) {
        THROW_ERROR_EXCEPTION("Skiff format config does not describe table #%v",
            tableIndex);
    }
    return CreateParserForSkiff(
        skiffSchemas[tableIndex],
        consumer);
}

std::unique_ptr<IParser> CreateParserForSkiff(
    TSkiffSchemaPtr skiffSchema,
    NTableClient::IValueConsumer* consumer)
{
    auto tableDescriptionList = CreateTableDescriptionList({skiffSchema}, RangeIndexColumnName, RowIndexColumnName);
    if (tableDescriptionList.size() != 1) {
        THROW_ERROR_EXCEPTION("Expected to have single table, actual table description count %Qv",
            tableDescriptionList.size());
    }
    auto tableColumnIds = PrepareTableDescription(tableDescriptionList[0], consumer->GetNameTable());
    return std::make_unique<TSkiffPushParser>(
        skiffSchema,
        tableColumnIds,
        consumer);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
