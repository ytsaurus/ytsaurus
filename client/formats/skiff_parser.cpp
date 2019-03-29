#include "skiff_parser.h"

#include "helpers.h"
#include "parser.h"
#include "yson_map_to_unversioned_value.h"

#include <yt/core/skiff/schema_match.h>
#include <yt/core/skiff/parser.h>

#include <yt/client/table_client/name_table.h>
#include <yt/client/table_client/table_consumer.h>
#include <yt/client/table_client/value_consumer.h>

#include <yt/core/concurrency/coroutine.h>

#include <yt/core/yson/parser.h>

#include <util/generic/strbuf.h>
#include <util/stream/zerocopy.h>

namespace NYT::NFormats {

using namespace NTableClient;
using namespace NSkiff;

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

    Y_FORCE_INLINE void OnStringScalar(TStringBuf value, ui16 columnId)
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

    Y_FORCE_INLINE void OnYsonString(TStringBuf value, ui16 columnId)
    {
        YsonToUnversionedValueConverter_.SetColumnIndex(columnId);
        ParseYsonStringBuffer(
            value,
            NYson::EYsonType::Node,
            &YsonToUnversionedValueConverter_);
    }

    Y_FORCE_INLINE void OnOtherColumns(TStringBuf value)
    {
        ParseYsonStringBuffer(
            value,
            NYson::EYsonType::Node,
            &OtherColumnsConsumer_);
    }

private:
    IValueConsumer* ValueConsumer_;

    TYsonToUnversionedValueConverter YsonToUnversionedValueConverter_;
    TYsonMapToUnversionedValueConverter OtherColumnsConsumer_;
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
        IValueConsumer* consumer)
    {
        Consumer_.reset(new TSkiffConsumer(consumer));
        Parser_.reset(new TSkiffMultiTableParser<TSkiffConsumer>(
            Consumer_.get(),
            {skiffSchema},
            {tableColumnIds},
            RangeIndexColumnName,
            RowIndexColumnName));
    }

    void Read(TStringBuf data) override
    {
        if (!data.empty()) {
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
    IValueConsumer* consumer)
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

} // namespace NYT::NFormats
