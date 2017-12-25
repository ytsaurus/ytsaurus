#include "skiff_parser.h"

#include "skiff_schema_match.h"

#include "parser.h"

#include <yt/ytlib/table_client/name_table.h>
#include <yt/ytlib/table_client/table_consumer.h>
#include <yt/ytlib/table_client/value_consumer.h>

#include <yt/core/concurrency/coroutine.h>
#include <yt/core/skiff/skiff.h>
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

struct TParserFieldInfo
{
    ui16 ColumnId;
    EWireType WireType;
    bool Required;

    TParserFieldInfo(
        ui16 columnId,
        EWireType wireType,
        bool required)
        : ColumnId(columnId)
        , WireType(wireType)
        , Required(required)
    { }
};

////////////////////////////////////////////////////////////////////////////////

struct TParserTableDescription
{
    std::vector<TParserFieldInfo> DenseFields;
    std::vector<TParserFieldInfo> SparseFields;
    bool HasOtherColumns = false;
};

////////////////////////////////////////////////////////////////////////////////

class TSkiffPullParser
{
public:
    TSkiffPullParser(
        IInputStream* input,
        TParserTableDescription rowDescription,
        const TSkiffSchemaPtr& skiffSchema,
        IValueConsumer* consumer)
        : Consumer_(consumer)
        , Parser_(skiffSchema, input)
        , RowDescription_(std::move(rowDescription))
        , OtherColumnsConsumer_(consumer)
    {
        YsonToUnversionedValueConverter_.SetValueConsumer(consumer);
    }

    Y_FORCE_INLINE void ParseField(const TParserFieldInfo& fieldInfo)
    {
        if (!fieldInfo.Required) {
            ui8 tag = Parser_.ParseVariant8Tag();
            if (tag == 0) {
                Consumer_->OnValue(MakeUnversionedSentinelValue(EValueType::Null, fieldInfo.ColumnId));
                return;
            } else if (tag > 1) {
                THROW_ERROR_EXCEPTION("Found bad variant8 tag %Qv when parsing optional field %Qv",
                    tag,
                    Consumer_->GetNameTable()->GetName(fieldInfo.ColumnId));
            }
        }
        switch (fieldInfo.WireType) {
            case EWireType::Yson32:
                Parser_.ParseYson32(&String_);
                YsonToUnversionedValueConverter_.SetColumnIndex(fieldInfo.ColumnId);
                ParseYsonStringBuffer(
                    String_,
                    NYson::EYsonType::Node,
                    &YsonToUnversionedValueConverter_);
                break;
            case EWireType::Int64:
                Consumer_->OnValue(MakeUnversionedInt64Value(Parser_.ParseInt64(), fieldInfo.ColumnId));
                break;
            case EWireType::Uint64:
                Consumer_->OnValue(MakeUnversionedUint64Value(Parser_.ParseUint64(), fieldInfo.ColumnId));
                break;
            case EWireType::Double:
                Consumer_->OnValue(MakeUnversionedDoubleValue(Parser_.ParseDouble(), fieldInfo.ColumnId));
                break;
            case EWireType::Boolean:
                Consumer_->OnValue(MakeUnversionedBooleanValue(Parser_.ParseBoolean(), fieldInfo.ColumnId));
                break;
            case EWireType::String32:
                Parser_.ParseString32(&String_);
                Consumer_->OnValue(MakeUnversionedStringValue(String_, fieldInfo.ColumnId));
                break;
            default:
                // Other types should be filtered out when we parsed skiff schema
                Y_UNREACHABLE();
        }
    }

    void Run()
    {
        while (Parser_.HasMoreData()) {
            auto tag = Parser_.ParseVariant16Tag();
            if (tag > 0) {
                THROW_ERROR_EXCEPTION(
                    "Unkwnown table index varint16 tag %v",
                    tag);
            }
            Consumer_->OnBeginRow();

            for (const auto& field : RowDescription_.DenseFields) {
                ParseField(field);
            }

            if (!RowDescription_.SparseFields.empty()) {
                for (auto sparseFieldIdx = Parser_.ParseVariant16Tag();
                    sparseFieldIdx != EndOfSequenceTag<ui16>();
                    sparseFieldIdx = Parser_.ParseVariant16Tag())
                {
                    if (sparseFieldIdx > RowDescription_.SparseFields.size()) {
                        THROW_ERROR_EXCEPTION("Bad sparse field index %Qv, total sparse field count %Qv",
                            sparseFieldIdx,
                            RowDescription_.SparseFields.size());
                    }
                    ParseField(RowDescription_.SparseFields[sparseFieldIdx]);
                }
            }

            if (RowDescription_.HasOtherColumns) {
                Parser_.ParseYson32(&String_);
                ParseYsonStringBuffer(
                    String_,
                    NYson::EYsonType::Node,
                    &OtherColumnsConsumer_);
            }

            Consumer_->OnEndRow();
        }
    }

private:
    IValueConsumer* const Consumer_;
    TCheckedInDebugSkiffParser Parser_;
    TParserTableDescription RowDescription_;

    // String that we parse string32 into.
    TString String_;

    TYsonToUnversionedValueConverter YsonToUnversionedValueConverter_;
    TOtherColumnsConsumer OtherColumnsConsumer_;
};

////////////////////////////////////////////////////////////////////////////////

using TSkiffParserCoroutine = NConcurrency::TCoroutine<void(TStringBuf)>;

////////////////////////////////////////////////////////////////////////////////

class TCoroStream
    : public IZeroCopyInput
{
public:
    TCoroStream(TStringBuf data, TSkiffParserCoroutine* coroutine)
        : Coroutine_(coroutine)
        , PendingData_(data)
        , Finished_(data.empty())
    { }

    size_t DoNext(const void** ptr, size_t len) override
    {
        if (PendingData_.empty()) {
            if (Finished_) {
                *ptr = nullptr;
                return 0;
            }
            std::tie(PendingData_) = Coroutine_->Yield();
            if (PendingData_.Empty()) {
                Finished_ = true;
                *ptr = nullptr;
                return 0;
            }
        }
        *ptr = PendingData_.Data();
        len = Min(len, PendingData_.Size());
        PendingData_.Skip(len);
        return len;
    }

    void Complete()
    {
        if (!Finished_) {
            const void* ptr;
            if (!PendingData_.Empty() || DoNext(&ptr, 1)) {
                THROW_ERROR_EXCEPTION("Stray data in stream");
            }
        }
    }

private:
    TSkiffParserCoroutine* const Coroutine_;
    TStringBuf PendingData_;
    bool Finished_ = false;
};

////////////////////////////////////////////////////////////////////////////////

class TSkiffPushParser
    : public IParser
{
public:
    TSkiffPushParser(TParserTableDescription rowDescription, const TSkiffSchemaPtr& skiffSchema, IValueConsumer* consumer)
        : Coroutine_(
            BIND([
                skiffSchema = skiffSchema,
                rowDescription = std::move(rowDescription),
                consumer = consumer
            ] (TSkiffParserCoroutine& self, TStringBuf data) {
                ParseProc(rowDescription, skiffSchema, consumer, self, data);
            }))
    { }

    void Read(const TStringBuf& data) override
    {
        if (!data.Empty()) {
            YCHECK(!Coroutine_.IsCompleted());
            Coroutine_.Run(data);
        }
    }

    void Finish() override
    {
        YCHECK(!Coroutine_.IsCompleted());
        Coroutine_.Run(TStringBuf());
        YCHECK(Coroutine_.IsCompleted());
    }

private:
    static void ParseProc(
        TParserTableDescription rowDescription,
        TSkiffSchemaPtr skiffSchema,
        IValueConsumer* consumer,
        TSkiffParserCoroutine& self,
        TStringBuf data)
    {
        TCoroStream stream(data, &self);
        TSkiffPullParser parser(&stream, std::move(rowDescription), skiffSchema, consumer);
        parser.Run();
        stream.Complete();
    }

private:
    TSkiffParserCoroutine Coroutine_;
};

////////////////////////////////////////////////////////////////////////////////

static TParserTableDescription CreateParserRowDescription(const TSkiffTableDescription& commonDescription, const TNameTablePtr& nameTable)
{
    TParserTableDescription result;
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
    result.HasOtherColumns = commonDescription.HasOtherColumns;

    for (const auto& field : commonDescription.DenseFieldDescriptionList) {
        result.DenseFields.emplace_back(
            nameTable->GetIdOrRegisterName(field.Name),
            field.DeoptionalizedSchema->GetWireType(),
            field.Required);
    }

    for (const auto& field : commonDescription.SparseFieldDescriptionList) {
        result.SparseFields.emplace_back(
            nameTable->GetIdOrRegisterName(field.Name),
            field.DeoptionalizedSchema->GetWireType(),
            true);
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IParser> CreateParserForSkiff(
    IValueConsumer* consumer,
    TSkiffFormatConfigPtr config,
    int tableIndex)
{
    auto skiffSchemas = ParseSkiffSchemas(config);
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
    auto tableDescriptionList = CreateTableDescriptionList({skiffSchema});
    if (tableDescriptionList.size() != 1) {
        THROW_ERROR_EXCEPTION("Expected to have single table, actual table description count %Qv",
            tableDescriptionList.size());
    }

    auto parserTableDescription = CreateParserRowDescription(tableDescriptionList[0], consumer->GetNameTable());
    return std::make_unique<TSkiffPushParser>(
        parserTableDescription,
        CreateVariant16Schema({skiffSchema}),
        consumer);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
