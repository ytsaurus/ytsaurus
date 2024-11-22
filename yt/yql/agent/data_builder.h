#pragma once

#include <yql/essentials/public/result_format/yql_result_format_data.h>
#include <yt/yt/client/table_client/value_consumer.h>

namespace NYT::NYqlAgent {
class TDataBuilder : public NYql::NResult::IDataVisitor {
public:
    TDataBuilder(NTableClient::IValueConsumer* consumer);
private:
    void OnVoid() final;
    void OnNull() final;
    void OnEmptyList() final;
    void OnEmptyDict() final;
    void OnBool(bool value) final;
    void OnInt8(i8 value) final;
    void OnUint8(ui8 value) final;
    void OnInt16(i16 value) final;
    void OnUint16(ui16 value) final;
    void OnInt32(i32 value) final;
    void OnUint32(ui32 value) final;
    void OnInt64(i64 value) final;
    void OnUint64(ui64 value) final;
    void OnFloat(float value) final;
    void OnDouble(double value) final;
    void OnString(TStringBuf value, bool isUtf8) final;
    void OnUtf8(TStringBuf value) final;
    void OnYson(TStringBuf value, bool isUtf8) final;
    void OnJson(TStringBuf value) final;
    void OnJsonDocument(TStringBuf value) final;
    void OnUuid(TStringBuf value, bool isUtf8) final;
    void OnDyNumber(TStringBuf value, bool isUtf8) final;
    void OnDate(ui16 value) final;
    void OnDatetime(ui32 value) final;
    void OnTimestamp(ui64 value) final;
    void OnTzDate(TStringBuf value) final;
    void OnTzDatetime(TStringBuf value) final;
    void OnTzTimestamp(TStringBuf value) final;
    void OnInterval(i64 value) final;
    void OnDate32(i32 value) final;
    void OnDatetime64(i64 value) final;
    void OnTimestamp64(i64 value) final;
    void OnTzDate32(TStringBuf value) final;
    void OnTzDatetime64(TStringBuf value) final;
    void OnTzTimestamp64(TStringBuf value) final;
    void OnInterval64(i64 value) final;
    void OnDecimal(TStringBuf value) final;
    void OnBeginOptional() final;
    void OnBeforeOptionalItem() final;
    void OnAfterOptionalItem() final;
    void OnEmptyOptional() final;
    void OnEndOptional() final;
    void OnBeginList() final;
    void OnBeforeListItem() final;
    void OnAfterListItem() final;
    void OnEndList() final;
    void OnBeginTuple() final;
    void OnBeforeTupleItem() final;
    void OnAfterTupleItem() final;
    void OnEndTuple() final;
    void OnBeginStruct() final;
    void OnBeforeStructItem() final;
    void OnAfterStructItem() final;
    void OnEndStruct() final;
    void OnBeginDict() final;
    void OnBeforeDictItem() final;
    void OnBeforeDictKey() final;
    void OnAfterDictKey() final;
    void OnBeforeDictPayload() final;
    void OnAfterDictPayload() final;
    void OnAfterDictItem() final;
    void OnEndDict() final;
    void OnBeginVariant(ui32 index) final;
    void OnEndVariant() final;
    void OnPg(TMaybe<TStringBuf> value, bool isUtf8) final;

    void AddNull();
    void AddBoolean(bool value);
    void AddSigned(i64 value);
    void AddUnsigned(ui64 value);
    void AddReal(double value);
    void AddString(TStringBuf value);
    void AddYson(TStringBuf value);

    void BeginList();
    void NextItem();
    void EndList();

    NTableClient::IValueConsumer *const ValueConsumer_;
    TBlobOutput ValueBuffer_;
    NYson::TBufferedBinaryYsonWriter ValueWriter_;

    int Depth_ = -2; // Starts from table level: -2 List< -1 Struct< 0 ... >>
    int ColumnIndex_ = 0;
};

}

