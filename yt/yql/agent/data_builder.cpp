#include "data_builder.h"

namespace NYT::NYqlAgent {

void TDataBuilder::OnVoid() {
    Cerr << __func__ << Endl;
}
void TDataBuilder::OnNull() {
    Cerr << __func__ << Endl;
}
void TDataBuilder::OnEmptyList() {
    Cerr << __func__ << Endl;
}
void TDataBuilder::OnEmptyDict() {
    Cerr << __func__ << Endl;
}
void TDataBuilder::OnBool(bool value) {
    Cerr << __func__ << '(' << value << ')' << Endl;
}
void TDataBuilder::OnInt8(i8 value) {
    Cerr << __func__ << '(' << value << ')' << Endl;
}
void TDataBuilder::OnUint8(ui8 value) {
    Cerr << __func__ << '(' << value << ')' << Endl;
}
void TDataBuilder::OnInt16(i16 value) {
    Cerr << __func__ << '(' << value << ')' << Endl;
}
void TDataBuilder::OnUint16(ui16 value) {
    Cerr << __func__ << '(' << value << ')' << Endl;
}
void TDataBuilder::OnInt32(i32 value) {
    Cerr << __func__ << '(' << value << ')' << Endl;
}
void TDataBuilder::OnUint32(ui32 value) {
    Cerr << __func__ << '(' << value << ')' << Endl;
}
void TDataBuilder::OnInt64(i64 value) {
    Cerr << __func__ << '(' << value << ')' << Endl;
}
void TDataBuilder::OnUint64(ui64 value) {
    Cerr << __func__ << '(' << value << ')' << Endl;
}
void TDataBuilder::OnFloat(float value) {
    Cerr << __func__ << '(' << value << ')' << Endl;
}
void TDataBuilder::OnDouble(double value) {
    Cerr << __func__ << '(' << value << ')' << Endl;
}
void TDataBuilder::OnString(TStringBuf value, bool isUtf8) {
    Cerr << __func__ << '(' << value << ',' << isUtf8 << ')' << Endl;
}
void TDataBuilder::OnUtf8(TStringBuf value) {
    Cerr << __func__ << '(' << value << ')' << Endl;
}
void TDataBuilder::OnYson(TStringBuf value, bool isUtf8) {
    Cerr << __func__ << '(' << value << ',' << isUtf8 << ')' << Endl;
}
void TDataBuilder::OnJson(TStringBuf value) {
    Cerr << __func__ << '(' << value << ')' << Endl;
}
void TDataBuilder::OnJsonDocument(TStringBuf value) {
    Cerr << __func__ << '(' << value << ')' << Endl;
}
void TDataBuilder::OnUuid(TStringBuf value, bool isUtf8) {
    Cerr << __func__ << '(' << value << ',' << isUtf8 << ')' << Endl;
}
void TDataBuilder::OnDyNumber(TStringBuf value, bool isUtf8) {
    Cerr << __func__ << '(' << value << ',' << isUtf8 << ')' << Endl;
}
void TDataBuilder::OnDate(ui16 value) {
    Cerr << __func__ << '(' << value << ')' << Endl;
}
void TDataBuilder::OnDatetime(ui32 value) {
    Cerr << __func__ << '(' << value << ')' << Endl;
}
void TDataBuilder::OnTimestamp(ui64 value) {
    Cerr << __func__ << '(' << value << ')' << Endl;
}
void TDataBuilder::OnTzDate(TStringBuf value) {
    Cerr << __func__ << '(' << value << ')' << Endl;
}
void TDataBuilder::OnTzDatetime(TStringBuf value) {
    Cerr << __func__ << '(' << value << ')' << Endl;
}
void TDataBuilder::OnTzTimestamp(TStringBuf value) {
    Cerr << __func__ << '(' << value << ')' << Endl;
}
void TDataBuilder::OnInterval(i64 value) {
    Cerr << __func__ << '(' << value << ')' << Endl;
}
void TDataBuilder::OnDate32(i32 value) {
    Cerr << __func__ << '(' << value << ')' << Endl;
}
void TDataBuilder::OnDatetime64(i64 value) {
    Cerr << __func__ << '(' << value << ')' << Endl;
}
void TDataBuilder::OnTimestamp64(i64 value) {
    Cerr << __func__ << '(' << value << ')' << Endl;
}
void TDataBuilder::OnTzDate32(TStringBuf value) {
    Cerr << __func__ << '(' << value << ')' << Endl;
}
void TDataBuilder::OnTzDatetime64(TStringBuf value) {
    Cerr << __func__ << '(' << value << ')' << Endl;
}
void TDataBuilder::OnTzTimestamp64(TStringBuf value) {
    Cerr << __func__ << '(' << value << ')' << Endl;
}
void TDataBuilder::OnInterval64(i64 value) {
    Cerr << __func__ << '(' << value << ')' << Endl;
}
void TDataBuilder::OnDecimal(TStringBuf value) {
    Cerr << __func__ << '(' << value << ')' << Endl;
}
void TDataBuilder::OnBeginOptional() {
    Cerr << __func__ << Endl;
}
void TDataBuilder::OnBeforeOptionalItem() {
    Cerr << __func__ << Endl;
}
void TDataBuilder::OnAfterOptionalItem() {
    Cerr << __func__ << Endl;
}
void TDataBuilder::OnEmptyOptional() {
    Cerr << __func__ << Endl;
}
void TDataBuilder::OnEndOptional() {
    Cerr << __func__ << Endl;
}
void TDataBuilder::OnBeginList() {
    Cerr << __func__ << Endl;
}
void TDataBuilder::OnBeforeListItem() {
    Cerr << __func__ << Endl;
}
void TDataBuilder::OnAfterListItem() {
    Cerr << __func__ << Endl;
}
void TDataBuilder::OnEndList() {
    Cerr << __func__ << Endl;
}
void TDataBuilder::OnBeginTuple() {
    Cerr << __func__ << Endl;
}
void TDataBuilder::OnBeforeTupleItem() {
    Cerr << __func__ << Endl;
}
void TDataBuilder::OnAfterTupleItem() {
    Cerr << __func__ << Endl;
}
void TDataBuilder::OnEndTuple() {
    Cerr << __func__ << Endl;
}
void TDataBuilder::OnBeginStruct() {
    Cerr << __func__ << Endl;
}
void TDataBuilder::OnBeforeStructItem() {
    Cerr << __func__ << Endl;
}
void TDataBuilder::OnAfterStructItem() {
    Cerr << __func__ << Endl;
}
void TDataBuilder::OnEndStruct() {
    Cerr << __func__ << Endl;
}
void TDataBuilder::OnBeginDict() {
    Cerr << __func__ << Endl;
}
void TDataBuilder::OnBeforeDictItem() {
    Cerr << __func__ << Endl;
}
void TDataBuilder::OnBeforeDictKey() {
    Cerr << __func__ << Endl;
}
void TDataBuilder::OnAfterDictKey() {
    Cerr << __func__ << Endl;
}
void TDataBuilder::OnBeforeDictPayload() {
    Cerr << __func__ << Endl;
}
void TDataBuilder::OnAfterDictPayload() {
    Cerr << __func__ << Endl;
}
void TDataBuilder::OnAfterDictItem() {
    Cerr << __func__ << Endl;
}
void TDataBuilder::OnEndDict() {
    Cerr << __func__ << Endl;
}
void TDataBuilder::OnBeginVariant(ui32 index) {
    Cerr << __func__ << '(' << index << ')' << Endl;
}
void TDataBuilder::OnEndVariant() {
    Cerr << __func__ << Endl;
}
void TDataBuilder::OnPg(TMaybe<TStringBuf> value, bool isUtf8) {
    Cerr << __func__ << '(' << value << ',' << isUtf8 << ')' << Endl;
}

}
