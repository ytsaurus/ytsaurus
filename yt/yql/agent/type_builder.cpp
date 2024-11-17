#include "type_builder.h"

namespace NYT::NYqlAgent {

using namespace NTableClient;

TTypeBuilder::TTypeBuilder() {}

TLogicalTypePtr TTypeBuilder::GetResult() const {
    return Last;
}

template<>
TLogicalTypePtr TTypeBuilder::Pop<TLogicalTypePtr>() {
    return std::move(Last);
}

template<class T>
T TTypeBuilder::Pop() {
    auto items = std::move(std::get<T>(ItemsStack.top()));
    ItemsStack.pop();
    Stack.pop();
    return items;
}

void TTypeBuilder::OnVoid() {
    Cerr << __func__ << Endl;
   THROW_ERROR_EXCEPTION("%s not implemented.", __func__);
}
void TTypeBuilder::OnNull() {
    Cerr << __func__ << Endl;
    Push(NullLogicalType());
}
void TTypeBuilder::OnEmptyList() {
    Cerr << __func__ << Endl;
}
void TTypeBuilder::OnEmptyDict() {
    Cerr << __func__ << Endl;
}
void TTypeBuilder::OnBool() {
    Cerr << __func__ << Endl;
    Push(SimpleLogicalType(ESimpleLogicalValueType::Boolean));
}
void TTypeBuilder::OnInt8() {
    Cerr << __func__ << Endl;
    Push(SimpleLogicalType(ESimpleLogicalValueType::Int8));
}
void TTypeBuilder::OnUint8() {
    Cerr << __func__ << Endl;
    Push(SimpleLogicalType(ESimpleLogicalValueType::Uint8));
}
void TTypeBuilder::OnInt16() {
    Cerr << __func__ << Endl;
    Push(SimpleLogicalType(ESimpleLogicalValueType::Int16));
}
void TTypeBuilder::OnUint16() {
    Cerr << __func__ << Endl;
    Push(SimpleLogicalType(ESimpleLogicalValueType::Uint16));
}
void TTypeBuilder::OnInt32() {
    Cerr << __func__ << Endl;
    Push(SimpleLogicalType(ESimpleLogicalValueType::Int32));
}
void TTypeBuilder::OnUint32() {
    Cerr << __func__ << Endl;
    Push(SimpleLogicalType(ESimpleLogicalValueType::Uint32));
}
void TTypeBuilder::OnInt64() {
    Cerr << __func__ << Endl;
    Push(SimpleLogicalType(ESimpleLogicalValueType::Int64));
}
void TTypeBuilder::OnUint64() {
    Cerr << __func__ << Endl;
    Push(SimpleLogicalType(ESimpleLogicalValueType::Uint64));
}
void TTypeBuilder::OnFloat() {
    Cerr << __func__ << Endl;
    Push(SimpleLogicalType(ESimpleLogicalValueType::Float));
}
void TTypeBuilder::OnDouble() {
    Cerr << __func__ << Endl;
    Push(SimpleLogicalType(ESimpleLogicalValueType::Double));
}
void TTypeBuilder::OnString() {
    Cerr << __func__ << Endl;
    Push(SimpleLogicalType(ESimpleLogicalValueType::String));
}
void TTypeBuilder::OnUtf8() {
    Cerr << __func__ << Endl;
    Push(SimpleLogicalType(ESimpleLogicalValueType::Utf8));
}
void TTypeBuilder::OnYson() {
    Cerr << __func__ << Endl;
   THROW_ERROR_EXCEPTION("%s not implemented.", __func__);
}
void TTypeBuilder::OnJson() {
    Cerr << __func__ << Endl;
    Push(SimpleLogicalType(ESimpleLogicalValueType::Json));
}
void TTypeBuilder::OnJsonDocument() {
    Cerr << __func__ << Endl;
   THROW_ERROR_EXCEPTION("%s not implemented.", __func__);
}
void TTypeBuilder::OnUuid() {
    Cerr << __func__ << Endl;
    Push(SimpleLogicalType(ESimpleLogicalValueType::Uuid));
}
void TTypeBuilder::OnDyNumber() {
    Cerr << __func__ << Endl;
   THROW_ERROR_EXCEPTION("%s not implemented.", __func__);
}
void TTypeBuilder::OnDate() {
    Cerr << __func__ << Endl;
    Push(SimpleLogicalType(ESimpleLogicalValueType::Date));
}
void TTypeBuilder::OnDatetime() {
    Cerr << __func__ << Endl;
    Push(SimpleLogicalType(ESimpleLogicalValueType::Datetime));
}
void TTypeBuilder::OnTimestamp() {
    Cerr << __func__ << Endl;
    Push(SimpleLogicalType(ESimpleLogicalValueType::Timestamp));
}
void TTypeBuilder::OnTzDate() {
    Cerr << __func__ << Endl;
   THROW_ERROR_EXCEPTION("%s not implemented.", __func__);
}
void TTypeBuilder::OnTzDatetime() {
    Cerr << __func__ << Endl;
   THROW_ERROR_EXCEPTION("%s not implemented.", __func__);
}
void TTypeBuilder::OnTzTimestamp() {
    Cerr << __func__ << Endl;
   THROW_ERROR_EXCEPTION("%s not implemented.", __func__);
}
void TTypeBuilder::OnInterval() {
    Cerr << __func__ << Endl;
    Push(SimpleLogicalType(ESimpleLogicalValueType::Interval));
}
void TTypeBuilder::OnDate32() {
    Cerr << __func__ << Endl;
    Push(SimpleLogicalType(ESimpleLogicalValueType::Date32));
}
void TTypeBuilder::OnDatetime64() {
    Cerr << __func__ << Endl;
    Push(SimpleLogicalType(ESimpleLogicalValueType::Datetime64));
}
void TTypeBuilder::OnTimestamp64() {
    Cerr << __func__ << Endl;
    Push(SimpleLogicalType(ESimpleLogicalValueType::Timestamp64));
}
void TTypeBuilder::OnTzDate32() {
    Cerr << __func__ << Endl;
   THROW_ERROR_EXCEPTION("%s not implemented.", __func__);
}
void TTypeBuilder::OnTzDatetime64() {
    Cerr << __func__ << Endl;
   THROW_ERROR_EXCEPTION("%s not implemented.", __func__);
}
void TTypeBuilder::OnTzTimestamp64() {
    Cerr << __func__ << Endl;
   THROW_ERROR_EXCEPTION("%s not implemented.", __func__);
}
void TTypeBuilder::OnInterval64() {
    Cerr << __func__ << Endl;
    Push(SimpleLogicalType(ESimpleLogicalValueType::Interval64));
}
void TTypeBuilder::OnDecimal(ui32 precision, ui32 scale) {
    Cerr << __func__ << '(' << precision << ',' << scale << ')' << Endl;
    Push(DecimalLogicalType(precision, scale));
}
void TTypeBuilder::OnBeginOptional() {
    Cerr << __func__ << Endl;
    Stack.push(EKind::Optional);
}
void TTypeBuilder::OnEndOptional() {
    Cerr << __func__ << Endl;
    Push(OptionalLogicalType(Pop()));
}
void TTypeBuilder::OnBeginList() {
    Cerr << __func__ << Endl;
    Stack.push(EKind::List);
}
void TTypeBuilder::OnEndList() {
    Cerr << __func__ << Endl;
    Push(ListLogicalType(Pop()));
}
void TTypeBuilder::OnBeginTuple() {
    Cerr << __func__ << Endl;
    Stack.push(EKind::Tuple);
    ItemsStack.push(TElements());
}
void TTypeBuilder::OnTupleItem() {
    Cerr << __func__ << Endl;
}
void TTypeBuilder::OnEndTuple() {
    Cerr << __func__ << Endl;
    Push(TupleLogicalType(Pop<TElements>()));
}
void TTypeBuilder::OnBeginStruct() {
    Cerr << __func__ << Endl;
    Stack.push(EKind::Struct);
    ItemsStack.push(TMembers());
}
void TTypeBuilder::OnStructItem(TStringBuf member) {
    Cerr << __func__ << '(' << member << ')' << Endl;
    Name = member;
}
void TTypeBuilder::OnEndStruct() {
    Cerr << __func__ << Endl;
    Push(StructLogicalType(Pop<TMembers>()));
}
void TTypeBuilder::OnBeginDict() {
    Cerr << __func__ << Endl;
    Stack.push(EKind::Dict);
    ItemsStack.push(TKeyAndPayload());
}
void TTypeBuilder::OnDictKey() {
    Cerr << __func__ << Endl;
    std::get<TKeyAndPayload>(ItemsStack.top()).Switch = true;
}
void TTypeBuilder::OnDictPayload() {
    Cerr << __func__ << Endl;
    std::get<TKeyAndPayload>(ItemsStack.top()).Switch = false;
}
void TTypeBuilder::OnEndDict() {
    Cerr << __func__ << Endl;
    auto items = Pop<TKeyAndPayload>();
    Push(DictLogicalType(std::move(items.Key), std::move(items.Payload)));
}
void TTypeBuilder::OnBeginVariant() {
    Cerr << __func__ << Endl;
    Stack.push(EKind::Variant);
}
void TTypeBuilder::OnEndVariant() {
    Cerr << __func__ << Endl;
    Stack.pop();
}
void TTypeBuilder::OnBeginTagged(TStringBuf tag) {
    Cerr << __func__ << '(' << tag << ')' << Endl;
    Stack.push(EKind::Tagged);
    ItemsStack.push(TTag(tag));
}
void TTypeBuilder::OnEndTagged() {
    Cerr << __func__ << Endl;
    Push(TaggedLogicalType(Pop<TTag>(), std::move(Last)));
}
void TTypeBuilder::OnPg(TStringBuf name, TStringBuf category) {
    Cerr << __func__ << '(' << name << ',' << category << ')' << Endl;
    THROW_ERROR_EXCEPTION("%s not implemented.", __func__);
}

void TTypeBuilder::Push(TLogicalTypePtr type) {
    if (Stack.empty()) {
        Last = std::move(type);
        return;
    }

    switch (Stack.top()) {
        case EKind::Tuple:
            std::get<TElements>(ItemsStack.top()).emplace_back(std::move(type));
            return;
        case EKind::Struct:
            std::get<TMembers>(ItemsStack.top()).emplace_back(std::move(Name), std::move(type));
            break;
        case EKind::Dict:
            std::get<TKeyAndPayload>(ItemsStack.top()).Set(std::move(type));
            break;
        case EKind::Variant:
            break; // TODO
        default:
            Last = std::move(type);
            return;
    }
}

}
