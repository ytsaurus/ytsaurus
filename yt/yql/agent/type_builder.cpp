#include "type_builder.h"

namespace NYT::NYqlAgent {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

TTypeBuilder::TTypeBuilder()
{
    ItemsStack.emplace(TItemType());
}

TLogicalTypePtr TTypeBuilder::PullResult() {
    return Pop();
}

template<class T>
T TTypeBuilder::Pop()
{
    auto items = std::move(std::get<T>(ItemsStack.top()));
    ItemsStack.pop();
    return items;
}

void TTypeBuilder::OnVoid()
{
    Push(SimpleLogicalType(ESimpleLogicalValueType::Void));
}

void TTypeBuilder::OnNull()
{
    Push(SimpleLogicalType(ESimpleLogicalValueType::Null));
}

void TTypeBuilder::OnEmptyList()
{
    Push(TaggedLogicalType("_EmptyList", NullLogicalType()));
}

void TTypeBuilder::OnEmptyDict()
{
    Push(TaggedLogicalType("_EmptyDict", NullLogicalType()));
}

void TTypeBuilder::OnBool()
{
    Push(SimpleLogicalType(ESimpleLogicalValueType::Boolean));
}

void TTypeBuilder::OnInt8()
{
    Push(SimpleLogicalType(ESimpleLogicalValueType::Int8));
}

void TTypeBuilder::OnUint8()
{
    Push(SimpleLogicalType(ESimpleLogicalValueType::Uint8));
}

void TTypeBuilder::OnInt16()
{
    Push(SimpleLogicalType(ESimpleLogicalValueType::Int16));
}

void TTypeBuilder::OnUint16()
{
    Push(SimpleLogicalType(ESimpleLogicalValueType::Uint16));
}

void TTypeBuilder::OnInt32()
{
    Push(SimpleLogicalType(ESimpleLogicalValueType::Int32));
}

void TTypeBuilder::OnUint32()
{
    Push(SimpleLogicalType(ESimpleLogicalValueType::Uint32));
}

void TTypeBuilder::OnInt64()
{
    Push(SimpleLogicalType(ESimpleLogicalValueType::Int64));
}

void TTypeBuilder::OnUint64()
{
    Push(SimpleLogicalType(ESimpleLogicalValueType::Uint64));
}

void TTypeBuilder::OnFloat()
{
    Push(SimpleLogicalType(ESimpleLogicalValueType::Float));
}

void TTypeBuilder::OnDouble()
{
    Push(SimpleLogicalType(ESimpleLogicalValueType::Double));
}

void TTypeBuilder::OnString()
{
    Push(SimpleLogicalType(ESimpleLogicalValueType::String));
}

void TTypeBuilder::OnUtf8()
{
    Push(SimpleLogicalType(ESimpleLogicalValueType::Utf8));
}

void TTypeBuilder::OnYson()
{
    Push(SimpleLogicalType(ESimpleLogicalValueType::Any));
}

void TTypeBuilder::OnJson()
{
    Push(SimpleLogicalType(ESimpleLogicalValueType::Json));
}

void TTypeBuilder::OnJsonDocument()
{
    Push(SimpleLogicalType(ESimpleLogicalValueType::String));
}

void TTypeBuilder::OnUuid()
{
    Push(SimpleLogicalType(ESimpleLogicalValueType::Uuid));
}

void TTypeBuilder::OnDyNumber()
{
    Push(SimpleLogicalType(ESimpleLogicalValueType::String));
}

void TTypeBuilder::OnDate()
{
    Push(SimpleLogicalType(ESimpleLogicalValueType::Date));
}

void TTypeBuilder::OnDatetime()
{
    Push(SimpleLogicalType(ESimpleLogicalValueType::Datetime));
}

void TTypeBuilder::OnTimestamp()
{
    Push(SimpleLogicalType(ESimpleLogicalValueType::Timestamp));
}

void TTypeBuilder::OnTzDate()
{
    Push(SimpleLogicalType(ESimpleLogicalValueType::String));
}

void TTypeBuilder::OnTzDatetime()
{
    Push(SimpleLogicalType(ESimpleLogicalValueType::String));
}

void TTypeBuilder::OnTzTimestamp()
{
    Push(SimpleLogicalType(ESimpleLogicalValueType::String));
}

void TTypeBuilder::OnInterval()
{
    Push(SimpleLogicalType(ESimpleLogicalValueType::Interval));
}

void TTypeBuilder::OnDate32()
{
    Push(SimpleLogicalType(ESimpleLogicalValueType::Date32));
}

void TTypeBuilder::OnDatetime64()
{
    Push(SimpleLogicalType(ESimpleLogicalValueType::Datetime64));
}

void TTypeBuilder::OnTimestamp64()
{
    Push(SimpleLogicalType(ESimpleLogicalValueType::Timestamp64));
}

void TTypeBuilder::OnTzDate32()
{
    Push(SimpleLogicalType(ESimpleLogicalValueType::String));
}

void TTypeBuilder::OnTzDatetime64()
{
    Push(SimpleLogicalType(ESimpleLogicalValueType::String));
}

void TTypeBuilder::OnTzTimestamp64()
{
    Push(SimpleLogicalType(ESimpleLogicalValueType::String));
}

void TTypeBuilder::OnInterval64()
{
    Push(SimpleLogicalType(ESimpleLogicalValueType::Interval64));
}

void TTypeBuilder::OnDecimal(ui32 precision, ui32 scale)
{
    Push(DecimalLogicalType(precision, scale));
}

void TTypeBuilder::OnBeginOptional()
{
    ItemsStack.emplace(TItemType());
}

void TTypeBuilder::OnEndOptional()
{
    Push(OptionalLogicalType(Pop()));
}
void TTypeBuilder::OnBeginList()
{
    ItemsStack.emplace(TItemType());
}

void TTypeBuilder::OnEndList()
{
    Push(ListLogicalType(Pop()));
}

void TTypeBuilder::OnBeginTuple()
{
    ItemsStack.emplace(TElements());
}

void TTypeBuilder::OnTupleItem()
{
}

void TTypeBuilder::OnEndTuple()
{
    Push(TupleLogicalType(Pop<TElements>()));
}

void TTypeBuilder::OnBeginStruct()
{
    ItemsStack.emplace(TMembers());
}
void TTypeBuilder::OnStructItem(TStringBuf member)
{
    MemberNames.emplace(member);
}

void TTypeBuilder::OnEndStruct()
{
    Push(StructLogicalType(Pop<TMembers>()));
}

void TTypeBuilder::OnBeginDict()
{
    ItemsStack.emplace(TKeyAndPayload());
}

void TTypeBuilder::OnDictKey()
{
    std::get<TKeyAndPayload>(ItemsStack.top()).Switch = true;
}

void TTypeBuilder::OnDictPayload()
{
    std::get<TKeyAndPayload>(ItemsStack.top()).Switch = false;
}

void TTypeBuilder::OnEndDict()
{
    auto items = Pop<TKeyAndPayload>();
    Push(DictLogicalType(std::move(items.Key), std::move(items.Payload)));
}

void TTypeBuilder::OnBeginVariant()
{
    ItemsStack.emplace(TItemType());
}

void TTypeBuilder::OnEndVariant()
{
    const auto internal = Pop();
    switch (internal->GetMetatype()) {
        case ELogicalMetatype::Struct:
            Push(VariantStructLogicalType(internal->GetFields()));
            break;
        case ELogicalMetatype::Tuple:
            Push(VariantTupleLogicalType(internal->GetElements()));
            break;
        default:
            THROW_ERROR_EXCEPTION("Invalid variant type.");
    }
}

void TTypeBuilder::OnBeginTagged(TStringBuf tag)
{
    ItemsStack.emplace(TTagAndType(tag, nullptr));
}

void TTypeBuilder::OnEndTagged()
{
    auto [tag, type] = Pop<TTagAndType>();
    Push(TaggedLogicalType(tag, std::move(type)));
}

void TTypeBuilder::OnPg(TStringBuf name, TStringBuf category)
{
    THROW_ERROR_EXCEPTION("%s not implemented.", __func__);
}

void TTypeBuilder::Push(TLogicalTypePtr type)
{
    if (const auto item = std::get_if<TItemType>(&ItemsStack.top())) {
        *item = std::move(type);
    } else if (const auto elements = std::get_if<TElements>(&ItemsStack.top())) {
        elements->emplace_back(std::move(type));
    } else if (const auto members = std::get_if<TMembers>(&ItemsStack.top())) {
        members->emplace_back(MemberNames.top(), std::move(type));
        MemberNames.pop();
    } else if (const auto items = std::get_if<TKeyAndPayload>(&ItemsStack.top())) {
        items->Set(std::move(type));
    } else if (const auto tagged = std::get_if<TTagAndType>(&ItemsStack.top())) {
        tagged->second = std::move(type);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYqlAgent
