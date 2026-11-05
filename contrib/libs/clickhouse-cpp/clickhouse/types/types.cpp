#include "types.h"

#include "../exceptions.h"

#include <city.h>

#include <stdexcept>

namespace clickhouse {

Type::Type(const Code code)
    : code_(code)
    , type_unique_id_(0)
{}

const char* Type::TypeName(Type::Code code) {
    switch (code) {
        case Type::Code::Void:           return "Void";
        case Type::Code::Int8:           return "Int8";
        case Type::Code::Int16:          return "Int16";
        case Type::Code::Int32:          return "Int32";
        case Type::Code::Int64:          return "Int64";
        case Type::Code::UInt8:          return "UInt8";
        case Type::Code::UInt16:         return "UInt16";
        case Type::Code::UInt32:         return "UInt32";
        case Type::Code::UInt64:         return "UInt64";
        case Type::Code::Float32:        return "Float32";
        case Type::Code::Float64:        return "Float64";
        case Type::Code::String:         return "String";
        case Type::Code::FixedString:    return "FixedString";
        case Type::Code::DateTime:       return "DateTime";
        case Type::Code::Date:           return "Date";
        case Type::Code::Array:          return "Array";
        case Type::Code::Nullable:       return "Nullable";
        case Type::Code::Tuple:          return "Tuple";
        case Type::Code::Enum8:          return "Enum8";
        case Type::Code::Enum16:         return "Enum16";
        case Type::Code::UUID:           return "UUID";
        case Type::Code::IPv4:           return "IPv4";
        case Type::Code::IPv6:           return "IPv6";
        case Type::Code::Int128:         return "Int128";
        case Type::Code::UInt128:        return "UInt128";
        case Type::Code::Decimal:        return "Decimal";
        case Type::Code::Decimal32:      return "Decimal32";
        case Type::Code::Decimal64:      return "Decimal64";
        case Type::Code::Decimal128:     return "Decimal128";
        case Type::Code::LowCardinality: return "LowCardinality";
        case Type::Code::DateTime64:     return "DateTime64";
        case Type::Code::Date32:         return "Date32";
        case Type::Code::Map:            return "Map";
        case Type::Code::Point:          return "Point";
        case Type::Code::Ring:           return "Ring";
        case Type::Code::Polygon:        return "Polygon";
        case Type::Code::MultiPolygon:   return "MultiPolygon";
        case Type::Code::Time:           return "Time";
        case Type::Code::Time64:         return "Time64";
        case Type::Code::JSON:           return "JSON";
        case Type::Code::Bool:           return "Bool";
    }

    return "Unknown type";
}

std::string Type::GetName() const {
    switch (code_) {
        case Void:
        case Int8:
        case Int16:
        case Int32:
        case Int64:
        case Int128:
        case UInt8:
        case UInt16:
        case UInt32:
        case UInt64:
        case UInt128:
        case UUID:
        case Float32:
        case Float64:
        case String:
        case IPv4:
        case IPv6:
        case Date:
        case Date32:
        case Time:
        case Point:
        case Ring:
        case Polygon:
        case MultiPolygon:
        case JSON:
        case Bool:
            return TypeName(code_);
        case Time64:
            return As<Time64Type>()->GetName();
        case FixedString:
            return As<FixedStringType>()->GetName();
        case DateTime:
            return As<DateTimeType>()->GetName();
        case DateTime64:
            return As<DateTime64Type>()->GetName();
        case Array:
            return As<ArrayType>()->GetName();
        case Nullable:
            return As<NullableType>()->GetName();
        case Tuple:
            return As<TupleType>()->GetName();
        case Enum8:
        case Enum16:
            return As<EnumType>()->GetName();
        case Decimal:
        case Decimal32:
        case Decimal64:
        case Decimal128:
            return As<DecimalType>()->GetName();
        case LowCardinality:
            return As<LowCardinalityType>()->GetName();
        case Map:
            return As<MapType>()->GetName();
    }

    // XXX: NOT REACHED!
    return std::string();
}

uint64_t Type::GetTypeUniqueId() const {
    // Helper method to optimize equality checks of types with Type::IsEqual(),
    // base invariant: types with same names produce same unique id (and hence considered equal).
    // As an optimization, full type name is constructed at most once, and only for complex types.
    switch (code_) {
        case Void:
        case Int8:
        case Int16:
        case Int32:
        case Int64:
        case Int128:
        case UInt8:
        case UInt16:
        case UInt32:
        case UInt64:
        case UInt128:
        case UUID:
        case Float32:
        case Float64:
        case String:
        case JSON:
        case IPv4:
        case IPv6:
        case Date:
        case Date32:
        case Point:
        case Ring:
        case Polygon:
        case MultiPolygon:
        case Bool:
            // For simple types, unique ID is the same as Type::Code
            return code_;

        case FixedString:
        case Time:
        case Time64:
        case DateTime:
        case DateTime64:
        case Array:
        case Nullable:
        case Tuple:
        case Enum8:
        case Enum16:
        case Decimal:
        case Decimal32:
        case Decimal64:
        case Decimal128:
        case LowCardinality:
        case Map: {
            // For complex types, exact unique ID depends on nested types and/or parameters,
            // the easiest way is to lazy-compute unique ID from name once.
            // Here we do not care if multiple threads are computing value simultaneosly since it is both:
            //   1. going to be the same
            //   2. going to be stored atomically

            if (type_unique_id_.load(std::memory_order_relaxed) == 0) {
                const auto name = GetName();
                type_unique_id_.store(CityHash_v1_0_2::CityHash64WithSeed(name.c_str(), name.size(), code_), std::memory_order_relaxed);
            }

            return type_unique_id_;
        }
    }
    assert(false);
    return 0;
}

TypeRef Type::CreateArray(TypeRef item_type) {
    return TypeRef(new ArrayType(item_type));
}

TypeRef Type::CreateDate() {
    return TypeRef(new Type(Type::Date));
}

TypeRef Type::CreateDate32() {
    return TypeRef(new Type(Type::Date32));
}

TypeRef Type::CreateTime() {
    return TypeRef(new Type(Type::Time));
}

TypeRef Type::CreateTime64(size_t precision) {
    return TypeRef(new Time64Type(precision));
}

TypeRef Type::CreateDateTime(std::string timezone) {
    return TypeRef(new DateTimeType(std::move(timezone)));
}

TypeRef Type::CreateDateTime64(size_t precision, std::string timezone) {
    return TypeRef(new DateTime64Type(precision, std::move(timezone)));
}

TypeRef Type::CreateDecimal(size_t precision, size_t scale) {
    return TypeRef(new DecimalType(precision, scale));
}

TypeRef Type::CreateIPv4() {
    return TypeRef(new Type(Type::IPv4));
}

TypeRef Type::CreateIPv6() {
    return TypeRef(new Type(Type::IPv6));
}

TypeRef Type::CreateNothing() {
    return TypeRef(new Type(Type::Void));
}

TypeRef Type::CreateNullable(TypeRef nested_type) {
    return TypeRef(new NullableType(nested_type));
}

TypeRef Type::CreateString() {
    return TypeRef(new Type(Type::String));
}

TypeRef Type::CreateString(size_t n) {
    return TypeRef(new FixedStringType(n));
}

TypeRef Type::CreateTuple(const std::vector<TypeRef>& item_types,
                          std::vector<std::string> item_names) {
    return TypeRef(new TupleType(item_types, std::move(item_names)));
}

TypeRef Type::CreateEnum8(const std::vector<EnumItem>& enum_items) {
    return TypeRef(new EnumType(Type::Enum8, enum_items));
}

TypeRef Type::CreateEnum16(const std::vector<EnumItem>& enum_items) {
    return TypeRef(new EnumType(Type::Enum16, enum_items));
}

TypeRef Type::CreateUUID() {
    return TypeRef(new Type(Type::UUID));
}

TypeRef Type::CreateLowCardinality(TypeRef item_type) {
    return std::make_shared<LowCardinalityType>(item_type);
}

TypeRef Type::CreateMap(TypeRef key_type, TypeRef value_type) {
    return std::make_shared<MapType>(key_type, value_type);
}

TypeRef Type::CreatePoint() {
    return TypeRef(new Type(Type::Point));
}

TypeRef Type::CreateRing() {
    return TypeRef(new Type(Type::Ring));
}

TypeRef Type::CreatePolygon() {
    return TypeRef(new Type(Type::Polygon));
}

TypeRef Type::CreateMultiPolygon() {
    return TypeRef(new Type(Type::MultiPolygon));
}

TypeRef Type::CreateJSON() {
    return TypeRef(new Type(Type::JSON));
}

/// class ArrayType

ArrayType::ArrayType(TypeRef item_type) : Type(Array), item_type_(item_type) {
}

/// class DecimalType

DecimalType::DecimalType(size_t precision, size_t scale)
    : Type(Decimal),
      precision_(precision),
      scale_(scale) {
    // TODO: assert(precision <= 38 && precision > 0);
}

std::string DecimalType::GetName() const {
    switch (GetCode()) {
        case Decimal:
            return "Decimal(" + std::to_string(precision_) + "," + std::to_string(scale_) + ")";
        case Decimal32:
            return "Decimal32(" + std::to_string(scale_) + ")";
        case Decimal64:
            return "Decimal64(" + std::to_string(scale_) + ")";
        case Decimal128:
            return "Decimal128(" + std::to_string(scale_) + ")";
        default:
            /// XXX: NOT REACHED!
            return "";
    }
}

/// class EnumType

EnumType::EnumType(Type::Code type, const std::vector<EnumItem>& items) : Type(type) {
    for (const auto& item : items) {
        auto result = name_to_value_.insert(item);
        value_to_name_[item.second] = result.first->first;
    }
}

std::string EnumType::GetName() const {
    std::string result;

    if (GetCode() == Enum8) {
        result = "Enum8(";
    } else {
        result = "Enum16(";
    }

    for (auto ei = value_to_name_.begin(); ei != value_to_name_.end();) {
        result += "'";
        result += ei->second;
        result += "' = ";
        result += std::to_string(ei->first);

        if (++ei != value_to_name_.end()) {
            result += ", ";
        } else {
            break;
        }
    }

    result += ")";

    return result;
}

std::string_view EnumType::GetEnumName(int16_t value) const {
    return value_to_name_.at(value);
}

int16_t EnumType::GetEnumValue(const std::string& name) const {
    return name_to_value_.at(name);
}

bool EnumType::HasEnumName(const std::string& name) const {
    return name_to_value_.find(name) != name_to_value_.end();
}

bool EnumType::HasEnumValue(int16_t value) const {
    return value_to_name_.find(value) != value_to_name_.end();
}

EnumType::ValueToNameIterator EnumType::BeginValueToName() const {
    return value_to_name_.begin();
}

EnumType::ValueToNameIterator EnumType::EndValueToName() const {
    return value_to_name_.end();
}


namespace details
{
TypeWithTimeZoneMixin::TypeWithTimeZoneMixin(std::string timezone)
    : timezone_(std::move(timezone)) {
}

const std::string & TypeWithTimeZoneMixin::Timezone() const {
    return timezone_;
}
}

/// class Time64
Time64Type::Time64Type(size_t precision)
    : Type(Time64), precision_{precision} {
    if (precision_ > 9) {
        throw ValidationError("Time64 precision is > 9");
    }
}

std::string Time64Type::GetName() const {
    return "Time64(" + std::to_string(precision_) + ")";
}

/// class DateTimeType
DateTimeType::DateTimeType(std::string timezone)
    : Type(DateTime), details::TypeWithTimeZoneMixin(std::move(timezone)) {
}

std::string DateTimeType::GetName() const {
    std::string datetime_representation = "DateTime";
    const auto & timezone = Timezone();
    if (!timezone.empty())
        datetime_representation += "('" + timezone + "')";

    return datetime_representation;
}

/// class DateTime64Type

DateTime64Type::DateTime64Type(size_t precision, std::string timezone)
    : Type(DateTime64), details::TypeWithTimeZoneMixin(std::move(timezone)), precision_(precision) {

    if (precision_ > 18) {
        throw ValidationError("DateTime64 precision is > 18");
    }
}

std::string DateTime64Type::GetName() const {
    std::string datetime64_representation;
    datetime64_representation.reserve(14);
    datetime64_representation += "DateTime64(";
    datetime64_representation += std::to_string(precision_);

    const auto & timezone = Timezone();
    if (!timezone.empty()) {
        datetime64_representation += ", '" + timezone + "'";
    }

    datetime64_representation += ")";
    return datetime64_representation;
}

/// class FixedStringType

FixedStringType::FixedStringType(size_t n) : Type(FixedString), size_(n) {
}

/// class NullableType

NullableType::NullableType(TypeRef nested_type) : Type(Nullable), nested_type_(nested_type) {
}

TupleType::TupleType(const std::vector<TypeRef>& item_types,
                     std::vector<std::string> item_names)
    : Type(Tuple), item_types_(item_types), item_names_(std::move(item_names)) {
    if (!item_names_.empty() && item_names_.size() != item_types_.size()) {
        throw ValidationError("Tuple field names count doesn't match tuple element count");
    }
    for (const auto& item_name : item_names_) {
        if (item_name.empty()) {
            throw ValidationError("Tuple field names can't be empty");
        }
    }
}

/// class LowCardinalityType
LowCardinalityType::LowCardinalityType(TypeRef nested_type) : Type(LowCardinality), nested_type_(nested_type) {
}

LowCardinalityType::~LowCardinalityType() {
}

// Checks if `name` is a valid plain identifier (must not be quoted).
// The condition for this is a match against `^[a-zA-Z_][0-9a-zA-Z_]*$`
static bool IsPlainIdentifier(const std::string& name) {
    if (name.empty()) return false;
    auto is_alpha_or_under = [](char c) { return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_'; };
    auto is_alnum_or_under = [&is_alpha_or_under](char c) { return is_alpha_or_under(c) || (c >= '0' && c <= '9'); };
    if (!is_alpha_or_under(name[0])) return false;
    for (size_t i = 1; i < name.size(); ++i)
        if (!is_alnum_or_under(name[i])) return false;
    return true;
}

// Appends a fieldname, potentially quoting it and escaping backticks.
static void AppendFieldname(const std::string& name, std::string& out) {
    if (IsPlainIdentifier(name)) {
        out += name;
        return;
    }
    out += '`';
    for (char c : name) {
        if (c == '`')
            out += "``";
        else
            out += c;
    }
    out += '`';
}

std::string TupleType::GetName() const {
    std::string result("Tuple(");
    bool has_complete_names = !item_names_.empty();

    for (size_t i = 0; i < item_types_.size(); ++i) {
        if (i > 0)
            result += ", ";
        if (has_complete_names) {
            AppendFieldname(item_names_[i], result);
            result += ' ';
        }
        result += item_types_[i]->GetName();
    }

    result += ")";

    return result;
}

/// class MapType
MapType::MapType(TypeRef key_type, TypeRef value_type)
    : Type(Map)
    , key_type_(key_type)
    , value_type_(value_type) {
}

std::string MapType::GetName() const {
    return std::string("Map(") + key_type_->GetName() + ", " +value_type_->GetName() + ")";
}

}  // namespace clickhouse
