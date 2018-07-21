#pragma once

#include "public.h"

#include <yt/client/table_client/unversioned_row.h>

#include <type_traits>

#include <llvm/IR/TypeBuilder.h>

namespace llvm {

////////////////////////////////////////////////////////////////////////////////

template <bool Cross>
class TypeBuilder<NYT::NTableClient::TUnversionedValueData, Cross>
{
public:
    typedef TypeBuilder<char, Cross> TBoolean;
    typedef TypeBuilder<i64, Cross> TInt64;
    typedef TypeBuilder<ui64, Cross> TUint64;
    typedef TypeBuilder<double, Cross> TDouble;
    typedef TypeBuilder<const char*, Cross> TStringType;
    typedef TypeBuilder<const char*, Cross> TAny;

    static Type* get(LLVMContext& context)
    {
        return TypeBuilder<i64, Cross>::get(context);
    }

    static Type* get(LLVMContext& context, NYT::NTableClient::EValueType staticType)
    {
        using NYT::NTableClient::EValueType;
        switch (staticType) {
            case EValueType::Boolean:
                return TBoolean::get(context);
            case EValueType::Int64:
                return TInt64::get(context);
            case EValueType::Uint64:
                return TUint64::get(context);
            case EValueType::Double:
                return TDouble::get(context);
            case EValueType::String:
                return TStringType::get(context);
            case EValueType::Any:
                return TAny::get(context);
            default:
                Y_UNREACHABLE();
        }
    }

    static_assert(
        std::is_union<NYT::NTableClient::TUnversionedValueData>::value,
        "TUnversionedValueData must be a union");
    static_assert(
        sizeof(NYT::NTableClient::TUnversionedValueData) == 8,
        "TUnversionedValueData size must be 64bit");
};

template <bool Cross>
class TypeBuilder<NYT::NTableClient::TUnversionedValue, Cross>
{
public:
    typedef TypeBuilder<ui16, Cross> TId;
    typedef TypeBuilder<ui8, Cross> TType;
    typedef TypeBuilder<ui8, Cross> TAggregate;
    typedef TypeBuilder<ui32, Cross> TLength;
    typedef TypeBuilder<NYT::NTableClient::TUnversionedValueData, Cross> TData;

    enum Fields
    {
        Id,
        Type,
        Aggregate,
        Length,
        Data
    };

    static StructType* get(LLVMContext& context)
    {
        return StructType::get(context, {
            TId::get(context),
            TType::get(context),
            TAggregate::get(context),
            TLength::get(context),
            TData::get(context)});
    }

    static_assert(
        std::is_standard_layout<NYT::NTableClient::TUnversionedValue>::value,
        "TUnversionedValue must be of standart layout type");
    static_assert(
        sizeof(NYT::NTableClient::TUnversionedValue) == 16,
        "TUnversionedValue size has to be 16 bytes");
    static_assert(
        offsetof(NYT::NTableClient::TUnversionedValue, Id) == 0
            && sizeof(NYT::NTableClient::TUnversionedValue::Id) == 2,
        "TUnversionedValue must be of type {i16, i8, i8, i32, i64}");
    static_assert(
        offsetof(NYT::NTableClient::TUnversionedValue, Type) == 2
            && sizeof(NYT::NTableClient::TUnversionedValue::Type) == 1,
        "TUnversionedValue must be of type {i16, i8, i8, i32, i64}");
    static_assert(
        offsetof(NYT::NTableClient::TUnversionedValue, Aggregate) == 3
            && sizeof(NYT::NTableClient::TUnversionedValue::Aggregate) == 1,
        "TUnversionedValue must be of type {i16, i8, i8, i32, i64}");
    static_assert(
        offsetof(NYT::NTableClient::TUnversionedValue, Length) == 4
            && sizeof(NYT::NTableClient::TUnversionedValue::Length) == 4,
        "TUnversionedValue must be of type {i16, i8, i8, i32, i64}");
    static_assert(
        offsetof(NYT::NTableClient::TUnversionedValue, Data) == 8
            && sizeof(NYT::NTableClient::TUnversionedValue::Data) == 8,
        "TUnversionedValue must be of type {i16, i8, i8, i32, i64}");
};

////////////////////////////////////////////////////////////////////////////////

} // namespace llvm

