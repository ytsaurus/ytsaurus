#pragma once

#include "public.h"

#include <yt/client/table_client/unversioned_row.h>
#include <yt/core/codegen/type_builder.h>

#include <type_traits>

namespace NYT::NCodegen {

using llvm::Type;
using llvm::StructType;
using llvm::LLVMContext;
using llvm::ArrayRef;

////////////////////////////////////////////////////////////////////////////////

template <>
struct TypeBuilder<NYT::NTableClient::TUnversionedValueData>
{
public:
    typedef TypeBuilder<char> TBoolean;
    typedef TypeBuilder<i64> TInt64;
    typedef TypeBuilder<ui64> TUint64;
    typedef TypeBuilder<double> TDouble;
    typedef TypeBuilder<const char*> TStringType;
    typedef TypeBuilder<const char*> TAny;

    static Type* Get(LLVMContext& context)
    {
        return TypeBuilder<i64>::Get(context);
    }

    static Type* Get(LLVMContext& context, NYT::NTableClient::EValueType staticType)
    {
        using NYT::NTableClient::EValueType;
        switch (staticType) {
            case EValueType::Boolean:
                return TBoolean::Get(context);
            case EValueType::Int64:
                return TInt64::Get(context);
            case EValueType::Uint64:
                return TUint64::Get(context);
            case EValueType::Double:
                return TDouble::Get(context);
            case EValueType::String:
                return TStringType::Get(context);
            case EValueType::Any:
                return TAny::Get(context);
            default:
                YT_ABORT();
        }
    }

    static_assert(
        std::is_union<NYT::NTableClient::TUnversionedValueData>::value,
        "TUnversionedValueData must be a union");
    static_assert(
        sizeof(NYT::NTableClient::TUnversionedValueData) == 8,
        "TUnversionedValueData size must be 64bit");
};

template <>
struct TypeBuilder<NYT::NTableClient::TUnversionedValue>
{
public:
    typedef TypeBuilder<ui16> TId;
    typedef TypeBuilder<ui8> TType;
    typedef TypeBuilder<ui8> TAggregate;
    typedef TypeBuilder<ui32> TLength;
    typedef TypeBuilder<NYT::NTableClient::TUnversionedValueData> TData;

    enum Fields
    {
        Id,
        Type,
        Aggregate,
        Length,
        Data
    };

    static StructType* Get(LLVMContext& context)
    {
        return StructType::get(context, {
            TId::Get(context),
            TType::Get(context),
            TAggregate::Get(context),
            TLength::Get(context),
            TData::Get(context)});
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

} // namespace NYT::NCodegen

