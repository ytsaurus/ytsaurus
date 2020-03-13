#pragma once

#include "public.h"

#include <yt/client/table_client/unversioned_row.h>
#include <yt/library/codegen/type_builder.h>

#include <type_traits>

namespace NYT::NCodegen {

using llvm::Type;
using llvm::StructType;
using llvm::LLVMContext;
using llvm::ArrayRef;

////////////////////////////////////////////////////////////////////////////////

template <>
struct TTypeBuilder<NYT::NTableClient::TUnversionedValueData>
{
public:
    typedef TTypeBuilder<char> TBoolean;
    typedef TTypeBuilder<i64> TInt64;
    typedef TTypeBuilder<ui64> TUint64;
    typedef TTypeBuilder<double> TDouble;
    typedef TTypeBuilder<const char*> TStringType;
    typedef TTypeBuilder<const char*> TAny;

    static Type* Get(LLVMContext& context)
    {
        return TTypeBuilder<i64>::Get(context);
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

            case EValueType::Composite:
            case EValueType::Null:

            case EValueType::Min:
            case EValueType::Max:
            case EValueType::TheBottom:
                break;
        }
        YT_ABORT();
    }

    static_assert(
        std::is_union<NYT::NTableClient::TUnversionedValueData>::value,
        "TUnversionedValueData must be a union");
    static_assert(
        sizeof(NYT::NTableClient::TUnversionedValueData) == 8,
        "TUnversionedValueData size must be 64bit");
};

template <>
struct TTypeBuilder<NYT::NTableClient::TUnversionedValue>
{
public:
    typedef TTypeBuilder<ui16> TId;
    typedef TTypeBuilder<ui8> TType;
    typedef TTypeBuilder<ui8> TAggregate;
    typedef TTypeBuilder<ui32> TLength;
    typedef TTypeBuilder<NYT::NTableClient::TUnversionedValueData> TData;

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

