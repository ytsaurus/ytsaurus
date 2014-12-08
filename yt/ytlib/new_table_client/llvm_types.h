#pragma once

#include "unversioned_row.h"

#include <llvm/IR/TypeBuilder.h>

#include <type_traits>

namespace llvm {

////////////////////////////////////////////////////////////////////////////////

template <bool Cross>
class TypeBuilder<NYT::NVersionedTableClient::TUnversionedValueData, Cross>
{
public:
    typedef TypeBuilder<char, Cross> TBoolean;
    typedef TypeBuilder<i64, Cross> TInt64;
    typedef TypeBuilder<ui64, Cross> TUint64;
    typedef TypeBuilder<double, Cross> TDouble;
    typedef TypeBuilder<const char*, Cross> TString;

    static Type* get(LLVMContext& context)
    {
        return TypeBuilder<i64, Cross>::get(context);
    }

    static_assert(
        std::is_union<NYT::NVersionedTableClient::TUnversionedValueData>::value,
        "TUnversionedValueData must be a union");
    static_assert(
        sizeof(NYT::NVersionedTableClient::TUnversionedValueData) == 8,
        "TUnversionedValueData size must be 64bit");
};

template <bool Cross>
class TypeBuilder<NYT::NVersionedTableClient::TUnversionedValue, Cross>
{
public:
    typedef TypeBuilder<ui16, Cross> TId;
    typedef TypeBuilder<ui16, Cross> TType;
    typedef TypeBuilder<ui32, Cross> TLength;
    typedef TypeBuilder<NYT::NVersionedTableClient::TUnversionedValueData, Cross> TData;

    enum Fields
    {
        Id,
        Type,
        Length,
        Data
    };

    static StructType* get(LLVMContext& context)
    {
        return StructType::get(
            TId::get(context),
            TType::get(context),
            TLength::get(context),
            TData::get(context),
            nullptr);
    }

    static_assert(
        std::is_standard_layout<NYT::NVersionedTableClient::TUnversionedValue>::value,
        "TUnversionedValue must be of standart layout type");
    static_assert(
        sizeof(NYT::NVersionedTableClient::TUnversionedValue) == 16,
        "TUnversionedValue size has to be 16 bytes");
    static_assert(
        offsetof(NYT::NVersionedTableClient::TUnversionedValue, Id) == 0
            && sizeof(NYT::NVersionedTableClient::TUnversionedValue::Id) == 2,
        "TUnversionedValue must be of type {i16, i16, i32, i64}");
    static_assert(
        offsetof(NYT::NVersionedTableClient::TUnversionedValue, Type) == 2
            && sizeof(NYT::NVersionedTableClient::TUnversionedValue::Type) == 2,
        "TUnversionedValue must be of type {i16, i16, i32, i64}");
    static_assert(
        offsetof(NYT::NVersionedTableClient::TUnversionedValue, Length) == 4
            && sizeof(NYT::NVersionedTableClient::TUnversionedValue::Length) == 4,
        "TUnversionedValue must be of type {i16, i16, i32, i64}");
    static_assert(
        offsetof(NYT::NVersionedTableClient::TUnversionedValue, Data) == 8
            && sizeof(NYT::NVersionedTableClient::TUnversionedValue::Data) == 8,
        "TUnversionedValue must be of type {i16, i16, i32, i64}");
};

////////////////////////////////////////////////////////////////////////////////

} // namespace llvm

