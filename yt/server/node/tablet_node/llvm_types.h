#pragma once

#include "dynamic_store_bits.h"

#include <yt/core/codegen/type_builder.h>

#include <type_traits>

namespace NYT::NCodegen {

////////////////////////////////////////////////////////////////////////////////

template <>
struct TypeBuilder<NYT::NTabletNode::TDynamicString>
{
public:
    typedef TypeBuilder<i32> TLength;
    typedef TypeBuilder<char> TData;

    enum Fields
    {
        Length,
        Data
    };

    static llvm::StructType* get(llvm::LLVMContext& context)
    {
        return llvm::StructType::get(context, {
            TLength::get(context),
            TData::get(context)});
    }

    static_assert(
        std::is_standard_layout<NYT::NTabletNode::TDynamicString>::value,
        "TDynamicString must be of standart layout type");
    static_assert(
        sizeof(NYT::NTabletNode::TDynamicString) <= 8,
        "TDynamicString must be of type {i32, i8}");
    static_assert(
        offsetof(NYT::NTabletNode::TDynamicString, Length) == 0
            && sizeof(NYT::NTabletNode::TDynamicString::Length) == 4,
        "TDynamicString must be of type {i32, i8}");
    static_assert(
        offsetof(NYT::NTabletNode::TDynamicString, Data) == 4
            && sizeof(NYT::NTabletNode::TDynamicString::Data) == 1,
        "TDynamicString must be of type {i32, i8}");
};

template <>
struct TypeBuilder<NYT::NTabletNode::TDynamicValueData>
{
public:
    typedef TypeBuilder<char> TBoolean;
    typedef TypeBuilder<i64> TInt64;
    typedef TypeBuilder<ui64> TUint64;
    typedef TypeBuilder<double> TDouble;
    typedef TypeBuilder<NYT::NTabletNode::TDynamicString*> TStringType;

    enum Fields
    {
        Uint64 = 0,
        Double = 0,
        Boolean = 0,
        String = 0,
        Any = 0,
    };

    static llvm::StructType* get(llvm::LLVMContext& context)
    {
        return llvm::StructType::get(context, llvm::ArrayRef<llvm::Type*>{
            TypeBuilder<i64>::get(context)});
    }

    static_assert(
        std::is_union<NYT::NTabletNode::TDynamicValueData>::value,
        "TDynamicValueData must be a union");
    static_assert(
        sizeof(NYT::NTabletNode::TDynamicValueData) == 8,
        "TDynamicValueData size must be 64bit");
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCodegen

