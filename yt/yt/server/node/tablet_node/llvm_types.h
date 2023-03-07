#pragma once

#include "dynamic_store_bits.h"

#include <yt/library/codegen/type_builder.h>

#include <type_traits>

namespace NYT::NCodegen {

////////////////////////////////////////////////////////////////////////////////

template <>
struct TTypeBuilder<NYT::NTabletNode::TDynamicString>
{
public:
    typedef TTypeBuilder<i32> TLength;
    typedef TTypeBuilder<char> TData;

    enum Fields
    {
        Length,
        Data
    };

    static llvm::StructType* Get(llvm::LLVMContext& context)
    {
        return llvm::StructType::get(context, {
            TLength::Get(context),
            TData::Get(context)});
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
struct TTypeBuilder<NYT::NTabletNode::TDynamicValueData>
{
public:
    typedef TTypeBuilder<char> TBoolean;
    typedef TTypeBuilder<i64> TInt64;
    typedef TTypeBuilder<ui64> TUint64;
    typedef TTypeBuilder<double> TDouble;
    typedef TTypeBuilder<NYT::NTabletNode::TDynamicString*> TStringType;

    enum Fields
    {
        Uint64 = 0,
        Double = 0,
        Boolean = 0,
        String = 0,
        Any = 0,
    };

    static llvm::StructType* Get(llvm::LLVMContext& context)
    {
        return llvm::StructType::get(context, llvm::ArrayRef<llvm::Type*>{
            TTypeBuilder<i64>::Get(context)});
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

