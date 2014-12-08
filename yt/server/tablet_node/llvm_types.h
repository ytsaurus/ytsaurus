#pragma once

#include "dynamic_memory_store_bits.h"

#include <llvm/IR/TypeBuilder.h>

#include <type_traits>

namespace llvm {

////////////////////////////////////////////////////////////////////////////////

template <bool Cross>
class TypeBuilder<NYT::NTabletNode::TDynamicString, Cross>
{
public:
    typedef TypeBuilder<i32, Cross> TLength;
    typedef TypeBuilder<char, Cross> TData;

    enum Fields
    {
        Length,
        Data
    };

    static StructType* get(LLVMContext& context)
    {
        return StructType::get(
            TLength::get(context),
            TData::get(context),
            nullptr);
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

template <bool Cross>
class TypeBuilder<NYT::NTabletNode::TDynamicValueData, Cross>
{
public:
    typedef TypeBuilder<char, Cross> TBoolean;
    typedef TypeBuilder<i64, Cross> TInt64;
    typedef TypeBuilder<ui64, Cross> TUint64;
    typedef TypeBuilder<double, Cross> TDouble;
    typedef TypeBuilder<NYT::NTabletNode::TDynamicString*, Cross> TString;

    enum Fields
    {
        Uint64 = 0,
        Double = 0,
        Boolean = 0,
        String = 0,
        Any = 0,
    };

    static StructType* get(LLVMContext& context)
    {
        return StructType::get(
            TypeBuilder<i64, Cross>::get(context),
             nullptr);
    }

    static_assert(
        std::is_union<NYT::NTabletNode::TDynamicValueData>::value,
        "TDynamicValueData must be a union");
    static_assert(
        sizeof(NYT::NTabletNode::TDynamicValueData) == 8,
        "TDynamicValueData size must be 64bit");
};

////////////////////////////////////////////////////////////////////////////////

} // namespace llvm

