#pragma once

#include <yt/yt/client/tablet_client/dynamic_value.h>

#include <yt/yt/library/codegen/type_builder.h>

#include <type_traits>

namespace NYT::NCodegen {

////////////////////////////////////////////////////////////////////////////////

template <>
struct TTypeBuilder<NYT::NTabletClient::TDynamicString>
{
public:
    using TLength = TTypeBuilder<i32>;
    using TData = TTypeBuilder<char>;

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
        std::is_standard_layout<NYT::NTabletClient::TDynamicString>::value,
        "TDynamicString must be of standard layout type");
    static_assert(
        sizeof(NYT::NTabletClient::TDynamicString) <= 8,
        "TDynamicString must be of type {i32, i8}");
    static_assert(
        offsetof(NYT::NTabletClient::TDynamicString, Length) == 0
            && sizeof(NYT::NTabletClient::TDynamicString::Length) == 4,
        "TDynamicString must be of type {i32, i8}");
    static_assert(
        offsetof(NYT::NTabletClient::TDynamicString, Data) == 4
            && sizeof(NYT::NTabletClient::TDynamicString::Data) == 1,
        "TDynamicString must be of type {i32, i8}");
};

template <>
struct TTypeBuilder<NYT::NTabletClient::TDynamicValueData>
{
public:
    using TBoolean = TTypeBuilder<char>;
    using TInt64 = TTypeBuilder<i64>;
    using TUint64 = TTypeBuilder<ui64>;
    using TDouble = TTypeBuilder<double>;
    using TStringType = TTypeBuilder<NYT::NTabletClient::TDynamicString*>;

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
        std::is_union<NYT::NTabletClient::TDynamicValueData>::value,
        "TDynamicValueData must be a union");
    static_assert(
        sizeof(NYT::NTabletClient::TDynamicValueData) == 8,
        "TDynamicValueData size must be 64bit");
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCodegen
