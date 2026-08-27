#pragma once

#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/mkql_alloc.h>
#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/minikql/mkql_mem_info.h>
#include <yql/essentials/minikql/mkql_type_builder.h>
#include <yql/essentials/public/udf/udf_type_builder.h>
#include <yql/essentials/public/udf/udf_value.h>
#include <yql/essentials/public/udf/udf_value_builder.h>


namespace NYql::NYtflow::NCodec::NTest {

struct TUnboxedValueSetup {
public:
    NKikimr::NMiniKQL::TScopedAlloc Alloc;
    NKikimr::NMiniKQL::TTypeEnvironment TypeEnv;
    NKikimr::NMiniKQL::TTypeBuilder TypeBuilder;
    NKikimr::NMiniKQL::TMemoryUsageInfo MemUsage;
    NKikimr::NMiniKQL::THolderFactory HolderFactory;
    const NKikimr::NMiniKQL::TStructType* Type;

    THolder<NYql::NUdf::IValueBuilder> ValueBuilder;
    NYql::NUdf::ITypeInfoHelper::TPtr TypeInfoHelper;
    NYql::TRuntimeSettings::TConstPtr RuntimeSettings;
    NYql::NUdf::IFunctionTypeInfoBuilderPtr FunctionTypeInfoBuilder;

public:
    TUnboxedValueSetup();

    // static interface
    const NKikimr::NMiniKQL::TType* BuildType() = delete;
    NYql::NUdf::TUnboxedValue BuildUnboxedValue() = delete;

    void AssertExpectedUnboxedValue(
        const NYql::NUdf::TUnboxedValue& unboxedValue) const = delete;

protected:
    void SetUnboxedValue(
        NYql::NUdf::TUnboxedValue* items,
        TStringBuf name,
        NYql::NUdf::TUnboxedValue&& unboxedValue);

    template <typename TValue>
    void SetSimpleValue(
        NYql::NUdf::TUnboxedValue* items,
        TStringBuf name,
        TValue&& value);

    template <typename TValue>
    void SetStringValue(
        NYql::NUdf::TUnboxedValue* items,
        TStringBuf name,
        TValue&& value);

    NYql::NUdf::TUnboxedValue GetMember(
        const NYql::NUdf::TUnboxedValue& unboxedValue,
        const NKikimr::NMiniKQL::TStructType* structType,
        TStringBuf name) const;

    void AssertStringUnboxedValue(
        const NYql::NUdf::TUnboxedValue& unboxedValue,
        TStringBuf value) const;

    void AssertStringValue(
        const NYql::NUdf::TUnboxedValue& unboxedValue,
        TStringBuf name,
        TStringBuf value) const;

    template <typename TValue>
    void AssertSimpleUnboxedValue(
        const NYql::NUdf::TUnboxedValue& unboxedValue,
        TValue&& value) const;

    template <typename TValue>
    void AssertSimpleValue(
        const NYql::NUdf::TUnboxedValue& unboxedValue,
        TStringBuf name,
        TValue&& value) const;
};

struct TUnboxedValueSetupFull: public TUnboxedValueSetup {
public:
    TUnboxedValueSetupFull();

public:
    const NKikimr::NMiniKQL::TType* BuildType();
    NYql::NUdf::TUnboxedValue BuildUnboxedValue();

    void AssertExpectedUnboxedValue(
        const NYql::NUdf::TUnboxedValue& unboxedValue) const;
};

struct TUnboxedValueSetupLarge: public TUnboxedValueSetup {
public:
    TUnboxedValueSetupLarge();

public:
    const NKikimr::NMiniKQL::TType* BuildType();
    NYql::NUdf::TUnboxedValue BuildUnboxedValue();

    void AssertExpectedUnboxedValue(
        const NYql::NUdf::TUnboxedValue& unboxedValue) const;
};

struct TUnboxedValueSetupLargeOptional: public TUnboxedValueSetup {
public:
    TUnboxedValueSetupLargeOptional();

public:
    const NKikimr::NMiniKQL::TType* BuildType();
    NYql::NUdf::TUnboxedValue BuildUnboxedValue();

    void AssertExpectedUnboxedValue(
        const NYql::NUdf::TUnboxedValue& unboxedValue) const;
};

struct TUnboxedValueSetupSmall: public TUnboxedValueSetup {
public:
    TUnboxedValueSetupSmall();

public:
    const NKikimr::NMiniKQL::TType* BuildType();
    NYql::NUdf::TUnboxedValue BuildUnboxedValue();

    void AssertExpectedUnboxedValue(
        const NYql::NUdf::TUnboxedValue& unboxedValue) const;
};

} // namespace NYql::NYtflow::NCodec::NTest
