#include "yql_ytflow_unboxed_value_setup.h"
#include "yql_ytflow_unversioned_row_setup.h"

#include <library/cpp/testing/unittest/registar.h>

#include <yt/yql/providers/ytflow/codec/yql_ytflow_input_codec.h>
#include <yt/yql/providers/ytflow/codec/yql_ytflow_output_codec.h>


Y_UNIT_TEST_SUITE(TYtflowCodec) {
#define TEST_UNBOXED_VALUE_SETUP(suffix) \
    Y_UNIT_TEST(UnboxedValueSetup##suffix) { \
        NYql::NYtflow::NCodec::NTest::TUnboxedValueSetup##suffix unboxedValueSetup; \
\
        auto unboxedValue = unboxedValueSetup.BuildUnboxedValue(); \
        unboxedValueSetup.AssertExpectedUnboxedValue(unboxedValue); \
    }

    TEST_UNBOXED_VALUE_SETUP(Full)
    TEST_UNBOXED_VALUE_SETUP(Large)
    TEST_UNBOXED_VALUE_SETUP(LargeOptional)
    TEST_UNBOXED_VALUE_SETUP(Small)

#undef TEST_UNBOXED_VALUE_SETUP

#define TEST_UNVERSIONED_ROW_SETUP(suffix) \
    Y_UNIT_TEST(UnversionedRowSetup##suffix) { \
        NYql::NYtflow::NCodec::NTest::TUnversionedRowSetup##suffix unversionedRowSetup; \
\
        auto unversionedRow = unversionedRowSetup.BuildUnversionedRow(); \
        unversionedRowSetup.AssertExpectedUnversionedRow(unversionedRow); \
    }

    TEST_UNVERSIONED_ROW_SETUP(Full)
    TEST_UNVERSIONED_ROW_SETUP(Large)
    TEST_UNVERSIONED_ROW_SETUP(LargeOptional)
    TEST_UNVERSIONED_ROW_SETUP(Small)

#undef TEST_UNVERSIONED_ROW_SETUP

#define TEST_INPUT(leftSuffix, rightSuffix, allowExtraYtFields, allowExtraYqlFields) \
    Y_UNIT_TEST(Input##leftSuffix##To##rightSuffix) { \
        NYql::NYtflow::NCodec::NTest::TUnversionedRowSetup##leftSuffix unversionedRowSetup; \
        NYql::NYtflow::NCodec::NTest::TUnboxedValueSetup##rightSuffix unboxedValueSetup; \
\
        auto inputCodec = NYql::NYtflow::NCodec::CreateRowInputCodec( \
            unboxedValueSetup.Type, \
            unversionedRowSetup.YtSchema, \
            *unboxedValueSetup.ValueBuilder, \
            *unboxedValueSetup.FunctionTypeInfoBuilder, \
            NYql::NYtflow::NCodec::TConvertOptions() \
                .WithAllowExtraYtFields(allowExtraYtFields) \
                .WithAllowExtraYqlFields(allowExtraYqlFields)); \
\
        auto unversionedRow = unversionedRowSetup.BuildUnversionedRow(); \
        auto unboxedValue = inputCodec->Convert(unversionedRow); \
\
        unboxedValueSetup.AssertExpectedUnboxedValue(unboxedValue); \
    }

    TEST_INPUT(Full, Full, false, false)
    TEST_INPUT(Small, LargeOptional, false, true)
    TEST_INPUT(Large, Small, true, false)

#undef TEST_INPUT

#define TEST_OUTPUT(leftSuffix, rightSuffix, allowExtraYtFields, allowExtraYqlFields) \
    Y_UNIT_TEST(Output##leftSuffix##To##rightSuffix) { \
        NYql::NYtflow::NCodec::NTest::TUnversionedRowSetup##rightSuffix unversionedRowSetup; \
        NYql::NYtflow::NCodec::NTest::TUnboxedValueSetup##leftSuffix unboxedValueSetup; \
\
        auto outputCodec = NYql::NYtflow::NCodec::CreateRowOutputCodec( \
            unboxedValueSetup.Type, \
            unversionedRowSetup.YtSchema, \
            unversionedRowSetup.RowBuffer, \
            NYql::NYtflow::NCodec::TConvertOptions() \
                .WithAllowExtraYtFields(allowExtraYtFields) \
                .WithAllowExtraYqlFields(allowExtraYqlFields)); \
\
        auto unboxedValue = unboxedValueSetup.BuildUnboxedValue(); \
        auto unversionedRow = outputCodec->Convert(unboxedValue); \
\
        unversionedRowSetup.AssertExpectedUnversionedRow(unversionedRow); \
    }

    TEST_OUTPUT(Full, Full, false, false)
    TEST_OUTPUT(Small, LargeOptional, true, false)
    TEST_OUTPUT(Large, Small, false, true)

#undef TEST_OUTPUT
}
