#include <yt/core/test_framework/framework.h>

#include <yt/client/table_client/logical_type.h>
#include <yt/client/table_client/validate_logical_type.h>

#include <util/string/escape.h>


namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

#define ERROR_ATTRS(logicalType, ysonString) \
        " at " << __FILE__ ":" << __LINE__ << '\n' \
        << "Type: " << ToString(*logicalType) << '\n' \
        << "YsonString: " << '"' << EscapeC(ysonString) << '"' << '\n'\

#define EXPECT_BAD_TYPE(logicalTypeExpr, ysonString) \
    do { \
        auto logicalType = logicalTypeExpr; \
        try { \
            ValidateComplexLogicalType(ysonString, logicalType); \
            ADD_FAILURE() << "Expected type failure" << ERROR_ATTRS(logicalType, ysonString); \
        } catch (const std::exception& ex) { \
            if (!IsSchemaViolationError(ex)) { \
                ADD_FAILURE() << "Unexpected error" << ERROR_ATTRS(logicalType, ysonString) \
                    << "what: " << ex.what(); \
            } \
        } \
    } while (0)

#define EXPECT_GOOD_TYPE(logicalTypeExpr, ysonString) \
    do { \
        auto logicalType = logicalTypeExpr; \
        try { \
            ValidateComplexLogicalType(ysonString, logicalType); \
        } catch (const std::exception& ex) { \
            ADD_FAILURE() << "Unexpected error" << ERROR_ATTRS(logicalType, ysonString) \
                    << "what: " << ex.what(); \
        } \
    } while (0)

bool IsSchemaViolationError(const std::exception& ex) {
    auto errorException = dynamic_cast<const TErrorException*>(&ex);
    if (!errorException) {
        return false;
    }
    return errorException->Error().FindMatching(NYT::NTableClient::EErrorCode::SchemaViolation).has_value();
}

TEST(TValidateLogicalTypeTest, TestBasicTypes)
{
    EXPECT_GOOD_TYPE(SimpleLogicalType(ESimpleLogicalValueType::String), " foo ");
    EXPECT_GOOD_TYPE(SimpleLogicalType(ESimpleLogicalValueType::Int64), " 42 ");
    EXPECT_GOOD_TYPE(SimpleLogicalType(ESimpleLogicalValueType::Uint64), " 42u ");
    EXPECT_GOOD_TYPE(SimpleLogicalType(ESimpleLogicalValueType::Double), " 3.14 ");
    EXPECT_GOOD_TYPE(SimpleLogicalType(ESimpleLogicalValueType::Boolean), " %false ");

    EXPECT_BAD_TYPE(SimpleLogicalType(ESimpleLogicalValueType::String), " 76 ");
    EXPECT_BAD_TYPE(SimpleLogicalType(ESimpleLogicalValueType::Int64), " %true ");
    EXPECT_BAD_TYPE(SimpleLogicalType(ESimpleLogicalValueType::Uint64), " 14 ");
    EXPECT_BAD_TYPE(SimpleLogicalType(ESimpleLogicalValueType::Double), " bar ");
    EXPECT_BAD_TYPE(SimpleLogicalType(ESimpleLogicalValueType::Boolean), " 1 ");
}

TEST(TValidateLogicalTypeTest, TestSimpleOptionalTypes)
{
    EXPECT_GOOD_TYPE(SimpleLogicalType(ESimpleLogicalValueType::String, /*required*/ false), " foo ");
    EXPECT_GOOD_TYPE(SimpleLogicalType(ESimpleLogicalValueType::String, /*required*/ false), " # ");
    EXPECT_BAD_TYPE(SimpleLogicalType(ESimpleLogicalValueType::String, /*required*/ false), " 42 ");

    EXPECT_GOOD_TYPE(SimpleLogicalType(ESimpleLogicalValueType::Uint64, /*required*/ false), " 42u ");
    EXPECT_GOOD_TYPE(SimpleLogicalType(ESimpleLogicalValueType::Uint64, /*required*/ false), " # ");
    EXPECT_BAD_TYPE(SimpleLogicalType(ESimpleLogicalValueType::Uint64, /*required*/ false), " 42 ");
}

TEST(TValidateLogicalTypeTest, TestLogicalTypes)
{
#define CHECK_GOOD(type, value) \
    do { \
        EXPECT_GOOD_TYPE(SimpleLogicalType(type), value); \
    } while (0)

#define CHECK_BAD(type, value) \
    do { \
        EXPECT_BAD_TYPE(SimpleLogicalType(type), value); \
    } while (0)

    // Int8
    CHECK_GOOD(ESimpleLogicalValueType::Int8, " 127 ");
    CHECK_GOOD(ESimpleLogicalValueType::Int8, " -128 ");
    CHECK_BAD(ESimpleLogicalValueType::Int8, " 128 ");
    CHECK_BAD(ESimpleLogicalValueType::Int8, " -129 ");

    // Uint8
    CHECK_GOOD(ESimpleLogicalValueType::Uint8, " 127u ");
    CHECK_GOOD(ESimpleLogicalValueType::Uint8, " 128u ");
    CHECK_GOOD(ESimpleLogicalValueType::Uint8, " 255u ");
    CHECK_BAD(ESimpleLogicalValueType::Uint8, " 256u ");
    CHECK_BAD(ESimpleLogicalValueType::Uint8, " 100500u ");

    // Int16
    CHECK_GOOD(ESimpleLogicalValueType::Int16, " 32767 ");
    CHECK_GOOD(ESimpleLogicalValueType::Int16, " -32768 ");
    CHECK_BAD(ESimpleLogicalValueType::Int16, " 32768 ");
    CHECK_BAD(ESimpleLogicalValueType::Int16, " -32769 ");

    // Uint16
    CHECK_GOOD(ESimpleLogicalValueType::Uint16, " 32768u ");
    CHECK_GOOD(ESimpleLogicalValueType::Uint16, " 65535u ");
    CHECK_BAD(ESimpleLogicalValueType::Uint16, " 65536u ");

    // Int32
    CHECK_GOOD(ESimpleLogicalValueType::Int32, " 2147483647 ");
    CHECK_GOOD(ESimpleLogicalValueType::Int32, " -2147483648 ");
    CHECK_BAD(ESimpleLogicalValueType::Int32, " 2147483648 ");
    CHECK_BAD(ESimpleLogicalValueType::Int32, " -2147483649 ");

    // Uint32
    CHECK_GOOD(ESimpleLogicalValueType::Uint32, " 2147483648u ");
    CHECK_GOOD(ESimpleLogicalValueType::Uint32, " 4294967295u ");
    CHECK_BAD(ESimpleLogicalValueType::Uint32, " 4294967297u ");

    // Uint64
    CHECK_GOOD(ESimpleLogicalValueType::Int64, " 2147483648 ");
    CHECK_GOOD(ESimpleLogicalValueType::Int64, " -2147483649 ");

    // Uint64
    CHECK_GOOD(ESimpleLogicalValueType::Uint64, " 4294967297u ");

    // Utf8
    CHECK_GOOD(ESimpleLogicalValueType::Utf8, " foo ");
    CHECK_GOOD(ESimpleLogicalValueType::Utf8, " \"фу\" ");
    CHECK_BAD(ESimpleLogicalValueType::Utf8, " \"\244\" ");
}

TEST(TValidateLogicalTypeTest, TestAnyType)
{
    EXPECT_GOOD_TYPE(SimpleLogicalType(ESimpleLogicalValueType::Any), " foo ");
    EXPECT_GOOD_TYPE(SimpleLogicalType(ESimpleLogicalValueType::Any), " 42 ");
    EXPECT_GOOD_TYPE(SimpleLogicalType(ESimpleLogicalValueType::Any), " 15u ");
    EXPECT_GOOD_TYPE(SimpleLogicalType(ESimpleLogicalValueType::Any), " %true ");
    EXPECT_GOOD_TYPE(SimpleLogicalType(ESimpleLogicalValueType::Any), " 3.14 ");
    EXPECT_BAD_TYPE(SimpleLogicalType(ESimpleLogicalValueType::Any), "#");

    EXPECT_GOOD_TYPE(SimpleLogicalType(ESimpleLogicalValueType::Any), "[142; 53u; {foo=bar; bar=[baz]};]");

    EXPECT_GOOD_TYPE(SimpleLogicalType(ESimpleLogicalValueType::Any, /*required*/ false), "#");
    EXPECT_GOOD_TYPE(SimpleLogicalType(ESimpleLogicalValueType::Any, /*required*/ false), "[{bar=<type=list>[]}; bar; [baz];]");

    EXPECT_BAD_TYPE(SimpleLogicalType(ESimpleLogicalValueType::Any), "<>1");
    EXPECT_GOOD_TYPE(SimpleLogicalType(ESimpleLogicalValueType::Any), "[<>1]");
}

TEST(TValidateLogicalTypeTest, TestComplexOptionalType)
{
    const auto optionalOptionalInt = OptionalLogicalType(
        SimpleLogicalType(ESimpleLogicalValueType::Int64, /*required*/ false)
    );
    EXPECT_GOOD_TYPE(optionalOptionalInt, " [5] ");
    EXPECT_GOOD_TYPE(optionalOptionalInt, " [#] ");
    EXPECT_GOOD_TYPE(optionalOptionalInt, " # ");
    EXPECT_BAD_TYPE(optionalOptionalInt, " 5 ");
    EXPECT_BAD_TYPE(optionalOptionalInt, " [] ");
    EXPECT_BAD_TYPE(optionalOptionalInt, " [5; 5] ");

    const auto optionalListInt = OptionalLogicalType(
        ListLogicalType(
            SimpleLogicalType(ESimpleLogicalValueType::Int64)
        )
    );
    EXPECT_GOOD_TYPE(optionalListInt, "[5]");
    EXPECT_GOOD_TYPE(optionalListInt, "[5; 5]");
    EXPECT_GOOD_TYPE(optionalListInt, "[]");
    EXPECT_BAD_TYPE(optionalListInt, "[[5]]");

    const auto optionalOptionalListInt = OptionalLogicalType(
        OptionalLogicalType(
            ListLogicalType(
                SimpleLogicalType(ESimpleLogicalValueType::Int64)
            )
        )
    );
    EXPECT_GOOD_TYPE(optionalOptionalListInt, "[[5]]");
    EXPECT_GOOD_TYPE(optionalOptionalListInt, "[[5; 5]]");
    EXPECT_GOOD_TYPE(optionalOptionalListInt, "[[]]");
    EXPECT_BAD_TYPE(optionalOptionalListInt, "[[[5]]]");
    EXPECT_BAD_TYPE(optionalOptionalListInt, "[5]");

    const auto optionalOptionalOptionalAny = OptionalLogicalType(
        OptionalLogicalType(
            SimpleLogicalType(ESimpleLogicalValueType::Any, /*required*/ false)
        )
    );
    EXPECT_GOOD_TYPE(optionalOptionalOptionalAny, " [[5]] ");
    EXPECT_GOOD_TYPE(optionalOptionalOptionalAny, " [[#]] ");
    EXPECT_GOOD_TYPE(optionalOptionalOptionalAny, " [#] ");
    EXPECT_GOOD_TYPE(optionalOptionalOptionalAny, " # ");
    EXPECT_GOOD_TYPE(optionalOptionalOptionalAny, " [[[]]] ");
    EXPECT_GOOD_TYPE(optionalOptionalOptionalAny, " [[{foo=bar}]] ");
    EXPECT_BAD_TYPE(optionalOptionalOptionalAny, " [] ");
    EXPECT_BAD_TYPE(optionalOptionalOptionalAny, " [[]] ");
}

TEST(TValidateLogicalTypeTest, TestListType)
{
    const auto listInt = ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64));
    EXPECT_GOOD_TYPE(listInt, " [3] ");
    EXPECT_GOOD_TYPE(listInt, " [5] ");
    EXPECT_GOOD_TYPE(listInt, " [5;42;] ");

    EXPECT_BAD_TYPE(listInt, " [5;#;] ");
    EXPECT_BAD_TYPE(listInt, " {} ");
}

TEST(TValidateLogicalTypeTest, TestStructType)
{
    const auto struct1 = StructLogicalType({
        {"number",  SimpleLogicalType(ESimpleLogicalValueType::Int64)},
        {"english", SimpleLogicalType(ESimpleLogicalValueType::String)},
        {"russian", SimpleLogicalType(ESimpleLogicalValueType::String, /*required*/ false)},
    });

    EXPECT_GOOD_TYPE(struct1, " [3; three; TRI ] ");
    EXPECT_GOOD_TYPE(struct1, " [1; one; # ] ");
    EXPECT_GOOD_TYPE(struct1, " [1; one ] ");

    EXPECT_BAD_TYPE(struct1, " [ # ; three; TRI ] ");
    EXPECT_BAD_TYPE(struct1, " [ 3 ; # ; TRI ] ");
    EXPECT_BAD_TYPE(struct1, " [ 1 ] ");
    EXPECT_BAD_TYPE(struct1, " [ ] ");

    const auto struct2 = StructLogicalType({
        {"key",  SimpleLogicalType(ESimpleLogicalValueType::String, /*required*/ false)},
        {"subkey", SimpleLogicalType(ESimpleLogicalValueType::String, /*required*/ false)},
        {"value", SimpleLogicalType(ESimpleLogicalValueType::String, /*required*/ false)},
    });

    EXPECT_GOOD_TYPE(struct2, " [k ; s ; v ] ");
    EXPECT_GOOD_TYPE(struct2, " [# ; # ; #] ");
    EXPECT_GOOD_TYPE(struct2, " [# ; # ;] ");
    EXPECT_GOOD_TYPE(struct2, " [# ; ] ");
    EXPECT_GOOD_TYPE(struct2, " [ ] ");
    EXPECT_BAD_TYPE(struct2, " [ 2 ] ");
}

TEST(TValidateLogicalTypeTest, TestTupleType)
{
    const auto tuple1 = TupleLogicalType({
        SimpleLogicalType(ESimpleLogicalValueType::Int64),
        SimpleLogicalType(ESimpleLogicalValueType::String),
        SimpleLogicalType(ESimpleLogicalValueType::String, /*required*/ false),
    });

    EXPECT_GOOD_TYPE(tuple1, " [3; three; TRI ] ");
    EXPECT_GOOD_TYPE(tuple1, " [1; one; # ] ");

    EXPECT_BAD_TYPE(tuple1, " [3u; three; TRI ] ");
    EXPECT_BAD_TYPE(tuple1, " [1; one ] ");
    EXPECT_BAD_TYPE(tuple1, " [ # ; three; TRI ] ");
    EXPECT_BAD_TYPE(tuple1, " [ 3 ; # ; TRI ] ");
    EXPECT_BAD_TYPE(tuple1, " [ 1 ] ");
    EXPECT_BAD_TYPE(tuple1, " [ ] ");

    const auto tuple2 = TupleLogicalType({
        SimpleLogicalType(ESimpleLogicalValueType::String, /*required*/ false),
        SimpleLogicalType(ESimpleLogicalValueType::String, /*required*/ false),
        SimpleLogicalType(ESimpleLogicalValueType::String, /*required*/ false),
    });

    EXPECT_GOOD_TYPE(tuple2, " [k ; s ; v ] ");
    EXPECT_GOOD_TYPE(tuple2, " [# ; # ; #] ");

    EXPECT_BAD_TYPE(tuple2, " [# ; # ;] ");
    EXPECT_BAD_TYPE(tuple2, " [# ; ] ");
    EXPECT_BAD_TYPE(tuple2, " [ ] ");
    EXPECT_BAD_TYPE(tuple2, " [ 2 ] ");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableChunkFormat
