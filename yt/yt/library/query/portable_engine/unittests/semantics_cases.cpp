#include "semantics_cases.h"

#include <util/generic/strbuf.h>

#include <limits>

namespace NYT::NQueryClient::NPortable::NTest {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

const std::vector<TScalarExpressionCase>& GetScalarExpressionCases()
{
    static const auto Cases = [] {
        const auto keySchema = TTableSchema({TColumnSchema("key", EValueType::String)});
        const auto signedSchema = TTableSchema({
            TColumnSchema("lhs", EValueType::Int64),
            TColumnSchema("rhs", EValueType::Int64),
        });
        const auto unsignedSchema = TTableSchema({
            TColumnSchema("lhs", EValueType::Uint64),
            TColumnSchema("rhs", EValueType::Uint64),
        });
        const auto compositeSchema = TTableSchema({
            TColumnSchema("key", EValueType::String),
            TColumnSchema("number", EValueType::Uint64),
        });
        const auto mixedSchema = TTableSchema({
            TColumnSchema("i", EValueType::Int64),
            TColumnSchema("u", EValueType::Uint64),
            TColumnSchema("b", EValueType::Boolean),
            TColumnSchema("s", EValueType::String),
        });
        const auto mixedIntegerSchema = TTableSchema({
            TColumnSchema("i", EValueType::Int64),
            TColumnSchema("u", EValueType::Uint64),
        });

        return std::vector<TScalarExpressionCase>{
            {
                .Name = "IntegerLiteral",
                .Source = "42",
                .InputRow = TUnversionedOwningRow(TUnversionedValueRange{}),
                .ExpectedValue = MakeUnversionedInt64Value(42),
                .Capabilities = {"literal"},
            },
            {
                .Name = "StringLiteral",
                .Source = "\"text\"",
                .InputRow = TUnversionedOwningRow(TUnversionedValueRange{}),
                .ExpectedValue = MakeUnversionedStringValue("text"),
                .Capabilities = {"literal"},
            },
            {
                .Name = "NullLiteral",
                .Source = "null",
                .InputRow = TUnversionedOwningRow(TUnversionedValueRange{}),
                .ExpectedValue = MakeUnversionedNullValue(),
                .Capabilities = {"literal"},
            },
            {
                .Name = "SchemaPositionReference",
                .Source = "key",
                .Schema = TTableSchema({
                    TColumnSchema("unused", EValueType::Int64),
                    TColumnSchema("key", EValueType::String),
                }),
                .InputRow = TUnversionedOwningRow({
                    MakeUnversionedInt64Value(99),
                    MakeUnversionedStringValue("payload", /*id*/ 1),
                }),
                .ExpectedValue = MakeUnversionedStringValue("payload"),
                .Capabilities = {"reference"},
            },
            {
                .Name = "HistoricalBareReferenceHash",
                .Source = "farm_hash(key)",
                .Schema = keySchema,
                .InputRow = TUnversionedOwningRow({MakeUnversionedStringValue("abc")}),
                .ExpectedValue = MakeUnversionedUint64Value(0x7b15151e28709746ULL),
                .Capabilities = {"reference", "farm_hash"},
            },
            {
                .Name = "HistoricalBracketedReferenceHash",
                .Source = "farm_hash([key])",
                .Schema = keySchema,
                .InputRow = TUnversionedOwningRow({MakeUnversionedStringValue("abc")}),
                .ExpectedValue = MakeUnversionedUint64Value(0x7b15151e28709746ULL),
                .Capabilities = {"reference", "farm_hash"},
            },
            {
                .Name = "HistoricalColumnNameLiteralHash",
                .Source = "farm_hash(\"key\")",
                .Schema = keySchema,
                .InputRow = TUnversionedOwningRow({MakeUnversionedStringValue("abc")}),
                .ExpectedValue = MakeUnversionedUint64Value(0x407e25082fdedf2cULL),
                .Capabilities = {"literal", "farm_hash"},
            },
            {
                .Name = "InputMessageIdHash",
                .Source = "farm_hash([$input_message_id])",
                .Schema = TTableSchema({TColumnSchema("$input_message_id", EValueType::String)}),
                .InputRow = TUnversionedOwningRow({MakeUnversionedStringValue("msg-17")}),
                .ExpectedValue = MakeUnversionedUint64Value(0x463ef428a4ff1c4dULL),
                .Capabilities = {"reference", "farm_hash"},
            },
            {
                .Name = "CompositeKeyHash",
                .Source = "farm_hash([key], [number])",
                .Schema = compositeSchema,
                .InputRow = TUnversionedOwningRow({
                    MakeUnversionedStringValue("alpha"),
                    MakeUnversionedUint64Value(7, /*id*/ 1),
                }),
                .ExpectedValue = MakeUnversionedUint64Value(0x269c2b99970a5b3dULL),
                .Capabilities = {"reference", "farm_hash"},
            },
            {
                .Name = "NullCompositeKeyHash",
                .Source = "farm_hash([key], [number])",
                .Schema = compositeSchema,
                .InputRow = TUnversionedOwningRow({
                    MakeUnversionedNullValue(),
                    MakeUnversionedUint64Value(7, /*id*/ 1),
                }),
                .ExpectedValue = MakeUnversionedUint64Value(0xf03df45b1975b535ULL),
                .Capabilities = {"reference", "farm_hash"},
            },
            {
                .Name = "MixedTypesHash",
                .Source = "farm_hash(i, u, b, s)",
                .Schema = mixedSchema,
                .InputRow = TUnversionedOwningRow({
                    MakeUnversionedInt64Value(-7),
                    MakeUnversionedUint64Value(11, /*id*/ 1),
                    MakeUnversionedBooleanValue(true, /*id*/ 2),
                    MakeUnversionedStringValue("xy", /*id*/ 3),
                }),
                .ExpectedValue = MakeUnversionedUint64Value(0x5f90424776524127ULL),
                .Capabilities = {"reference", "farm_hash"},
            },
            {
                .Name = "ReversedArgumentsHash",
                .Source = "farm_hash(s, b, u, i)",
                .Schema = mixedSchema,
                .InputRow = TUnversionedOwningRow({
                    MakeUnversionedInt64Value(-7),
                    MakeUnversionedUint64Value(11, /*id*/ 1),
                    MakeUnversionedBooleanValue(true, /*id*/ 2),
                    MakeUnversionedStringValue("xy", /*id*/ 3),
                }),
                .ExpectedValue = MakeUnversionedUint64Value(0xfd2435eda17206d9ULL),
                .Capabilities = {"reference", "farm_hash"},
            },
            {
                .Name = "SignedBitsHash",
                .Source = "farm_hash(i)",
                .Schema = TTableSchema({TColumnSchema("i", EValueType::Int64)}),
                .InputRow = TUnversionedOwningRow({MakeUnversionedInt64Value(-1)}),
                .ExpectedValue = MakeUnversionedUint64Value(0x62f280995c3dea58ULL),
                .Capabilities = {"reference", "farm_hash"},
            },
            {
                .Name = "UnsignedBitsHash",
                .Source = "farm_hash(u)",
                .Schema = TTableSchema({TColumnSchema("u", EValueType::Uint64)}),
                .InputRow = TUnversionedOwningRow({
                    MakeUnversionedUint64Value(std::numeric_limits<ui64>::max()),
                }),
                .ExpectedValue = MakeUnversionedUint64Value(0x62f280995c3dea58ULL),
                .Capabilities = {"reference", "farm_hash"},
            },
            {
                .Name = "RepeatedArgumentsHash",
                .Source = "farm_hash(i, i)",
                .Schema = TTableSchema({TColumnSchema("i", EValueType::Int64)}),
                .InputRow = TUnversionedOwningRow({MakeUnversionedInt64Value(-1)}),
                .ExpectedValue = MakeUnversionedUint64Value(0x940af1fb538b6bd5ULL),
                .Capabilities = {"reference", "farm_hash"},
            },
            {
                .Name = "EmbeddedZeroHash",
                .Source = "farm_hash(key)",
                .Schema = keySchema,
                .InputRow = TUnversionedOwningRow({
                    MakeUnversionedStringValue(TStringBuf("a\0b", /*size*/ 3)),
                }),
                .ExpectedValue = MakeUnversionedUint64Value(0x0706e84ffa9edd01ULL),
                .Capabilities = {"reference", "farm_hash"},
            },
            {
                .Name = "EmptyStringHash",
                .Source = "farm_hash(key)",
                .Schema = keySchema,
                .InputRow = TUnversionedOwningRow({MakeUnversionedStringValue("")}),
                .ExpectedValue = MakeUnversionedUint64Value(0xfcc22541f5657d35ULL),
                .Capabilities = {"reference", "farm_hash"},
            },
            {
                .Name = "NullHash",
                .Source = "farm_hash(key)",
                .Schema = keySchema,
                .InputRow = TUnversionedOwningRow({MakeUnversionedNullValue()}),
                .ExpectedValue = MakeUnversionedUint64Value(0x2e03bcb5a0233a41ULL),
                .Capabilities = {"reference", "farm_hash"},
            },
            {
                .Name = "PositiveModulo",
                .Source = "lhs % rhs",
                .Schema = signedSchema,
                .InputRow = TUnversionedOwningRow({
                    MakeUnversionedInt64Value(17),
                    MakeUnversionedInt64Value(5, /*id*/ 1),
                }),
                .ExpectedValue = MakeUnversionedInt64Value(2),
                .Capabilities = {"reference", "modulo_int64"},
            },
            {
                .Name = "NegativeDividendModulo",
                .Source = "lhs % rhs",
                .Schema = signedSchema,
                .InputRow = TUnversionedOwningRow({
                    MakeUnversionedInt64Value(-17),
                    MakeUnversionedInt64Value(5, /*id*/ 1),
                }),
                .ExpectedValue = MakeUnversionedInt64Value(-2),
                .Capabilities = {"reference", "modulo_int64"},
            },
            {
                .Name = "NegativeDivisorModulo",
                .Source = "lhs % rhs",
                .Schema = signedSchema,
                .InputRow = TUnversionedOwningRow({
                    MakeUnversionedInt64Value(17),
                    MakeUnversionedInt64Value(-5, /*id*/ 1),
                }),
                .ExpectedValue = MakeUnversionedInt64Value(2),
                .Capabilities = {"reference", "modulo_int64"},
            },
            {
                .Name = "NegativeOperandsModulo",
                .Source = "lhs % rhs",
                .Schema = signedSchema,
                .InputRow = TUnversionedOwningRow({
                    MakeUnversionedInt64Value(-17),
                    MakeUnversionedInt64Value(-5, /*id*/ 1),
                }),
                .ExpectedValue = MakeUnversionedInt64Value(-2),
                .Capabilities = {"reference", "modulo_int64"},
            },
            {
                .Name = "MinimumSignedModulo",
                .Source = "lhs % rhs",
                .Schema = signedSchema,
                .InputRow = TUnversionedOwningRow({
                    MakeUnversionedInt64Value(std::numeric_limits<i64>::min()),
                    MakeUnversionedInt64Value(1, /*id*/ 1),
                }),
                .ExpectedValue = MakeUnversionedInt64Value(0),
                .Capabilities = {"reference", "modulo_int64"},
            },
            {
                .Name = "MaximumSignedModulo",
                .Source = "lhs % rhs",
                .Schema = signedSchema,
                .InputRow = TUnversionedOwningRow({
                    MakeUnversionedInt64Value(std::numeric_limits<i64>::max()),
                    MakeUnversionedInt64Value(3, /*id*/ 1),
                }),
                .ExpectedValue = MakeUnversionedInt64Value(1),
                .Capabilities = {"reference", "modulo_int64"},
            },
            {
                .Name = "MaximumUnsignedModulo",
                .Source = "lhs % rhs",
                .Schema = unsignedSchema,
                .InputRow = TUnversionedOwningRow({
                    MakeUnversionedUint64Value(std::numeric_limits<ui64>::max()),
                    MakeUnversionedUint64Value(2, /*id*/ 1),
                }),
                .ExpectedValue = MakeUnversionedUint64Value(1),
                .Capabilities = {"reference", "modulo_uint64"},
            },
            {
                .Name = "ZeroUnsignedModulo",
                .Source = "lhs % rhs",
                .Schema = unsignedSchema,
                .InputRow = TUnversionedOwningRow({
                    MakeUnversionedUint64Value(0),
                    MakeUnversionedUint64Value(std::numeric_limits<ui64>::max(), /*id*/ 1),
                }),
                .ExpectedValue = MakeUnversionedUint64Value(0),
                .Capabilities = {"reference", "modulo_uint64"},
            },
            {
                .Name = "NullSignedModuloZero",
                .Source = "lhs % rhs",
                .Schema = signedSchema,
                .InputRow = TUnversionedOwningRow({
                    MakeUnversionedNullValue(),
                    MakeUnversionedInt64Value(0, /*id*/ 1),
                }),
                .ExpectedValue = MakeUnversionedNullValue(),
                .Capabilities = {"reference", "modulo_int64"},
            },
            {
                .Name = "NullUnsignedModuloZero",
                .Source = "lhs % rhs",
                .Schema = unsignedSchema,
                .InputRow = TUnversionedOwningRow({
                    MakeUnversionedNullValue(),
                    MakeUnversionedUint64Value(0, /*id*/ 1),
                }),
                .ExpectedValue = MakeUnversionedNullValue(),
                .Capabilities = {"reference", "modulo_uint64"},
            },
            {
                .Name = "SignedModuloNull",
                .Source = "lhs % rhs",
                .Schema = signedSchema,
                .InputRow = TUnversionedOwningRow({
                    MakeUnversionedInt64Value(17),
                    MakeUnversionedNullValue(/*id*/ 1),
                }),
                .ExpectedValue = MakeUnversionedNullValue(),
                .Capabilities = {"reference", "modulo_int64"},
            },
            {
                .Name = "UnsignedModuloNull",
                .Source = "lhs % rhs",
                .Schema = unsignedSchema,
                .InputRow = TUnversionedOwningRow({
                    MakeUnversionedUint64Value(17),
                    MakeUnversionedNullValue(/*id*/ 1),
                }),
                .ExpectedValue = MakeUnversionedNullValue(),
                .Capabilities = {"reference", "modulo_uint64"},
            },
            {
                .Name = "SignedModuloZero",
                .Source = "lhs % rhs",
                .Schema = signedSchema,
                .InputRow = TUnversionedOwningRow({
                    MakeUnversionedInt64Value(17),
                    MakeUnversionedInt64Value(0, /*id*/ 1),
                }),
                .ExpectedError = "Division by zero",
                .Capabilities = {"reference", "modulo_int64"},
            },
            {
                .Name = "UnsignedModuloZero",
                .Source = "lhs % rhs",
                .Schema = unsignedSchema,
                .InputRow = TUnversionedOwningRow({
                    MakeUnversionedUint64Value(17),
                    MakeUnversionedUint64Value(0, /*id*/ 1),
                }),
                .ExpectedError = "Division by zero",
                .Capabilities = {"reference", "modulo_uint64"},
            },
            {
                .Name = "SignedModuloOverflow",
                .Source = "lhs % rhs",
                .Schema = signedSchema,
                .InputRow = TUnversionedOwningRow({
                    MakeUnversionedInt64Value(std::numeric_limits<i64>::min()),
                    MakeUnversionedInt64Value(-1, /*id*/ 1),
                }),
                .ExpectedError = "Division of INT_MIN by -1",
                .Capabilities = {"reference", "modulo_int64"},
            },
            {
                .Name = "CoercedNegativeLiteralModulo",
                .Source = "u % -1",
                .Schema = TTableSchema({TColumnSchema("u", EValueType::Uint64)}),
                .InputRow = TUnversionedOwningRow({MakeUnversionedUint64Value(3)}),
                .ExpectedValue = MakeUnversionedUint64Value(3),
                .Capabilities = {"literal", "reference", "modulo_uint64"},
            },
            {
                .Name = "SignedToUnsignedDividendModulo",
                .Source = "i % u",
                .Schema = mixedIntegerSchema,
                .InputRow = TUnversionedOwningRow({
                    MakeUnversionedInt64Value(-1),
                    MakeUnversionedUint64Value(3, /*id*/ 1),
                }),
                .ExpectedValue = MakeUnversionedUint64Value(0),
                .BuilderVersion = 2,
                .Capabilities = {"reference", "modulo_uint64", "uint64_from_int64"},
            },
            {
                .Name = "SignedToUnsignedDivisorModulo",
                .Source = "u % i",
                .Schema = mixedIntegerSchema,
                .InputRow = TUnversionedOwningRow({
                    MakeUnversionedInt64Value(-1),
                    MakeUnversionedUint64Value(3, /*id*/ 1),
                }),
                .ExpectedValue = MakeUnversionedUint64Value(3),
                .BuilderVersion = 2,
                .Capabilities = {"reference", "modulo_uint64", "uint64_from_int64"},
            },
            {
                .Name = "HashModuloSignedDivisor",
                .Source = "farm_hash(key) % divisor",
                .Schema = TTableSchema({
                    TColumnSchema("key", EValueType::String),
                    TColumnSchema("divisor", EValueType::Int64),
                }),
                .InputRow = TUnversionedOwningRow({
                    MakeUnversionedStringValue("abc"),
                    MakeUnversionedInt64Value(10, /*id*/ 1),
                }),
                .ExpectedValue = MakeUnversionedUint64Value(8),
                .BuilderVersion = 2,
                .Capabilities = {"reference", "farm_hash", "modulo_uint64", "uint64_from_int64"},
            },
            {
                .Name = "NullSignedToUnsignedModulo",
                .Source = "i % u",
                .Schema = mixedIntegerSchema,
                .InputRow = TUnversionedOwningRow({
                    MakeUnversionedNullValue(),
                    MakeUnversionedUint64Value(3, /*id*/ 1),
                }),
                .ExpectedValue = MakeUnversionedNullValue(),
                .BuilderVersion = 2,
                .Capabilities = {"reference", "modulo_uint64", "uint64_from_int64"},
            },
            {
                .Name = "MinimumSignedToUnsignedModulo",
                .Source = "i % u",
                .Schema = mixedIntegerSchema,
                .InputRow = TUnversionedOwningRow({
                    MakeUnversionedInt64Value(std::numeric_limits<i64>::min()),
                    MakeUnversionedUint64Value(3, /*id*/ 1),
                }),
                .ExpectedValue = MakeUnversionedUint64Value(2),
                .BuilderVersion = 2,
                .Capabilities = {"reference", "modulo_uint64", "uint64_from_int64"},
            },
            {
                .Name = "NullLiteralHash",
                .Source = "farm_hash(null)",
                .InputRow = TUnversionedOwningRow(TUnversionedValueRange{}),
                .ExpectedValue = MakeUnversionedUint64Value(0x2e03bcb5a0233a41ULL),
                .Capabilities = {"literal", "farm_hash"},
            },
            {
                .Name = "NegativeSignedToUnsigned",
                .Source = "uint64(i)",
                .Schema = TTableSchema({TColumnSchema("i", EValueType::Int64)}),
                .InputRow = TUnversionedOwningRow({MakeUnversionedInt64Value(-1)}),
                .ExpectedValue = MakeUnversionedUint64Value(0xffffffffffffffffULL),
                .Capabilities = {"reference", "uint64_from_int64"},
            },
            {
                .Name = "MinimumSignedToUnsigned",
                .Source = "uint64(i)",
                .Schema = TTableSchema({TColumnSchema("i", EValueType::Int64)}),
                .InputRow = TUnversionedOwningRow({
                    MakeUnversionedInt64Value(std::numeric_limits<i64>::min()),
                }),
                .ExpectedValue = MakeUnversionedUint64Value(0x8000000000000000ULL),
                .Capabilities = {"reference", "uint64_from_int64"},
            },
            {
                .Name = "MaximumSignedToUnsigned",
                .Source = "uint64(i)",
                .Schema = TTableSchema({TColumnSchema("i", EValueType::Int64)}),
                .InputRow = TUnversionedOwningRow({
                    MakeUnversionedInt64Value(std::numeric_limits<i64>::max()),
                }),
                .ExpectedValue = MakeUnversionedUint64Value(0x7fffffffffffffffULL),
                .Capabilities = {"reference", "uint64_from_int64"},
            },
            {
                .Name = "ZeroSignedToUnsigned",
                .Source = "uint64(i)",
                .Schema = TTableSchema({TColumnSchema("i", EValueType::Int64)}),
                .InputRow = TUnversionedOwningRow({MakeUnversionedInt64Value(0)}),
                .ExpectedValue = MakeUnversionedUint64Value(0),
                .Capabilities = {"reference", "uint64_from_int64"},
            },
            {
                .Name = "NullSignedToUnsigned",
                .Source = "uint64(i)",
                .Schema = TTableSchema({TColumnSchema("i", EValueType::Int64)}),
                .InputRow = TUnversionedOwningRow({MakeUnversionedNullValue()}),
                .ExpectedValue = MakeUnversionedNullValue(),
                .Capabilities = {"reference", "uint64_from_int64"},
            },
        };
    }();

    return Cases;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient::NPortable::NTest
