#include "ql_helpers.h"

#include <yt/yt/library/query/engine/folding_profiler.h>

#include <yt/yt/library/query/engine_api/column_evaluator.h>

#include <yt/yt/core/yson/public.h>
#include <yt/yt/core/yson/string.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NQueryClient {

using NCodegen::EExecutionBackend;

////////////////////////////////////////////////////////////////////////////////

void PrintTo(TConstExpressionPtr expr, ::std::ostream* os)
{
    *os << InferName(expr);
}

TValue MakeInt64(i64 value)
{
    return MakeUnversionedInt64Value(value);
}

TValue MakeUint64(ui64 value)
{
    return MakeUnversionedUint64Value(value);
}

TValue MakeDouble(double value)
{
    return MakeUnversionedDoubleValue(value);
}

TValue MakeBoolean(bool value)
{
    return MakeUnversionedBooleanValue(value);
}

TValue MakeString(TStringBuf value)
{
    return MakeUnversionedStringValue(value);
}

TValue MakeNull()
{
    return MakeUnversionedSentinelValue(EValueType::Null);
}

TKeyColumns GetSampleKeyColumns()
{
    TKeyColumns keyColumns;
    keyColumns.push_back("k");
    keyColumns.push_back("l");
    keyColumns.push_back("m");
    return keyColumns;
}

TKeyColumns GetSampleKeyColumns2()
{
    TKeyColumns keyColumns;
    keyColumns.push_back("k");
    keyColumns.push_back("l");
    keyColumns.push_back("m");
    keyColumns.push_back("s");
    return keyColumns;
}

TKeyColumns GetSampleKeyColumns3()
{
    TKeyColumns keyColumns;
    keyColumns.push_back("k");
    keyColumns.push_back("any_key");
    return keyColumns;
}

TTableSchemaPtr GetSampleTableSchema()
{
    return New<TTableSchema>(std::vector{
        TColumnSchema("k", EValueType::Int64).SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("l", EValueType::Int64).SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("m", EValueType::Int64).SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("a", EValueType::Int64),
        TColumnSchema("b", EValueType::Int64),
        TColumnSchema("c", EValueType::Int64),
        TColumnSchema("s", EValueType::String),
        TColumnSchema("u", EValueType::String),
        TColumnSchema("ki", EValueType::Int64),
        TColumnSchema("ku", EValueType::Uint64),
        TColumnSchema("kd", EValueType::Double),
        TColumnSchema("any_key", EValueType::Any),
    });
}

TDataSplit MakeSimpleSplit(const TYPath& /*path*/, ui64 /*counter*/)
{
    TDataSplit dataSplit;
    dataSplit.TableSchema = GetSampleTableSchema();
    return dataSplit;
}

TDataSplit MakeSplit(const std::vector<TColumnSchema>& columns, ui64 /*counter*/)
{
    TDataSplit dataSplit;
    dataSplit.TableSchema = New<TTableSchema>(columns);
    return dataSplit;
}

TFuture<TDataSplit> RaiseTableNotFound(const TYPath& path)
{
    return MakeFuture<TDataSplit>(TError(
        "Could not find table %v",
        path));
}

////////////////////////////////////////////////////////////////////////////////

void ProfileForBothExecutionBackends(
    const TConstBaseQueryPtr& query,
    llvm::FoldingSetNodeID* id,
    TCGVariables* variables,
    TJoinSubqueryProfiler joinProfiler)
{
    Profile(query, id, variables, joinProfiler, /*useCanonicalNullRelations*/ false, EExecutionBackend::Native)();
    if (EnableWebAssemblyInUnitTests()) {
        Profile(query, id, variables, joinProfiler, /*useCanonicalNullRelations*/ false, EExecutionBackend::WebAssembly)();
    }
}

void ProfileForBothExecutionBackends(
    const TConstExpressionPtr& expr,
    const TTableSchemaPtr& schema,
    llvm::FoldingSetNodeID* id,
    TCGVariables* variables)
{
    Profile(expr, schema, id, variables, /*useCanonicalNullRelations*/ false, EExecutionBackend::Native)();
    if (EnableWebAssemblyInUnitTests()) {
        Profile(expr, schema, id, variables, /*useCanonicalNullRelations*/ false, EExecutionBackend::WebAssembly)();
    }
}

////////////////////////////////////////////////////////////////////////////////

const TString TRandomExpressionGenerator::Letters("ABCDEFGHIJKLMNOPQRSTUVWXYZ");

int TRandomExpressionGenerator::GetExponentialDistribution(int power)
{
    YT_VERIFY(power > 0);
    int uniformValue = Rng.Uniform(1 << (power - 1));
    int valueBitCount = uniformValue != 0 ? GetValueBitCount(uniformValue) : 0;
    int result = (power - 1) - valueBitCount;
    YT_VERIFY(result >= 0 && result < power);
    return result;
}

std::vector<ui64> TRandomExpressionGenerator::GenerateRandomValues()
{
    std::vector<ui64> result;

    // Corner cases.
    result.push_back(std::numeric_limits<i64>::min());
    result.push_back(std::numeric_limits<i64>::max());
    result.push_back(std::numeric_limits<ui64>::min());
    result.push_back(std::numeric_limits<ui64>::max());

    // Some random values.
    for (int i = 0; i < 7; ++i) {
        result.push_back(Rng.GenRand());
    }

    for (int i = 0; i < 5; ++i) {
        ui64 bits = Rng.Uniform(64);
        ui64 value = (1ULL << bits) | Rng.Uniform(1ULL << bits);

        result.push_back(value);
    }

    std::sort(result.begin(), result.end());
    result.erase(std::unique(result.begin(), result.end()), result.end());

    return result;
}

std::vector<int> TRandomExpressionGenerator::GenerateRandomFieldIds(int size)
{
    std::vector<int> result;
    for (int i = 0; i < size; ++i) {
        result.push_back(GetExponentialDistribution(Schema->GetKeyColumnCount() - 1) + 1);
    }

    return result;
}

TString TRandomExpressionGenerator::GenerateFieldTuple(TRange<int> ids)
{
    YT_VERIFY(!ids.Empty());

    TString result = ids.Size() > 1 ? "(" : "";

    bool first = true;
    for (auto id : ids) {
        if (!first) {
            result += ", ";
        } else {
            first = false;
        }

        result += Schema->Columns()[id].Name();
    }

    result += ids.Size() > 1 ? ")" : "";
    return result;
}

TString TRandomExpressionGenerator::GenerateLiteralTuple(TRange<int> ids)
{
    YT_VERIFY(!ids.Empty());

    TString result = ids.Size() > 1 ? "(" : "";

    bool first = true;
    for (auto id : ids) {
        if (!first) {
            result += ", ";
        } else {
            first = false;
        }

        result += GenerateRandomLiteral(Schema->Columns()[id].GetWireType());
    }

    result += ids.Size() > 1 ? ")" : "";
    return result;
}

TString TRandomExpressionGenerator::GenerateRandomLiteral(EValueType type)
{
    bool nullValue = Rng.Uniform(10) == 0;
    if (nullValue) {
        return "null";
    }

    switch (type) {
        case EValueType::Int64:
            return Format("%v", static_cast<i64>(GenerateInt()));
        case EValueType::Uint64:
            return Format("%vu", GenerateInt());
        // FIXME: Pregenerate random strings.
        case EValueType::String: {
            static constexpr int MaxStringLength = 10;
            auto length = Rng.Uniform(MaxStringLength);

            TString result(length, '\0');
            for (size_t index = 0; index < length; ++index) {
                result[index] = Letters[Rng.Uniform(Letters.size())];
            }

            return Format("%Qv", result);
        }
        case EValueType::Double:
            return Format("%v", Rng.GenRandReal1() * (1ULL << 63));
        case EValueType::Boolean:
            return Rng.Uniform(2) ? "true" : "false";
        default:
            YT_ABORT();
    }
}

TUnversionedValue TRandomExpressionGenerator::GenerateRandomUnversionedLiteral(EValueType type)
{
    bool nullValue = Rng.Uniform(10) == 0;
    if (nullValue) {
        return MakeUnversionedNullValue();
    }

    switch (type) {
        case EValueType::Int64:
            return MakeUnversionedInt64Value(GenerateInt());
        case EValueType::Uint64:
            return MakeUnversionedUint64Value(GenerateInt());
        case EValueType::String: {
            static constexpr int MaxStringLength = 10;
            auto length = Rng.Uniform(MaxStringLength);

            char* data = RowBuffer->GetPool()->AllocateUnaligned(length);
            for (size_t index = 0; index < length; ++index) {
                data[index] = Letters[Rng.Uniform(Letters.size())];
            }

            return MakeUnversionedStringValue(TStringBuf(data, length));
        }
        case EValueType::Double:
            return MakeUnversionedDoubleValue(Rng.GenRandReal1() * (1ULL << 63));
        case EValueType::Boolean:
            return MakeUnversionedBooleanValue(Rng.Uniform(2));
        default:
            YT_ABORT();
    }
}

ui64 TRandomExpressionGenerator::GenerateInt()
{
    return RandomValues[Rng.Uniform(RandomValues.size())];
}

TString TRandomExpressionGenerator::GenerateRelation(int tupleSize)
{
    auto ids = GenerateRandomFieldIds(tupleSize);

    std::sort(ids.begin(), ids.end());
    ids.erase(std::unique(ids.begin(), ids.end()), ids.end());

    return GenerateRelation(ids);
}

TString TRandomExpressionGenerator::GenerateRelation(TRange<int> ids)
{
    const char* reationOps[] = {">", ">=", "<", "<=", "=", "!=", "IN"};
    const char* reationOp = reationOps[Rng.Uniform(7)];

    return GenerateRelation(ids, reationOp);
}

TString TRandomExpressionGenerator::GenerateRelation(TRange<int> ids, const char* reationOp)
{
    TString result = GenerateFieldTuple(ids);
    result += Format(" %v ", reationOp);
    if (reationOp == TString("IN")) {
        result += "(";
        int tupleCount = GetExponentialDistribution(9) + 1;
        bool first = true;
        for (int i = 0; i < tupleCount; ++i) {
            if (!first) {
                result += ", ";
            } else {
                first = false;
            }
            result += GenerateLiteralTuple(ids);
        }

        result += ")";
    } else {
        result += GenerateLiteralTuple(ids);
    }

    return result;
}

TString TRandomExpressionGenerator::RowToLiteralTuple(TUnversionedRow row)
{
    TString result =  "(";
    bool first = true;
    for (int columnIndex = 0; columnIndex < static_cast<int>(row.GetCount()); ++columnIndex) {
        if (!first) {
            result += ", ";
        } else {
            first = false;
        }

        auto value = row[columnIndex];

        switch (value.Type) {
            case EValueType::Null:
                result += "null";
                break;
            case EValueType::Int64:
                result += Format("%v", value.Data.Int64);
                break;
            case EValueType::Uint64:
                result += Format("%vu", value.Data.Uint64);
                break;
            case EValueType::String:
                result += Format("%Qv", value.AsStringBuf());
                break;
            case EValueType::Double:
                result += Format("%v", value.Data.Double);
                break;
            case EValueType::Boolean:
                result += value.Data.Boolean ? "true" : "false";
                break;
            default:
                YT_ABORT();
        }
    }

    result += ")";
    return result;
}

TString TRandomExpressionGenerator::GenerateContinuationToken(int keyColumnCount)
{
    YT_VERIFY(keyColumnCount > 1);
    std::vector<int> ids;
    for (int columnIndex = 0; columnIndex < keyColumnCount; ++columnIndex) {
        ids.push_back(columnIndex);
    }

    TString result = GenerateFieldTuple(ids);

    const char* reationOps[] = {">", ">=", "<", "<="};
    const char* reationOp = reationOps[Rng.Uniform(4)];

    result += Format(" %v ", reationOp);
    result += RowToLiteralTuple(GenerateRandomRow(keyColumnCount));
    return result;
}

TRow TRandomExpressionGenerator::GenerateRandomRow(int keyColumnCount)
{
    auto row = RowBuffer->AllocateUnversioned(keyColumnCount);
    for (int columnIndex = 0; columnIndex < keyColumnCount; ++columnIndex) {
        if (!Schema->Columns()[columnIndex].Expression()) {
            row[columnIndex] = GenerateRandomUnversionedLiteral(Schema->Columns()[columnIndex].GetWireType());
        }
    }

    ColumnEvaluator->EvaluateKeys(row, RowBuffer);

    return row;
}

TString TRandomExpressionGenerator::GenerateRelationOrContinuationToken()
{
    return Rng.Uniform(4) == 0
        ? GenerateContinuationToken(GetExponentialDistribution(Schema->GetKeyColumnCount() - 2) + 2)
        : GenerateRelation(GetExponentialDistribution(3) + 1);
}

TString TRandomExpressionGenerator::GenerateExpression2()
{
    TString result = GenerateRelationOrContinuationToken();

    int count = GetExponentialDistribution(5);
    for (int i = 0; i < count; ++i) {
        result += Rng.Uniform(4) == 0 ? " OR " : " AND ";
        result += GenerateRelation(1);

    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
