#include "helpers.h"

#include <yt/yt/library/query/engine_api/column_evaluator.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

const std::string TRandomExpressionGenerator::Letters("ABCDEFGHIJKLMNOPQRSTUVWXYZ");

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

std::string TRandomExpressionGenerator::GenerateFieldTuple(TRange<int> ids)
{
    YT_VERIFY(!ids.Empty());

    std::string result = ids.Size() > 1 ? "(" : "";

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

std::string TRandomExpressionGenerator::GenerateLiteralTuple(TRange<int> ids)
{
    YT_VERIFY(!ids.Empty());

    std::string result = ids.Size() > 1 ? "(" : "";

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

std::string TRandomExpressionGenerator::GenerateRandomLiteral(EValueType type)
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
        // FIXME(lukyan): Pregenerate random strings.
        case EValueType::String: {
            static constexpr int MaxStringLength = 10;
            auto length = Rng.Uniform(MaxStringLength);

            std::string result(length, '\0');
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

std::string TRandomExpressionGenerator::GenerateRelation(int tupleSize)
{
    auto ids = GenerateRandomFieldIds(tupleSize);

    std::sort(ids.begin(), ids.end());
    ids.erase(std::unique(ids.begin(), ids.end()), ids.end());

    return GenerateRelation(ids);
}

std::string TRandomExpressionGenerator::GenerateRelation(TRange<int> ids)
{
    const char* relationOps[] = {">", ">=", "<", "<=", "=", "!=", "IN", "is_prefix"};
    int maxOp = 7;
    if (ids.size() == 1 && Schema->Columns()[ids[0]].GetWireType() == EValueType::String) {
        maxOp++;
    }
    const char* relationOp = relationOps[Rng.Uniform(maxOp)];

    return GenerateRelation(ids, relationOp);
}

std::string TRandomExpressionGenerator::GenerateRelation(TRange<int> ids, const char* relationOp)
{
    if (relationOp == TStringBuf("is_prefix")) {
        auto id = ids[0];

        return Format("is_prefix(%v, %v)",
            GenerateRandomLiteral(Schema->Columns()[id].GetWireType()),
            Schema->Columns()[id].Name());
    }

    std::string result = GenerateFieldTuple(ids);
    result += Format(" %v ", relationOp);
    if (relationOp == std::string("IN")) {
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

std::string TRandomExpressionGenerator::RowToLiteralTuple(TUnversionedRow row)
{
    std::string result =  "(";
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

std::string TRandomExpressionGenerator::GenerateContinuationToken(int keyColumnCount)
{
    YT_VERIFY(keyColumnCount > 1);
    std::vector<int> ids;
    for (int columnIndex = 0; columnIndex < keyColumnCount; ++columnIndex) {
        ids.push_back(columnIndex);
    }

    std::string result = GenerateFieldTuple(ids);

    const char* relationOps[] = {">", ">=", "<", "<="};
    const char* relationOp = relationOps[Rng.Uniform(4)];

    result += Format(" %v ", relationOp);
    result += RowToLiteralTuple(GenerateRandomRow(keyColumnCount));
    return result;
}

TRow TRandomExpressionGenerator::GenerateRandomRow(int keyColumnCount)
{
    auto row = RowBuffer->AllocateUnversioned(keyColumnCount);
    for (int columnIndex = 0; columnIndex < keyColumnCount; ++columnIndex) {
        if (!Schema->Columns()[columnIndex].Expression()) {
            row[columnIndex] = GenerateRandomUnversionedLiteral(Schema->Columns()[columnIndex].GetWireType());
            row[columnIndex].Id = columnIndex;
        } else {
            row[columnIndex] = MakeUnversionedNullValue(columnIndex);
        }
    }

    if (ColumnEvaluator) {
        ColumnEvaluator->EvaluateKeys(row, RowBuffer, /*preserveColumnsIds*/ false);
    }

    return row;
}

std::string TRandomExpressionGenerator::GenerateRelationOrContinuationToken()
{
    return Rng.Uniform(4) == 0
        ? GenerateContinuationToken(GetExponentialDistribution(Schema->GetKeyColumnCount() - 2) + 2)
        : GenerateRelation(GetExponentialDistribution(3) + 1);
}

std::string TRandomExpressionGenerator::GenerateExpression2()
{
    std::string result = GenerateRelationOrContinuationToken();

    int count = GetExponentialDistribution(5);
    for (int i = 0; i < count; ++i) {
        result += Rng.Uniform(4) == 0 ? " OR " : " AND ";
        result += Rng.Uniform(4) == 0 ? " NOT " : "";
        result += GenerateRelation(1);

    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
