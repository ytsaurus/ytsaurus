#pragma once

#include <yt/yt/library/query/base/query.h>

#include <util/random/fast.h>

namespace NYT::NQueryClient {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

class TRandomExpressionGenerator
{
public:
    TTableSchemaPtr Schema;
    TRowBufferPtr RowBuffer;
    TColumnEvaluatorPtr ColumnEvaluator;

    TFastRng64 Rng{42};

    std::vector<ui64> RandomValues = GenerateRandomValues();

    static const std::string Letters;

    std::string GenerateExpression2();

    int GetExponentialDistribution(int power);

    std::string GenerateRelation(int tupleSize);

    TRow GenerateRandomRow(int keyColumnCount);

private:
    std::vector<ui64> GenerateRandomValues();

    std::vector<int> GenerateRandomFieldIds(int size);

    std::string GenerateFieldTuple(TRange<int> ids);

    std::string GenerateLiteralTuple(TRange<int> ids);

    std::string GenerateRandomLiteral(EValueType type);

    TUnversionedValue GenerateRandomUnversionedLiteral(EValueType type);

    ui64 GenerateInt();

    std::string GenerateRelation(TRange<int> ids);

    std::string GenerateRelation(TRange<int> ids, const char* reationOp);

    std::string RowToLiteralTuple(TUnversionedRow row);

    std::string GenerateContinuationToken(int keyColumnCount);

    std::string GenerateRelationOrContinuationToken();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
