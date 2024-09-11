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

    static const TString Letters;

    TString GenerateExpression2();

    int GetExponentialDistribution(int power);

    TString GenerateRelation(int tupleSize);

    TRow GenerateRandomRow(int keyColumnCount);

private:
    std::vector<ui64> GenerateRandomValues();

    std::vector<int> GenerateRandomFieldIds(int size);

    TString GenerateFieldTuple(TRange<int> ids);

    TString GenerateLiteralTuple(TRange<int> ids);

    TString GenerateRandomLiteral(EValueType type);

    TUnversionedValue GenerateRandomUnversionedLiteral(EValueType type);

    ui64 GenerateInt();

    TString GenerateRelation(TRange<int> ids);

    TString GenerateRelation(TRange<int> ids, const char* reationOp);

    TString RowToLiteralTuple(TUnversionedRow row);

    TString GenerateContinuationToken(int keyColumnCount);

    TString GenerateRelationOrContinuationToken();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
