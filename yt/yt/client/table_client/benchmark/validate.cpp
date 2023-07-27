#include <benchmark/benchmark.h>

#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/experiments/random_row/wild_schema_catalog.h>
#include <yt/yt/experiments/random_row/random_row.h>

using namespace NYT::NRandomRow;
using namespace NYT::NTableClient;
using namespace NYT::NWildSchemaCatalog;

////////////////////////////////////////////////////////////////////////////////

static inline void ValidateUnversionedRow(const TTableSchemaPtr& schema, const TUnversionedOwningRow& row)
{
    for (int i = 0; i < schema->GetColumnCount(); ++i) {
        auto value = *(row.Begin() + i);
        YT_VERIFY(value.Id == i);
        ValidateValueType(value, schema->Columns()[i], false);
    }
}

void Validate(benchmark::State& state, const TTableSchemaPtr& schema)
{
    auto gen = CreateRandomRowGenerator(schema, 43);
    auto row = gen->GenerateRow();
    for (auto _ : state) {
        ValidateUnversionedRow(schema, row);
    }
}

BENCHMARK_CAPTURE(Validate, TIntermediateSemidupsDataSchema, GetTIntermediateSemidupsDataSchema());
BENCHMARK_CAPTURE(Validate, TCanonisizeUrlSchema, GetTCanonizedUrlSchema());
BENCHMARK_CAPTURE(Validate, TUrlDataSchema, GetTUrlDataSchema());

////////////////////////////////////////////////////////////////////////////////
