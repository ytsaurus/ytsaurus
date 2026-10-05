#include "builtin_registry.h"
#include "evaluation_helpers.h"
#include "program.h"

#include <yt/yt/library/query/engine_api/builtin_function_profiler.h>
#include <yt/yt/library/query/engine_api/column_evaluator.h>

#include <yt/yt/library/query/base/query_preparer.h>

#include <algorithm>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

TColumnEvaluatorPtr TColumnEvaluator::Create(
    const TTableSchemaPtr& schema,
    const TConstTypeInferrerMapPtr& typeInferrers,
    const TConstFunctionProfilerMapPtr& profilers)
{
    if (profilers && !profilers->empty()) {
        THROW_ERROR_EXCEPTION("Portable column evaluator does not support custom function profilers");
    }

    std::vector<TColumn> columns(schema->GetColumnCount());
    std::vector<bool> isAggregate(schema->GetColumnCount());

    for (int index = 0; index < schema->GetColumnCount(); ++index) {
        const auto& columnSchema = schema->Columns()[index];
        if (columnSchema.Aggregate()) {
            THROW_ERROR_EXCEPTION("Portable column evaluator does not support aggregate column %Qv",
                columnSchema.Name());
        }

        auto& column = columns[index];
        if (columnSchema.Expression()) {
            try {
                THashSet<std::string> references;
                column.Expression = PrepareExpression(
                    *columnSchema.Expression(),
                    *schema,
                    typeInferrers,
                    &references);
                column.EvaluatorImage = NPortable::CreateExpressionImage(NPortable::CompileExpression(
                    column.Expression,
                    *schema,
                    NPortable::GetBuiltinExpressionRegistry()));
                column.EvaluatorInstance = column.EvaluatorImage.Instantiate();

                for (const auto& reference : references) {
                    column.ReferenceIds.push_back(schema->GetColumnIndexOrThrow(reference));
                }
                std::ranges::sort(column.ReferenceIds);
            } catch (const std::exception& ex) {
                THROW_ERROR_EXCEPTION("Cannot prepare portable expression for column %Qv",
                    columnSchema.Name())
                    .With("column", columnSchema.Name())
                    .With("expression", *columnSchema.Expression())
                    .With(ex);
            }
        }

        // Initialize lazy literal storage before sharing the evaluator.
        column.Variables.GetLiteralValues();
    }

    return New<TColumnEvaluator>(std::move(columns), std::move(isAggregate));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
