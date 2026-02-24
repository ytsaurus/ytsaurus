#include <yt/yt/library/query/engine_api/row_level_security.h>

#include <yt/yt/library/query/engine/folding_profiler.h>

#include <yt/yt/library/query/base/query.h>
#include <yt/yt/library/query/base/query_preparer.h>

namespace NYT::NQueryClient {

using namespace NTableClient;
namespace {

////////////////////////////////////////////////////////////////////////////////

struct TCGInstanceHolder final : public ICGInstanceHolder
{
    TCGExpressionInstance Instance;
    TCGVariables Variables;
    TCGExpressionImage Image;

    void Run(
        TUnversionedValue* value,
        TUnversionedRow schemafulRow,
        const TRowBufferPtr& rowBuffer) override
    {
        Instance.Run(
            Variables.GetLiteralValues(),
            Variables.GetOpaqueData(),
            Variables.GetOpaqueDataSizes(),
            value,
            schemafulRow.Elements(),
            rowBuffer);
    }
};

////////////////////////////////////////////////////////////////////////////////

}


std::pair<ICGInstanceHolderPtr, NTableClient::TTableSchemaPtr> MakeRlsCGInstance(const NTableClient::TTableSchemaPtr& schema, const std::string& predicate) {
    YT_VERIFY(schema);

    THashSet<std::string> references;
    auto preparedExpression = PrepareExpression(
        predicate,
        *schema,
        GetBuiltinTypeInferrers(),
        &references);

    // Drop unused columns from the schema.
    std::vector<TColumnSchema> columns;
    columns.reserve(references.size());
    for (const auto& column : schema->Columns()) {
        if (references.contains(column.Name())) {
            columns.push_back(column);
        }
    }
    auto adjustedSchema = New<TTableSchema>(std::move(columns));

    TCGVariables variables;

    auto profiler = Profile(
        preparedExpression,
        adjustedSchema,
        /*id*/ nullptr,
        &variables,
        /*useCanonicalNullRelations*/ false,
        NYT::NCodegen::EExecutionBackend::Native,
        GetBuiltinFunctionProfilers());

    auto image = profiler();
    auto instance = image.Instantiate();
    auto holder = New<TCGInstanceHolder>(
        std::move(instance),
        std::move(variables),
        std::move(image));
    return {holder, adjustedSchema};
}

}
