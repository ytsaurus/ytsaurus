#ifndef PARSER_HELPERS_INL_H_
#error "Direct inclusion of this file is not allowed, include parser_helpers.h"
// For the sake of sane code completion.
#include "parser_helpers.h"
#endif

#include "parser_helpers.h"

namespace NYT::NPython {

////////////////////////////////////////////////////////////////////////////////

template <class TConsumer>
std::unique_ptr<NSkiffExt::TSkiffMultiTableParser<TConsumer>> CreateSkiffMultiTableParser(
    TConsumer* consumer,
    const std::vector<Py::PythonClassObject<TSkiffSchemaPython>>& pythonSkiffSchemaList,
    const TString& rangeIndexColumnName,
    const TString& rowIndexColumnName)
{
    NSkiff::TSkiffSchemaList skiffSchemaList;
    std::vector<NSkiffExt::TSkiffTableColumnIds> tablesColumnIds;
    for (auto schema : pythonSkiffSchemaList) {
        auto skiffSchema = schema.getCxxObject()->GetSchemaObject();
        skiffSchemaList.push_back(skiffSchema->GetSkiffSchema());

        NSkiffExt::TSkiffTableColumnIds tableColumnIds;
        tableColumnIds.DenseFieldColumnIds.assign(skiffSchema->GetDenseFieldsCount(), 0);
        std::iota(tableColumnIds.DenseFieldColumnIds.begin(), tableColumnIds.DenseFieldColumnIds.end(), 0);

        tableColumnIds.SparseFieldColumnIds.assign(skiffSchema->GetSparseFieldsCount(), skiffSchema->GetDenseFieldsCount());
        std::iota(tableColumnIds.SparseFieldColumnIds.begin(), tableColumnIds.SparseFieldColumnIds.end(), skiffSchema->GetDenseFieldsCount());
        tablesColumnIds.push_back(tableColumnIds);
    }

    return std::make_unique<NSkiffExt::TSkiffMultiTableParser<TConsumer>>(
        consumer,
        skiffSchemaList,
        tablesColumnIds,
        rangeIndexColumnName,
        rowIndexColumnName);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPython
