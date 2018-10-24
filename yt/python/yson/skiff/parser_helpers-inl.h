#pragma once
#ifndef PARSER_HELPERS_INL_H_
#error "Direct inclusion of this file is not allowed, include parser_helpers.h"
// For the sake of sane code completion.
#include "parser_helpers.h"
#endif

#include "parser_helpers.h"

namespace NYT {
namespace NPython {

////////////////////////////////////////////////////////////////////////////////

template <class TConsumer>
std::unique_ptr<NSkiff::TSkiffMultiTableParser<TConsumer>> CreateSkiffMultiTableParser(
    TConsumer* consumer,
    const std::vector<Py::PythonClassObject<TSkiffSchemaPython>>& pythonSkiffSchemaList,
    const TString& rangeIndexColumnName,
    const TString& rowIndexColumnName)
{
    NSkiff::TSkiffSchemaList skiffSchemaList;
    std::vector<NSkiff::TSkiffTableColumnIds> tablesColumnIds;
    for (auto schema : pythonSkiffSchemaList) {
        auto skiffSchema = schema.getCxxObject()->GetSchemaObject();
        skiffSchemaList.push_back(skiffSchema->GetSkiffSchema());

        NSkiff::TSkiffTableColumnIds tableColumnIds;
        tableColumnIds.DenseFieldColumnIds.assign(skiffSchema->GetDenseFieldsCount(), 0);
        std::iota(tableColumnIds.DenseFieldColumnIds.begin(), tableColumnIds.DenseFieldColumnIds.end(), 0);

        tableColumnIds.SparseFieldColumnIds.assign(skiffSchema->GetSparseFieldsCount(), skiffSchema->GetDenseFieldsCount());
        std::iota(tableColumnIds.SparseFieldColumnIds.begin(), tableColumnIds.SparseFieldColumnIds.end(), skiffSchema->GetDenseFieldsCount());
        tablesColumnIds.push_back(tableColumnIds);
    }

    return std::make_unique<NSkiff::TSkiffMultiTableParser<TConsumer>>(
        consumer,
        skiffSchemaList,
        tablesColumnIds,
        rangeIndexColumnName,
        rowIndexColumnName);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NPython
} // namespace NYT
