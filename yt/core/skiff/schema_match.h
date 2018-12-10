#pragma once

#include "public.h"
#include "skiff_schema.h"

namespace NYT {
namespace NSkiff {

////////////////////////////////////////////////////////////////////////////////

extern const TString SparseColumnsName;
extern const TString OtherColumnsName;
extern const TString KeySwitchColumnName;

////////////////////////////////////////////////////////////////////////////////

struct TDenseFieldDescription
{
    TString Name;
    NSkiff::TSkiffSchemaPtr DeoptionalizedSchema;
    bool Required = false;

    TDenseFieldDescription(TString name, const NSkiff::TSkiffSchemaPtr& deoptionalizedSchema, bool isRequired);
};

////////////////////////////////////////////////////////////////////////////////

struct TSparseFieldDescription
{
    TString Name;
    NSkiff::TSkiffSchemaPtr DeoptionalizedSchema;

    TSparseFieldDescription(TString name, const NSkiff::TSkiffSchemaPtr& deoptionalizedSchema);
};

////////////////////////////////////////////////////////////////////////////////

struct TSkiffTableDescription
{
    // Dense fields of the row.
    std::vector<TDenseFieldDescription> DenseFieldDescriptionList;

    // Sparse fields of the row.
    std::vector<TSparseFieldDescription> SparseFieldDescriptionList;

    // Indexes of $key_switch/$row_index/$range_index field inside dense part of the row.
    std::optional<size_t> KeySwitchFieldIndex;
    std::optional<size_t> RowIndexFieldIndex;
    std::optional<size_t> RangeIndexFieldIndex;

    // Whether or not row contains $other_columns field.
    bool HasOtherColumns = false;
};

////////////////////////////////////////////////////////////////////////////////

struct TSkiffTableColumnIds
{
    std::vector<ui16> DenseFieldColumnIds;
    std::vector<ui16> SparseFieldColumnIds;
};

////////////////////////////////////////////////////////////////////////////////

std::vector<TSkiffTableDescription> CreateTableDescriptionList(
    const std::vector<NSkiff::TSkiffSchemaPtr>& skiffSchema,
    const TString& rangeIndexColumnName,
    const TString& rowIndexColumnName);

std::vector<NSkiff::TSkiffSchemaPtr> ParseSkiffSchemas(
    const NYTree::IMapNodePtr& skiffSchemaRegistry,
    const NYTree::IListNodePtr& tableSkiffSchemas);

////////////////////////////////////////////////////////////////////////////////

} // namespace NSkiff
} // namespace NYT
