#pragma once

#include "public.h"

#include <yt/core/skiff/skiff_schema.h>

#include <yt/ytlib/table_client/schema.h>

namespace NYT {
namespace NFormats {

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
    TNullable<size_t> KeySwitchFieldIndex;
    TNullable<size_t> RowIndexFieldIndex;
    TNullable<size_t> RangeIndexFieldIndex;

    // Whether or not row contains $other_columns field.
    bool HasOtherColumns = false;
};

////////////////////////////////////////////////////////////////////////////////

std::vector<TSkiffTableDescription> CreateTableDescriptionList(const std::vector<NSkiff::TSkiffSchemaPtr>& skiffSchema);

std::vector<NSkiff::TSkiffSchemaPtr> ParseSkiffSchemas(const TSkiffFormatConfigPtr& config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
