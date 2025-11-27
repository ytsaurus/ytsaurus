#pragma once

#include <yt/yt/client/table_client/public.h>

#include <contrib/libs/apache/arrow_next/cpp/src/arrow/type.h>

namespace NYT::NArrow {

////////////////////////////////////////////////////////////////////////////////

// NB: Keep in sync with `CreateYTTableSchemaFromArrowSchema`.
// For now this is only used in `schemaless_block_generator.cpp`, so it only needs to support types returned from `CreateYTTableSchemaFromArrowSchema`.
arrow20::Schema CreateArrowSchemaFromYTTableSchema(
    const NTableClient::TTableSchema& tableSchema);

// NB: Keep in sync with `CreateArrowSchemaFromYTTableSchema`.
NTableClient::TTableSchemaPtr CreateYTTableSchemaFromArrowSchema(
    const std::shared_ptr<arrow20::Schema>& arrowSchema);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NArrow
