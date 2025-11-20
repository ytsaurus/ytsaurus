#pragma once

#include <yt/cpp/mapreduce/interface/common.h>

#include <contrib/libs/apache/arrow_next/cpp/src/arrow/api.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TTableSchema CreateYTTableSchemaFromArrowSchema(const std::shared_ptr<arrow20::Schema>& arrowSchema);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
