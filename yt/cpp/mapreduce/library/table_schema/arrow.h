#pragma once

#include <yt/yt/library/arrow_adapter/public.h>

#include <yt/cpp/mapreduce/interface/common.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/api.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TTableSchema CreateYTTableSchemaFromArrowSchema(const NArrow::TArrowSchemaPtr& arrowSchema);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
