#pragma once

#include <contrib/libs/apache/arrow/cpp/src/arrow/io/interfaces.h>

namespace NYT::NArrow {

////////////////////////////////////////////////////////////////////////////////

using TArrowSchemaPtr = std::shared_ptr<arrow::Schema>;
using TArrowRandomAccessFilePtr = std::shared_ptr<arrow::io::RandomAccessFile>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NArrow
