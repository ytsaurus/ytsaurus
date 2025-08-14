#pragma once

#include <contrib/libs/apache/arrow_next/cpp/src/arrow/io/interfaces.h>

namespace NYT::NArrow {

////////////////////////////////////////////////////////////////////////////////

using TArrowSchemaPtr = std::shared_ptr<arrow20::Schema>;
using TArrowRandomAccessFilePtr = std::shared_ptr<arrow20::io::RandomAccessFile>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NArrow
