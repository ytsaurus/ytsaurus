#pragma once

#include <library/cpp/skiff/skiff_schema.h>

////////////////////////////////////////////////////////////////////////////////

namespace NSkiff {

////////////////////////////////////////////////////////////////////////////////

using TSkiffSchemaPtr = std::shared_ptr<TSkiffSchema>;

using TSimpleTypeSchemaPtr = std::shared_ptr<TSimpleTypeSchema>;

using TTupleSchemaPtr = std::shared_ptr<TTupleSchema>;
using TVariant8SchemaPtr = std::shared_ptr<TVariant8Schema>;
using TVariant16SchemaPtr = std::shared_ptr<TVariant16Schema>;
using TRepeatedVariant8SchemaPtr = std::shared_ptr<TRepeatedVariant8Schema>;
using TRepeatedVariant16SchemaPtr = std::shared_ptr<TRepeatedVariant16Schema>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NSkiff
