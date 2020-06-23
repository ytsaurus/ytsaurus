#pragma once

#ifndef SKIFF_SCHEMA_H
#error "Direct inclusion of this file is not allowed, include skiff_schema.h"
#endif
#undef SKIFF_SCHEMA_H

namespace NSkiff {

////////////////////////////////////////////////////////////////////////////////

inline bool IsSimpleType(EWireType type)
{
    switch (type) {
        case EWireType::Yson32:
        case EWireType::Int64:
        case EWireType::Uint64:
        case EWireType::Double:
        case EWireType::Boolean:
        case EWireType::String32:
        case EWireType::Nothing:
            return true;
        case EWireType::Tuple:
        case EWireType::Variant8:
        case EWireType::Variant16:
        case EWireType::RepeatedVariant8:
        case EWireType::RepeatedVariant16:
            return false;
    }
    Y_FAIL("Unknown EWireType %d", static_cast<int>(type));
}

////////////////////////////////////////////////////////////////////////////////

template <EWireType WireType>
TComplexSchema<WireType>::TComplexSchema(TSkiffSchemaList elements)
    : TSkiffSchema(WireType)
    , Elements_(elements)
{ }

template <EWireType WireType>
const TSkiffSchemaList& TComplexSchema<WireType>::GetChildren() const
{
    return Elements_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NSkiff
