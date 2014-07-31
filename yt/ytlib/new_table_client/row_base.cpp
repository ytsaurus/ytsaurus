#include "stdafx.h"
#include "row_base.h"

#include <core/misc/string.h>
#include <core/misc/error.h>

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

void ValidateDataValueType(EValueType type)
{
	// TODO(babenko): handle any
 	if (type != EValueType::Int64 &&
 	    type != EValueType::Double &&
 	    type != EValueType::Boolean &&
 	    type != EValueType::String &&
 	    type != EValueType::Null)
    {
        THROW_ERROR_EXCEPTION("Invalid date value type %Qv", type);
    }       
}

void ValidateKeyValueType(EValueType type)
{
	// TODO(babenko): handle any
 	if (type != EValueType::Int64 &&
 	    type != EValueType::Double &&
 	    type != EValueType::Boolean &&
 	    type != EValueType::String &&
 	    type != EValueType::Null &&
 	    type != EValueType::Min &&
 	    type != EValueType::Max)
    {
        THROW_ERROR_EXCEPTION("Invalid key value type %Qv", type);
    }       
}

void ValidateSchemaValueType(EValueType type)
{
	// TODO(babenko): handle any
 	if (type != EValueType::Int64 &&
 	    type != EValueType::Double &&
 	    type != EValueType::Boolean &&
 	    type != EValueType::String)
    {
        THROW_ERROR_EXCEPTION("Invalid schema value type %Qv", type);
    }       
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
