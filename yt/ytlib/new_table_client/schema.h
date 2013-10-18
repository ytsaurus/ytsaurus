#pragma once

#include "public.h"

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

struct TColumnSchema
{
    Stroka Name;
    EColumnType Type;
};

struct TTableSchema
{
    std::vector<TColumnSchema> Columns;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT

