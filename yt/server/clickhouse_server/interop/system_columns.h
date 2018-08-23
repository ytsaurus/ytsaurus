#pragma once

#include "table_schema.h"

#include <util/generic/maybe.h>

namespace NInterop {

////////////////////////////////////////////////////////////////////////////////

struct TSystemColumns
{
    TMaybe<TString> TableName;

    size_t GetCount() const
    {
        return TableName.Defined() ? 1 : 0;
    }

    TColumnList ToColumnList() const;
};

}   // namespace NInterop
