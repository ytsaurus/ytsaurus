#pragma once

#include "types.h"

namespace NYT 
{

class TUserIndexes 
{
    yhash_map<THValue, size_t> Map;
    yvector<TValue> Array; // Values point to THValues in Map

public:
    TValue operator[](size_t idx) const;
    size_t operator[](const TValue& value);
    size_t GetSize() const;
};

}
