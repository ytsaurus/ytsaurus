#include "user_indexes.h"

namespace NYT
{

size_t TUserIndexes::GetSize() const
{
    return Array.size();
}

TValue TUserIndexes::operator[](size_t idx) const
{
    YASSERT(idx < GetSize());
    return Array[idx];
}

size_t TUserIndexes::operator[](const TValue& value)
{
    auto i = Map.find(value);
    if (i == Map.end()) {
        size_t idx = GetSize();
        Map[value] = idx;
        Array.push_back();
        i = Map.find(value);
        Array.back() = i->first;
        return idx;
    } else
        return i->second;
}

}
