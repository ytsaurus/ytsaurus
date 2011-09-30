#include "value.h"
namespace NYT {

////////////////////////////////////////////////////////////////////////////////

int ValueCompare(const TValue& a, const TValue& b)
{
    size_t sizeA = a.GetSize();
    size_t sizeB = b.GetSize();
    size_t min = Min(sizeA, sizeB);

    if (min) {
        int c = memcmp(a.GetData(), b.GetData(), min);
        if (c)
            return c;
    }
    return (int)sizeA - (int)sizeB;
}

bool operator==(const TValue& a, const TValue& b)
{
    return ValueCompare(a, b) == 0;
}

bool operator!=(const TValue& a, const TValue& b)
{
    return ValueCompare(a, b) == 0;
}

bool operator<(const TValue& a, const TValue& b)
{
    return ValueCompare(a, b) < 0;
}

bool operator>(const TValue& a, const TValue& b)
{
    return ValueCompare(a, b) > 0;
}

bool operator<=(const TValue& a, const TValue& b)
{
    return ValueCompare(a, b) <= 0;
}

bool operator>=(const TValue& a, const TValue& b)
{
    return ValueCompare(a, b) >= 0;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
