#include "value.h"

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

TValue::TValue(TRef data)
    : Data(data)
{ }

TValue::TValue(Stroka data)
    : Data(data.begin(), data.Size())
{ }

TValue::TValue()
    : Data(NULL, 0)
{ }

const char* TValue::GetData() const
{
    return Data.Begin();
}

size_t TValue::GetSize() const
{
    return Data.Size();
}

const char* TValue::Begin() const
{
    return Data.Begin();
}

const char* TValue::End() const
{
    return Data.End();
}

bool TValue::IsEmpty() const
{
    return (Data.Begin() != NULL) && (Data.Size() == 0);
}

bool TValue::IsNull() const
{
    return Data.Begin() == NULL;
}

Stroka TValue::ToString() const
{
    return Stroka(Data.Begin(), Data.End());
}

TBlob TValue::ToBlob() const
{
    return Data.ToBlob();
}

////////////////////////////////////////////////////////////////////////////////

int CompareValue(const TValue& lhs, const TValue& rhs)
{
    if (lhs.IsNull()) {
        if (rhs.IsNull()) {
            return 0;
        } else {
            return 1;
        }
    } else if (rhs.IsNull()) {
        return -1;
    }

    size_t lhsSize = lhs.GetSize();
    size_t rhsSize = rhs.GetSize();
    size_t min = Min(lhsSize, rhsSize);

    if (min > 0) {
        int result = memcmp(lhs.GetData(), rhs.GetData(), min);
        if (result != 0)
            return result;
    }

    return (int)lhsSize - (int)rhsSize;
}

bool operator==(const TValue& lhs, const TValue& rhs)
{
    return CompareValue(lhs, rhs) == 0;
}

bool operator!=(const TValue& lhs, const TValue& rhs)
{
    return CompareValue(lhs, rhs) == 0;
}

bool operator<(const TValue& lhs, const TValue& rhs)
{
    return CompareValue(lhs, rhs) < 0;
}

bool operator>(const TValue& lhs, const TValue& rhs)
{
    return CompareValue(lhs, rhs) > 0;
}

bool operator<=(const TValue& lhs, const TValue& rhs)
{
    return CompareValue(lhs, rhs) <= 0;
}

bool operator>=(const TValue& lhs, const TValue& rhs)
{
    return CompareValue(lhs, rhs) >= 0;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
