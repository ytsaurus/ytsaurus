#include "value.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TValue::TValue(const TSharedRef& data)
    : Data(data)
{}

//! Data is swapped out to TValue
TValue::TValue(TBlob& data)
    : Data(data)
{ }

TValue::TValue()
    : Data(TValue::Null().Data)
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

TValue TValue::Null() 
{
    TSharedRef::TBlobPtr blob;
    TRef ref(NULL, 0);
    TSharedRef data(blob, ref);
    return TValue(data);
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

TValue NextValue(const TValue& value)
{
    TBlob blob = value.ToBlob();
    if (blob.back() < 0xFF) {
        ++blob.back();
    } else {
        blob.push_back('\0');
    }

    return TValue(blob);
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

} // namespace NYT
