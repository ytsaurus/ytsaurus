#pragma once

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

template <class TValue>
TValue MakeSentinelValue(EValueType type, int id = 0)
{
    TValue result;
    result.Id = id;
    result.Type = type;
    return result;
}

template <class TValue>
TValue MakeIntegerValue(i64 value, int id = 0)
{
    TValue result;
    result.Id = id;
    result.Type = EValueType::Integer;
    result.Data.Integer = value;
    return result;
}

template <class TValue>
TValue MakeDoubleValue(double value, int id = 0)
{
    TValue result;
    result.Id = id;
    result.Type = EValueType::Double;
    result.Data.Double = value;
    return result;
}

template <class TValue>
TValue MakeStringValue(const TStringBuf& value, int id = 0)
{
    TValue result;
    result.Id = id;
    result.Type = EValueType::String;
    result.Length = value.length();
    result.Data.String = value.begin();
    return result;
}

template <class TValue>
TValue MakeAnyValue(const TStringBuf& value, int id = 0)
{
    TValue result;
    result.Id = id;
    result.Type = EValueType::Any;
    result.Length = value.length();
    result.Data.String = value.begin();
    return result;
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
int GetKeyComparerValueCount(const T&, int prefixLength)
{
    return prefixLength;
}

//! Provides a comparer functor for row-like entities
//! trimmed to a given length.
class TKeyComparer
{
public:
    explicit TKeyComparer(int prefixLength)
        : PrefixLength_(prefixLength)
    { }

    template <class TLhs, class TRhs>
    int operator () (TLhs lhs, TRhs rhs) const
    {
        int lhsLength = GetKeyComparerValueCount(lhs, PrefixLength_);
        int rhsLength = GetKeyComparerValueCount(rhs, PrefixLength_);
        int minLength = std::min(lhsLength, rhsLength);
        for (int index = 0; index < minLength; ++index) {
            int result = CompareRowValues(lhs[index], rhs[index]);
            if (result != 0) {
                return result;
            }
        }
        return lhsLength - rhsLength;
    }

private:
    int PrefixLength_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
