#include "types.h"

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

THValue::THValue()
{}

THValue::THValue(const THValue& value)
    : TValue()
{
    *this = value;
}

THValue::THValue(const TValue& value)
    : TValue()
{
    *this = value;
}

THValue& THValue::operator=(const THValue& value)
{
    Holder = value.Holder;
    Data = (const char*)Holder.begin();
    Size = Holder.size();
    return *this;
}

THValue& THValue::operator=(const TValue& value)
{
    size_t size = value.GetSize();
    Holder.yresize(size);
    memcpy(Holder.begin(), value.GetData(), size);
    Data = (const char*)Holder.begin();
    Size = size;
    return *this;
}

////////////////////////////////////////////////////////////////////////////////

THValue MakeOpenFromClosed(const THValue& value)
{
    size_t size = value.GetSize();
    yvector<char> data;
    data.yresize(size + 1);
    memcpy(data.begin(), value.GetData(), size);
    data[size] = 0; // append zero at the end

    THValue result = TValue(data.begin(), data.size());
    return result;
}

TRange Range(const TValue& begin, const TValue& end, bool closedEnd)
{
    YASSERT(begin <= end);
    return TRange(begin, end, closedEnd);
}

TRange RayRange(const TValue& begin)
{
    return TRange(begin, TValue());
}

TRange PrefixRange(const TValue& prefix)
{
    return TRange(prefix, prefix); // TODO
}

////////////////////////////////////////////////////////////////////////////////

THKey MakeOpenFromClosed(const THKey& key)
{
    THKey result = key; // TODO
    return result;
}

TKeyRange KeyRange(const TKey& begin, const TKey& end, bool closedEnd)
{
    return TKeyRange(begin, end, closedEnd);
}

TKeyRange PrefixKeyRange(const TKey& prefix)
{
    return TKeyRange(prefix, prefix); // TODO
}

////////////////////////////////////////////////////////////////////////////////

int KeyCompare(const TKey& a, const TKey& b)
{
    size_t sizeA = a.size();
    size_t sizeB = b.size();
    size_t min = Min(sizeA, sizeB);

    for (size_t i = 0; i < min; ++i) {
        int c = ValueCompare(a[i], b[i]);
        if (c)
            return c;
    }
    return (int)sizeA - (int)sizeB;
}

bool operator==(const TKey& a, const TKey& b)
{
    return KeyCompare(a, b) == 0;
}

bool operator!=(const TKey& a, const TKey& b)
{
    return KeyCompare(a, b) != 0;
}

bool operator<(const TKey& a, const TKey& b)
{
    return KeyCompare(a, b) < 0;
}

bool operator>(const TKey& a, const TKey& b)
{
    return KeyCompare(a, b) > 0;
}

bool operator<=(const TKey& a, const TKey& b)
{
    return KeyCompare(a, b) <= 0;
}

bool operator>=(const TKey& a, const TKey& b)
{
    return KeyCompare(a, b) >= 0;
}

////////////////////////////////////////////////////////////////////////////////

bool TChannel::IsEmpty() const
{
    return Columns.empty() && Ranges.empty();
}

bool TChannel::Match(const TValue& column) const
{
    for (size_t c = 0; c < Columns.size(); ++c) {
        if (Columns[c] == column)
            return true;
    }
    for (size_t r = 0; r < Ranges.size(); ++r) {
        if (Ranges[r].Match(column))
            return true;
    }
    return false;
}



bool TChannel::Match(const TRange& range) const
{
    for (size_t c = 0; c < Columns.size(); ++c) {
        if (range.Match(Columns[c]))
            return true;
    }
    for (size_t r = 0; r < Ranges.size(); ++r) {
        if (range.Intersects(Ranges[r]))
            return true;
    }
    return false;
}

bool TChannel::Match(const TChannel& channel) const
{
    for (size_t c = 0; c < channel.Columns.size(); ++c) {
        if (Match(channel.Columns[c]))
            return true;
    }
    for (size_t r = 0; r < channel.Ranges.size(); ++r) {
        if (Match(channel.Ranges[r]))
            return true;
    }
    return false;
}

TChannel& TChannel::operator()(const TValue& column)
{
    for (size_t c = 0; c < Columns.size(); ++c)
        if (Columns[c] == column)
            return *this;

    Columns.push_back(column);
    return *this;
}

TChannel& TChannel::operator()(const TRange& range)
{
    TRange unionRange(range);
    yvector<TRange> newRanges;
    for (size_t r = 0; r < Ranges.size(); ++r) {
        if (Ranges[r].Intersects(range)) {
            unionRange.Unite(Ranges[r]); 
        } else
            newRanges.push_back(Ranges[r]);
    }
    newRanges.push_back(unionRange);
    Ranges.swap(newRanges);
    return *this;
}

TChannel& TChannel::operator-= (const TChannel& channel)
{ 
    yvector<THValue> newColumns;
    for (size_t c = 0; c < Columns.size(); ++c)
        if (!channel.Match(Columns[c]))
            newColumns.push_back(Columns[c]);
    Columns.swap(newColumns);

    yvector<TRange> newRanges;
    yvector<TRange> rhsRanges(channel.Ranges);
    // artificial ranges with one element
    for (size_t c = 0; c < channel.Columns.size(); ++c)
        rhsRanges.push_back(Range(channel.Columns[c], channel.Columns[c], true));

    for (size_t r = 0; r < rhsRanges.size(); ++r) {
        const TRange& rhsRange = rhsRanges[r];
        for (size_t i = 0; i < Ranges.size(); ++i) {
            TRange &lhsRange = Ranges[i];
            if (!lhsRange.Intersects(rhsRange)) {
                newRanges.push_back(lhsRange);
                continue;
            }

            if (lhsRange.Begin() < rhsRange.Begin())
                newRanges.push_back(Range(lhsRange.Begin(), rhsRange.Begin()));

            if (rhsRange.End().IsSet()) {
                if (!lhsRange.End().IsSet())
                    newRanges.push_back(RayRange(rhsRange.End()));
                else if (lhsRange.End() > rhsRange.End()) 
                    newRanges.push_back(Range(rhsRange.End(), lhsRange.End()));
            }
        }
        Ranges.swap(newRanges);
        newRanges.clear();
    }
    return *this;
}

const TChannel TChannel::operator- (const TChannel& channel)
{
   return (TChannel(*this) -= channel);
}

////////////////////////////////////////////////////////////////////////////////

}
