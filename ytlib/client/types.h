#pragma once

#include "../misc/common.h"
#include "../misc/ptr.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////
// value (reference type)
// array of bytes
// null: Data == NULL
// empty: Data == 1, Size == 0
// several special values (later): Data == 2, 3, ..., Size == 0
// non-empty: Data != NULL, Size != 0

class TValue
{
protected:

    const char* Data;
    size_t Size;

public:

    TValue()
        : Data(NULL)
        , Size(0)
    {}

    TValue(const char* data)
        : Data(data)
        , Size(strlen(data))
    {}

    TValue(const void* data, size_t size)
        : Data((const char*)data)
        , Size(size)
    {}

    const char* GetData() const { return Data; }
    size_t GetSize() const { return Size; }

    const char* Begin() const { return Data; }
    const char* End() const { return Data + Size; }

    bool IsSet() const { return Size; }

    bool IsEmpty() const { return Data == (const char*)1 && !Size; }
    static TValue Empty() { return TValue((const char*)1, 0); }

    bool IsNull() const { return !Data; }
    static TValue Null() { return TValue(); }

    Stroka AsString() const { return Stroka(Begin(), End()); }
};

bool operator==(const TValue& a, const TValue& b);
bool operator!=(const TValue& a, const TValue& b);
bool operator< (const TValue& a, const TValue& b);
bool operator> (const TValue& a, const TValue& b);
bool operator<=(const TValue& a, const TValue& b);
bool operator>=(const TValue& a, const TValue& b);

// construct from POD types
template <class T>
TValue Value(const T& data)
{
    return TValue(&data, sizeof(T));
}

////////////////////////////////////////////////////////////////////////////////
// value (holder type)
// can be only empty or set

class THValue
    : public TValue
{
    TBlob Holder;

public:

    THValue();
    THValue(const THValue& value);
    THValue(const TValue& value);
    THValue& operator=(const THValue& value);
    THValue& operator=(const TValue& value);
};

////////////////////////////////////////////////////////////////////////////////
// row key (reference type)
// array of reference values

class TKey
    : public yvector<TValue>
{
public:
    
    TKey()
    {}

    bool IsSet() const
    {
        for (size_t i = 0; i < size(); ++i)
            if ((*this)[i].IsSet())
                return true;
        return false;
    }

    TKey& operator()(const TValue& value)
    {
        push_back(value);
        return *this;
    }
};

bool operator==(const TKey& a, const TKey& b);
bool operator!=(const TKey& a, const TKey& b);
bool operator< (const TKey& a, const TKey& b);
bool operator> (const TKey& a, const TKey& b);
bool operator<=(const TKey& a, const TKey& b);
bool operator>=(const TKey& a, const TKey& b);

////////////////////////////////////////////////////////////////////////////////
// row key (holder type)

class THKey
    : public yvector<THValue>
{
public:

    THKey()
    {}

    THKey(const TKey& key)
    {
        *this = key;
    }

    THKey& operator=(const TKey& key)
    {
        yresize(key.size());
        for (size_t i = 0; i < key.size(); ++i)
            (*this)[i] = key[i];
        return *this;
    }

    bool IsSet() const
    {
        for (size_t i = 0; i < size(); ++i)
            if ((*this)[i].IsSet())
                return true;
        return false;
    }

    THKey& operator()(const TValue& value)
    {
        push_back(value);
        return *this;
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class T>
class TRangeOf
{
    T BeginK;
    T EndK;

public:

    TRangeOf(const T& begin, const T& end, bool closedEnd = false)
        : BeginK(begin)
        , EndK(end)
    {
        if (closedEnd)
            EndK = MakeOpenFromClosed(end);
    }

    const T& Begin() const { return BeginK; }
    const T& End() const { return EndK; }

    bool Match(const T& val) const
    {
        if (BeginK.IsSet() && val < BeginK)
            return false;

        if (EndK.IsSet() && val >= EndK)
            return false;

        return true;
    }

    bool Intersects(const TRangeOf& range) const
    {
        if ((BeginK <= range.BeginK && (!EndK.IsSet() || range.BeginK < EndK)) || 
            (BeginK < range.EndK && (!EndK.IsSet() || range.EndK <= EndK)) ||
            (range.BeginK <= BeginK && (!range.EndK.IsSet() || BeginK < range.EndK)))
            return true;
        else
            return false;
    }

    void Unite(const TRangeOf& range)
    {
        YASSERT(Intersects(range));

        if (range.BeginK < BeginK)
            BeginK = range.BeginK;
        if (EndK.IsSet() && (!range.EndK.IsSet() || (range.EndK > EndK)))
            EndK = range.EndK;
    }
};

////////////////////////////////////////////////////////////////////////////////
// range of column names (holder type only)

typedef TRangeOf<THValue> TRange;

THValue MakeOpenFromClosed(const THValue& value);

TRange Range(const TValue& begin, const TValue& end, bool closedEnd = false);
TRange RayRange(const TValue& begin = TValue());
TRange PrefixRange(const TValue& prefix);

////////////////////////////////////////////////////////////////////////////////
// range of row keys (holder type only)

typedef TRangeOf<THKey> TKeyRange;

THKey MakeOpenFromClosed(const THKey& key);

TKeyRange KeyRange(const TKey& begin, const TKey& end, bool closedEnd = false);
TKeyRange PrefixKeyRange(const TKey& prefix);

////////////////////////////////////////////////////////////////////////////////
// channel: type used for schema descriptions
// set of stand-alone columns and column ranges

class TChannel
{
public:

    yvector<THValue> Columns;
    yvector<TRange> Ranges;
    
    bool IsEmpty() const;

    bool Match(const TValue& column) const;
    bool Match(const TRange& range) const;
    bool Match(const TChannel& channel) const;

    bool Intersect(const TRange& range) const;
    bool Intersect(const TChannel& channel) const;

    TChannel& operator()(const TValue& column);
    TChannel& operator()(const TRange& range);

    TChannel& operator-= (const TChannel& channel);
    const TChannel operator- (const TChannel& channel);
};

////////////////////////////////////////////////////////////////////////////////

}

template<>
struct THash<NYT::THValue>
{
    size_t operator()(const NYT::THValue& value) const
    {
        return MurmurHash<ui32>(value.GetData(), value.GetSize());
    }
};

