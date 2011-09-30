#pragma once

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! Array of bytes. The only data type stored inside tables (column names and values)

class TValue
{
public:

    TValue(const TSharedRef& data)
        : Data(data)
    {}

    //! Data is swapped out to TValue
    TValue(TBlob& data)
        : Data(data)
    { }

    const char* GetData() const { return Data.Begin(); }
    size_t GetSize() const { return Data.Size(); }

    const char* Begin() const { return Data.Begin(); }
    const char* End() const { return Data.End(); }

    bool IsEmpty() const { return (Data != NULL) && (Size == 0); }
    bool IsNull() const { return Data.Begin() == NULL; }

    static TValue Null() 
    {
        TBlob blob(0);
        TRef ref;
        TSharedRef data(blob, ref);
        return TValue(data);
    }

    Stroka AsString() const { return Stroka(Data.Begin(), Data.End()); }

private:
    const TSharedRef Data;
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
    TBlob blob(&data, &data + sizeof(T));
    return TValue(blob);
}

template <>
TValue Value(const Stroka& data)
{
    TBlob blob(~data, ~data + data.Length());
    return TValue(blob);
}

////////////////////////////////////////////////////////////////////////////////

typedef yhash_map<TValue, TValue> TTableRow;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

////////////////////////////////////////////////////////////////////////////////

template<>
struct THash<NYT::TValue>
{
    size_t operator()(const NYT::TValue& value) const
    {
        return MurmurHash<ui32>(value.GetData(), value.GetSize());
    }
};

