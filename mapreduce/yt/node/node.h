#pragma once

#include <util/generic/bt_exception.h>
#include <util/generic/hash.h>
#include <util/generic/variant.h>
#include <util/generic/vector.h>
#include <util/generic/yexception.h>
#include <util/string/cast.h>

#include <cmath>

class IInputStream;
class IOutputStream;

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TNode
{
public:
    class TTypeError
        : public TWithBackTrace<yexception>
    { };

    enum EType {
        Undefined = 0   /*"undefined"*/,

        // NOTE: string representation of all node types
        // are compatible with server node type (except `Undefined' which is missing on server).
        String = 1  /*"string_node"*/,
        Int64 = 2   /*"int64_node"*/,
        Uint64 = 3  /*"uint64_node"*/,
        Double = 4  /*"double_node"*/,
        Bool = 5    /*"boolean_node"*/,
        List = 6    /*"list_node"*/,
        Map = 7     /*"map_node"*/,
        Null = 8    /*"null"*/,
    };

    using TListType = TVector<TNode>;
    using TMapType = THashMap<TString, TNode>;

private:
    struct TNull {
        bool operator==(const TNull&) const;
    };

    struct TUndefined {
        bool operator==(const TUndefined&) const;
    };

    using TValue = ::TVariant<
        bool,
        i64,
        ui64,
        double,
        TString,
        TListType,
        TMapType,
        TNull,
        TUndefined
        >;

public:

    TNode();
    TNode(const char* s);
    TNode(const TStringBuf& s);
    TNode(TString s);
    TNode(int i);

    //this case made speccially for prevent mess cast of EType into TNode through TNode(int) constructor
    //usual case of error SomeNode == TNode::Undefined <-- SomeNode indeed will be compared with TNode(0) without this method
    //correct way is SomeNode.GetType() == TNode::Undefined
    template<class T = EType>
    Y_FORCE_INLINE TNode(EType)
    {
        static_assert(!std::is_same<T, EType>::value, "looks like a mistake, may be you forget .GetType()");
    }

    //this case made speccially for prevent mess cast of T* into TNode through implicit bool ctr
    template<class T = int>
    Y_FORCE_INLINE TNode(const T*) : TNode() {
        static_assert(!std::is_same<T,T>::value, "looks like a mistake, and pointer have converted to bool");
    }

    TNode(unsigned int ui);
    TNode(long i);
    TNode(unsigned long ui);
    TNode(long long i);
    TNode(unsigned long long ui);
    TNode(double d);
    TNode(bool b);
    TNode(TMapType map);

    TNode(const TNode& rhs);
    TNode& operator=(const TNode& rhs);

    TNode(TNode&& rhs);
    TNode& operator=(TNode&& rhs);

    ~TNode();

    void Clear();

    bool IsString() const;
    bool IsInt64() const;
    bool IsUint64() const;
    bool IsDouble() const;
    bool IsBool() const;
    bool IsList() const;
    bool IsMap() const;

    // `IsEntity' is deprecated use `IsNull' instead.
    bool IsEntity() const;
    bool IsNull() const;
    bool IsUndefined() const;

    template<typename T>
    bool IsOfType() const noexcept;

    bool Empty() const;
    size_t Size() const;

    EType GetType() const;

    const TString& AsString() const;
    i64 AsInt64() const;
    ui64 AsUint64() const;
    double AsDouble() const;
    bool AsBool() const;
    const TListType& AsList() const;
    const TMapType& AsMap() const;
    TListType& AsList();
    TMapType& AsMap();

    const TString& UncheckedAsString() const noexcept;
    i64 UncheckedAsInt64() const noexcept;
    ui64 UncheckedAsUint64() const noexcept;
    double UncheckedAsDouble() const noexcept;
    bool UncheckedAsBool() const noexcept;
    const TListType& UncheckedAsList() const noexcept;
    const TMapType& UncheckedAsMap() const noexcept;
    TListType& UncheckedAsList() noexcept;
    TMapType& UncheckedAsMap() noexcept;

    // ui64 <-> i64
    // makes overflow checks
    template<typename T>
    T IntCast() const;

    // ui64 <-> i64 <-> double <-> string
    // makes overflow checks
    template<typename T>
    T ConvertTo() const;

    template<typename T>
    T& As();

    template<typename T>
    const T& As() const;

    static TNode CreateList();
    static TNode CreateMap();
    static TNode CreateEntity();

    const TNode& operator[](size_t index) const;
    TNode& operator[](size_t index);

    TNode& Add() &;
    TNode Add() &&;
    TNode& Add(const TNode& node) &;
    TNode Add(const TNode& node) &&;
    TNode& Add(TNode&& node) &;
    TNode Add(TNode&& node) &&;

    bool HasKey(const TStringBuf key) const;

    TNode& operator()(const TString& key, const TNode& value) &;
    TNode operator()(const TString& key, const TNode& value) &&;
    TNode& operator()(const TString& key, TNode&& value) &;
    TNode operator()(const TString& key, TNode&& value) &&;

    const TNode& operator[](const TStringBuf key) const;
    TNode& operator[](const TStringBuf key);
    const TNode& At(const TStringBuf key) const;

    // attributes
    bool HasAttributes() const;
    void ClearAttributes();
    const TNode& GetAttributes() const;
    TNode& Attributes();

    void MoveWithoutAttributes(TNode&& rhs);

    // Serialize TNode using binary yson format.
    // Methods for ysaveload.
    void Save(IOutputStream* output) const;
    void Load(IInputStream* input);

private:
    void Copy(const TNode& rhs);
    void Move(TNode&& rhs);

    void CheckType(EType type) const;

    void AssureMap();
    void AssureList();

    void CreateAttributes();

private:
    TValue Value_;
    THolder<TNode> Attributes_;

    friend bool operator==(const TNode& lhs, const TNode& rhs);
    friend bool operator!=(const TNode& lhs, const TNode& rhs);
};

bool operator==(const TNode& lhs, const TNode& rhs);
bool operator!=(const TNode& lhs, const TNode& rhs);

bool GetBool(const TNode& node);

template<typename T>
inline T TNode::IntCast() const {
    static_assert(sizeof(T) != sizeof(T), "implemented only for ui64/i64");
}

template<>
inline ui64 TNode::IntCast<ui64>() const {
    switch (GetType()) {
        case TNode::Uint64:
            return AsUint64();
        case TNode::Int64:
            if (AsInt64() < 0) {
                ythrow TTypeError() << AsInt64() << " can't be converted to ui64";
            }
            return AsInt64();
        default:
            ythrow TTypeError() << "IntCast() called for type " << GetType();
    }
}

template<>
inline i64 TNode::IntCast<i64>() const {
    switch (GetType()) {
        case TNode::Uint64:
            if (AsUint64() > (ui64)std::numeric_limits<i64>::max()) {
                ythrow TTypeError() << AsUint64() << " can't be converted to i64";
            }
            return AsUint64();
        case TNode::Int64:
            return AsInt64();
        default:
            ythrow TTypeError() << "IntCast() called for type " << GetType();
    }
}

template<typename T>
inline T TNode::ConvertTo() const {
    static_assert(sizeof(T) != sizeof(T), "should have template specialization");
}

template<>
inline TString TNode::ConvertTo<TString>() const {
    switch (GetType()) {
        case NYT::TNode::String:
            return AsString();
        case NYT::TNode::Int64:
            return ::ToString(AsInt64());
        case NYT::TNode::Uint64:
            return ::ToString(AsUint64());
        case NYT::TNode::Double:
            return ::ToString(AsDouble());
        case NYT::TNode::Bool:
            return ::ToString(AsBool());
        case NYT::TNode::List:
        case NYT::TNode::Map:
        case NYT::TNode::Null:
        case NYT::TNode::Undefined:
            ythrow TTypeError() << "ConvertTo<TString>() called for type " << GetType();
    }
}

template<>
inline ui64 TNode::ConvertTo<ui64>() const {
    switch (GetType()) {
        case NYT::TNode::String:
            return ::FromString(AsString());
        case NYT::TNode::Int64:
        case NYT::TNode::Uint64:
            return IntCast<ui64>();
        case NYT::TNode::Double:
            // >= because of (1<<64) + 1
            if (AsDouble() < std::numeric_limits<ui64>::min() || AsDouble() >= std::numeric_limits<ui64>::max() || !std::isfinite(AsDouble())) {
                ythrow TTypeError() << AsDouble() << " can't be converted to ui64";
            }
            return AsDouble();
        case NYT::TNode::Bool:
            return AsBool();
        case NYT::TNode::List:
        case NYT::TNode::Map:
        case NYT::TNode::Null:
        case NYT::TNode::Undefined:
            ythrow TTypeError() << "ConvertTo<ui64>() called for type " << GetType();
    }
}

template<>
inline i64 TNode::ConvertTo<i64>() const {
    switch (GetType()) {
        case NYT::TNode::String:
            return ::FromString(AsString());
        case NYT::TNode::Int64:
        case NYT::TNode::Uint64:
            return IntCast<i64>();
        case NYT::TNode::Double:
            // >= because of (1<<63) + 1
            if (AsDouble() < std::numeric_limits<i64>::min() || AsDouble() >= std::numeric_limits<i64>::max() || !std::isfinite(AsDouble())) {
                ythrow TTypeError() << AsDouble() << " can't be converted to i64";
            }
            return AsDouble();
        case NYT::TNode::Bool:
            return AsBool();
        case NYT::TNode::List:
        case NYT::TNode::Map:
        case NYT::TNode::Null:
        case NYT::TNode::Undefined:
            ythrow TTypeError() << "ConvertTo<i64>() called for type " << GetType();
    }
}

template<>
inline double TNode::ConvertTo<double>() const {
    switch (GetType()) {
        case NYT::TNode::String:
            return ::FromString(AsString());
        case NYT::TNode::Int64:
            return AsInt64();
        case NYT::TNode::Uint64:
            return AsUint64();
        case NYT::TNode::Double:
            return AsDouble();
        case NYT::TNode::Bool:
            return AsBool();
        case NYT::TNode::List:
        case NYT::TNode::Map:
        case NYT::TNode::Null:
        case NYT::TNode::Undefined:
            ythrow TTypeError() << "ConvertTo<double>() called for type " << GetType();
    }
}

template<>
inline bool TNode::ConvertTo<bool>() const {
    switch (GetType()) {
        case NYT::TNode::String:
            return ::FromString(AsString());
        case NYT::TNode::Int64:
            return AsInt64();
        case NYT::TNode::Uint64:
            return AsUint64();
        case NYT::TNode::Double:
            return AsDouble();
        case NYT::TNode::Bool:
            return AsBool();
        case NYT::TNode::List:
        case NYT::TNode::Map:
        case NYT::TNode::Null:
        case NYT::TNode::Undefined:
            ythrow TTypeError() << "ConvertTo<bool>() called for type " << GetType();
    }
}

template<typename T>
inline bool TNode::IsOfType() const noexcept {
    return ::HoldsAlternative<T>(Value_);
}

template<typename T>
inline T& TNode::As() {
    return Get<T>(Value_);
}

template<typename T>
inline const T& TNode::As() const {
    return Get<T>(Value_);
}

////////////////////////////////////////////////////////////////////////////////

namespace NNodeCmp {
    bool operator<(const TNode& lhs, const TNode& rhs);
    bool operator<=(const TNode& lhs, const TNode& rhs);
    bool operator>(const TNode& lhs, const TNode& rhs);
    bool operator>=(const TNode& lhs, const TNode& rhs);
    bool IsComparableType(const TNode::EType type);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
