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

        StringNode = 1  /*"string_node"*/,
        Int64Node = 2   /*"int64_node"*/,
        Uint64Node = 3  /*"uint64_node"*/,
        DoubleNode = 4  /*"double_node"*/,
        BooleanNode = 5 /*"boolean_node"*/,
        ListNode = 6    /*"list_node"*/,
        MapNode = 7     /*"map_node"*/,
        Null = 8        /*"null"*/,

        // Backward compatibility part
        // TODO: exterminate
        UNDEFINED = Undefined,
        STRING = StringNode,
        INT64 = Int64Node,
        UINT64 = Uint64Node,
        DOUBLE = DoubleNode,
        BOOL = BooleanNode,
        LIST = ListNode,
        MAP = MapNode,
        ENTITY = Null,
    };

    using TList = yvector<TNode>;
    using TMap = yhash<TString, TNode>;

private:
    struct TEntity {
        bool operator==(const TEntity&) const;
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
        TList,
        TMap,
        TEntity,
        TUndefined
        >;

public:

    TNode();
    TNode(const char* s);
    TNode(const TStringBuf& s);
    TNode(TString s);
    TNode(int i);

    //this case made speccially for prevent mess cast of EType into TNode through TNode(int) constructor
    //usual case of error SomeNode == TNode::UNDEFINED <-- SomeNode indeed will be compared with TNode(0) without this method
    //correct way is SomeNode.GetType() == TNode::UNDEFINED
    template<class T = EType>
    Y_FORCE_INLINE TNode(EType)
    {
        static_assert(std::is_same<T, EType>::type, "looks like a mistake, may be you forget .GetType()");
    }

    TNode(unsigned int ui);
    TNode(long i);
    TNode(unsigned long ui);
    TNode(long long i);
    TNode(unsigned long long ui);
    TNode(double d);
    TNode(bool b);

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
    bool IsEntity() const;
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
    const TList& AsList() const;
    const TMap& AsMap() const;
    TList& AsList();
    TMap& AsMap();

    const TString& UncheckedAsString() const noexcept;
    i64 UncheckedAsInt64() const noexcept;
    ui64 UncheckedAsUint64() const noexcept;
    double UncheckedAsDouble() const noexcept;
    bool UncheckedAsBool() const noexcept;
    const TList& UncheckedAsList() const noexcept;
    const TMap& UncheckedAsMap() const noexcept;
    TList& UncheckedAsList() noexcept;
    TMap& UncheckedAsMap() noexcept;

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

    static const TString& TypeToString(EType type);

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
        case TNode::UINT64:
            return AsUint64();
        case TNode::INT64:
            if (AsInt64() < 0) {
                ythrow TTypeError() << AsInt64() << " can't be converted to ui64";
            }
            return AsInt64();
        default:
            ythrow TTypeError() << "IntCast() called for type " << TypeToString(GetType());
    }
}

template<>
inline i64 TNode::IntCast<i64>() const {
    switch (GetType()) {
        case TNode::UINT64:
            if (AsUint64() > (ui64)std::numeric_limits<i64>::max()) {
                ythrow TTypeError() << AsUint64() << " can't be converted to i64";
            }
            return AsUint64();
        case TNode::INT64:
            return AsInt64();
        default:
            ythrow TTypeError() << "IntCast() called for type " << TypeToString(GetType());
    }
}

template<typename T>
inline T TNode::ConvertTo() const {
    static_assert(sizeof(T) != sizeof(T), "should have template specialization");
}

template<>
inline TString TNode::ConvertTo<TString>() const {
    switch (GetType()) {
        case NYT::TNode::STRING:
            return AsString();
        case NYT::TNode::INT64:
            return ::ToString(AsInt64());
        case NYT::TNode::UINT64:
            return ::ToString(AsUint64());
        case NYT::TNode::DOUBLE:
            return ::ToString(AsDouble());
        case NYT::TNode::BOOL:
            return ::ToString(AsBool());
        case NYT::TNode::LIST:
        case NYT::TNode::MAP:
        case NYT::TNode::ENTITY:
        case NYT::TNode::UNDEFINED:
            ythrow TTypeError() << "ConvertTo<TString>() called for type " << TypeToString(GetType());
    }
}

template<>
inline ui64 TNode::ConvertTo<ui64>() const {
    switch (GetType()) {
        case NYT::TNode::STRING:
            return ::FromString(AsString());
        case NYT::TNode::INT64:
        case NYT::TNode::UINT64:
            return IntCast<ui64>();
        case NYT::TNode::DOUBLE:
            // >= because of (1<<64) + 1
            if (AsDouble() < std::numeric_limits<ui64>::min() || AsDouble() >= std::numeric_limits<ui64>::max() || !std::isfinite(AsDouble())) {
                ythrow TTypeError() << AsDouble() << " can't be converted to ui64";
            }
            return AsDouble();
        case NYT::TNode::BOOL:
            return AsBool();
        case NYT::TNode::LIST:
        case NYT::TNode::MAP:
        case NYT::TNode::ENTITY:
        case NYT::TNode::UNDEFINED:
            ythrow TTypeError() << "ConvertTo<ui64>() called for type " << TypeToString(GetType());
    }
}

template<>
inline i64 TNode::ConvertTo<i64>() const {
    switch (GetType()) {
        case NYT::TNode::STRING:
            return ::FromString(AsString());
        case NYT::TNode::INT64:
        case NYT::TNode::UINT64:
            return IntCast<i64>();
        case NYT::TNode::DOUBLE:
            // >= because of (1<<63) + 1
            if (AsDouble() < std::numeric_limits<i64>::min() || AsDouble() >= std::numeric_limits<i64>::max() || !std::isfinite(AsDouble())) {
                ythrow TTypeError() << AsDouble() << " can't be converted to i64";
            }
            return AsDouble();
        case NYT::TNode::BOOL:
            return AsBool();
        case NYT::TNode::LIST:
        case NYT::TNode::MAP:
        case NYT::TNode::ENTITY:
        case NYT::TNode::UNDEFINED:
            ythrow TTypeError() << "ConvertTo<i64>() called for type " << TypeToString(GetType());
    }
}

template<>
inline double TNode::ConvertTo<double>() const {
    switch (GetType()) {
        case NYT::TNode::STRING:
            return ::FromString(AsString());
        case NYT::TNode::INT64:
            return AsInt64();
        case NYT::TNode::UINT64:
            return AsUint64();
        case NYT::TNode::DOUBLE:
            return AsDouble();
        case NYT::TNode::BOOL:
            return AsBool();
        case NYT::TNode::LIST:
        case NYT::TNode::MAP:
        case NYT::TNode::ENTITY:
        case NYT::TNode::UNDEFINED:
            ythrow TTypeError() << "ConvertTo<double>() called for type " << TypeToString(GetType());
    }
}

template<>
inline bool TNode::ConvertTo<bool>() const {
    switch (GetType()) {
        case NYT::TNode::STRING:
            return ::FromString(AsString());
        case NYT::TNode::INT64:
            return AsInt64();
        case NYT::TNode::UINT64:
            return AsUint64();
        case NYT::TNode::DOUBLE:
            return AsDouble();
        case NYT::TNode::BOOL:
            return AsBool();
        case NYT::TNode::LIST:
        case NYT::TNode::MAP:
        case NYT::TNode::ENTITY:
        case NYT::TNode::UNDEFINED:
            ythrow TTypeError() << "ConvertTo<bool>() called for type " << TypeToString(GetType());
    }
}

template<typename T>
inline bool TNode::IsOfType() const noexcept {
    return (Value_.Tag() == TValue::TagOf<T>());
}

template<typename T>
inline T& TNode::As() {
    return Value_.As<T>();
}

template<typename T>
inline const T& TNode::As() const {
    return Value_.As<T>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
