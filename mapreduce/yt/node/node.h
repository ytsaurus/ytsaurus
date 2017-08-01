#pragma once

#include <util/generic/hash.h>
#include <util/generic/vector.h>
#include <util/generic/yexception.h>
#include <util/generic/bt_exception.h>

#include <util/generic/variant.h>


namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TNode
{
public:
    class TTypeError
        : public TWithBackTrace<yexception>
    { };

    enum EType {
        UNDEFINED,
        STRING,
        INT64,
        UINT64,
        DOUBLE,
        BOOL,
        LIST,
        MAP,
        ENTITY
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

    template<typename T>
    T& As();

    template<typename T>
    const T& As() const;

    static TNode CreateList();
    static TNode CreateMap();
    static TNode CreateEntity();

    const TNode& operator[](size_t index) const;
    TNode& operator[](size_t index);

    TNode& Add();
    TNode& Add(const TNode& node);
    TNode& Add(TNode&& node);

    bool HasKey(const TStringBuf key) const;

    TNode& operator()(const TString& key, const TNode& value);
    TNode& operator()(const TString& key, TNode&& value);

    const TNode& operator[](const TStringBuf key) const;
    TNode& operator[](const TStringBuf key);

    // attributes
    bool HasAttributes() const;
    void ClearAttributes();
    const TNode& GetAttributes() const;
    TNode& Attributes();

    void MoveWithoutAttributes(TNode&& rhs);

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
