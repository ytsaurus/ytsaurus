#pragma once

#include <util/generic/hash.h>
#include <util/generic/vector.h>
#include <util/generic/yexception.h>
#include <util/string/printf.h>

#include <util/memory/pool.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TNode
{
public:
    class TTypeError
        : public yexception
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
    using TMap = yhash<Stroka, TNode>;

    TNode()
    { }

    TNode(const char* s)
        : Type_(STRING)
    {
        String_ = new Stroka(s);
    }

    TNode(const TStringBuf& s)
        : Type_(STRING)
    {
        String_ = new Stroka(s);
    }

    TNode(const Stroka& s)
        : Type_(STRING)
    {
        String_ = new Stroka(s);
    }

    TNode(int i)
        : Type_(INT64)
    {
        Int64_ = i;
    }

    TNode(unsigned int ui)
        : Type_(UINT64)
    {
        Uint64_ = ui;
    }

    TNode(long i)
        : Type_(INT64)
    {
        Int64_ = i;
    }

    TNode(unsigned long ui)
        : Type_(UINT64)
    {
        Uint64_ = ui;
    }

    TNode(long long i)
        : Type_(INT64)
    {
        Int64_ = i;
    }

    TNode(unsigned long long ui)
        : Type_(UINT64)
    {
        Uint64_ = ui;
    }

    TNode(double d)
        : Type_(DOUBLE)
    {
        Double_ = d;
    }

    TNode(const TNode& rhs)
    {
        Copy(rhs);
    }

    TNode& operator=(const TNode& rhs)
    {
        Clear();
        Copy(rhs);
        return *this;
    }

    TNode(TNode&& rhs)
    {
        Move(MoveArg(rhs));
    }

    TNode& operator=(TNode&& rhs)
    {
        Clear();
        Move(MoveArg(rhs));
        return *this;
    }

    ~TNode()
    {
        Clear();
    }

    bool IsString() const
    {
        return Type_ == STRING;
    }

    bool IsInt64() const
    {
        return Type_ == INT64;
    }

    bool IsUint64() const
    {
        return Type_ == UINT64;
    }

    bool IsDouble() const
    {
        return Type_ == DOUBLE;
    }

    bool IsBool() const
    {
        return Type_ == BOOL;
    }

    bool IsList() const
    {
        return Type_ == LIST;
    }

    bool IsMap() const
    {
        return Type_ == MAP;
    }

    bool IsEntity() const
    {
        return Type_ == ENTITY;
    }

    bool Empty() const
    {
        switch (Type_) {
            case STRING:
                return String_->empty();
            case LIST:
                return List_->empty();
            case MAP:
                return Map_->empty();
            default:
                ythrow TTypeError()
                    << Sprintf("Empty() called for type %s", ~TypeToString(Type_));
        }
    }

    size_t Size() const
    {
        switch (Type_) {
            case STRING:
                return String_->size();
            case LIST:
                return List_->size();
            case MAP:
                return Map_->size();
            default:
                ythrow TTypeError()
                    << Sprintf("Size() called for type %s", ~TypeToString(Type_));
        }
    }

    EType GetType() const
    {
        return Type_;
    }

    const Stroka& AsString() const
    {
        CheckType(STRING);
        return *String_;
    }

    i64 AsInt64() const
    {
        CheckType(INT64);
        return Int64_;
    }

    ui64 AsUint64() const
    {
        CheckType(UINT64);
        return Uint64_;
    }

    double AsDouble() const
    {
        CheckType(DOUBLE);
        return Double_;
    }

    bool AsBool() const
    {
        CheckType(BOOL);
        return Bool_;
    }

    const TList& AsList() const
    {
        CheckType(LIST);
        return *List_;
    }

    const TMap& AsMap() const
    {
        CheckType(MAP);
        return *Map_;
    }

    TList& AsList()
    {
        CheckType(LIST);
        return *List_;
    }

    TMap& AsMap()
    {
        CheckType(MAP);
        return *Map_;
    }

    static TNode CreateList()
    {
        TNode node;
        node.Type_ = LIST;
        node.List_ = new TList;
        return node;
    }

    static TNode CreateMap()
    {
        TNode node;
        node.Type_ = MAP;
        node.Map_ = new TMap;
        return node;
    }

    static TNode CreateEntity()
    {
        TNode node;
        node.Type_ = ENTITY;
        return node;
    }

    // list

    const TNode& operator[](size_t index) const
    {
        CheckType(LIST);
        return (*List_)[index];
    }

    TNode& operator[](size_t index)
    {
        CheckType(LIST);
        return (*List_)[index];
    }

    TNode& Add()
    {
        AssureList();
        List_->push_back();
        return List_->back();
    }

    void Add(TNode node)
    {
        AssureList();
        List_->push_back(node);
    }

    // map

    TNode& operator()(const Stroka& key, const TNode& value)
    {
        AssureMap();
        (*Map_)[key] = value;
        return *this;
    }

    TNode& operator()(const Stroka& key, TNode&& value)
    {
        AssureMap();
        (*Map_)[key] = MoveArg(value);
        return *this;
    }

    const TNode& operator[](const Stroka& key) const
    {
        CheckType(MAP);
        static TNode notFound;
        TMap::const_iterator i = Map_->find(key);
        if (i == Map_->end()) {
            return notFound;
        } else {
            return i->second;
        }
    }

    TNode& operator[](const Stroka& key)
    {
        AssureMap();
        return (*Map_)[key];
    }

    // attributes

    bool HasAttributes() const
    {
        return Attributes_ && !Attributes_->Empty();
    }

    void ClearAttributes()
    {
        if (!Attributes_) {
            return;
        }
        delete Attributes_;
        Attributes_ = nullptr;
    }

    const TNode& GetAttributes() const
    {
        static TNode notFound = TNode::CreateMap();
        if (!Attributes_) {
            return notFound;
        }
        return *Attributes_;
    }

    TNode& Attributes()
    {
        if (!Attributes_) {
            CreateAttributes();
        }
        return *Attributes_;
    }

    void MoveWithoutAttributes(TNode&& rhs)
    {
        Type_ = rhs.Type_;
        memcpy(&Int64_, &rhs.Int64_, sizeof(i64));
        rhs.Type_ = UNDEFINED;
        if (rhs.Attributes_) {
            delete rhs.Attributes_;
            rhs.Attributes_ = nullptr;
        }
    }

    static Stroka TypeToString(EType type)
    {
        static Stroka typeNames[] = {
            "UNDEFINED",
            "STRING",
            "INT64",
            "UINT64",
            "DOUBLE",
            "BOOL",
            "LIST",
            "MAP",
            "ENTITY"
        };
        if (type < sizeof(typeNames)) {
            return typeNames[type];
        }
        ythrow yexception()
            << Sprintf("incorrect TNode::EType = %d", static_cast<int>(type));
    }

private:
    void Clear()
    {
        if (Attributes_) {
            delete Attributes_;
            Attributes_ = nullptr;
        }

        switch (Type_) {
            case UNDEFINED:
                return;
            case STRING:
                delete String_;
                break;
            case LIST:
                delete List_;
                break;
            case MAP:
                delete Map_;
                break;
            default:
                break;
        }

        Type_ = UNDEFINED;
    }

    void Copy(const TNode& rhs)
    {
        Type_ = rhs.Type_;

        if (rhs.Attributes_) {
            if (!Attributes_) {
                CreateAttributes();
            }
            *Attributes_ = *rhs.Attributes_;
        }

        switch (Type_) {
            case UNDEFINED:
                return;
            case STRING:
                String_ = new Stroka(*rhs.String_);
                break;
            case INT64:
                Int64_ = rhs.Int64_;
                break;
            case UINT64:
                Uint64_ = rhs.Uint64_;
                break;
            case DOUBLE:
                Double_ = rhs.Double_;
                break;
            case BOOL:
                Bool_ = rhs.Bool_;
                break;
            case LIST:
                List_ = new TList(*rhs.List_);
                break;
            case MAP:
                Map_ = new TMap(*rhs.Map_);
                break;
            default:
                break;
        }
    }

    void Move(TNode&& rhs)
    {
        if (Attributes_) {
            delete Attributes_;
        }
        memcpy(this, &rhs, sizeof(TNode));
        if (rhs.Attributes_) {
            rhs.Attributes_ = nullptr;
        }
        rhs.Type_ = UNDEFINED;
    }

    void CheckType(EType type) const
    {
        if (Type_ != type) {
            ythrow TTypeError()
                << Sprintf("TNode type %s expected, actual type %s",
                    ~TypeToString(type), ~TypeToString(Type_));
        }
    }

    void AssureMap()
    {
        if (Type_ == UNDEFINED) {
            Type_ = MAP;
            Map_ = new TMap;
        } else {
            CheckType(MAP);
        }
    }

    void AssureList()
    {
        if (Type_ == UNDEFINED) {
            Type_ = LIST;
            List_ = new TList;
        } else {
            CheckType(LIST);
        }
    }

    void CreateAttributes()
    {
        Attributes_ = new TNode;
        Attributes_->Type_ = MAP;
        Attributes_->Map_ = new TMap;
    }

private:
    EType Type_ = UNDEFINED;
    union {
        Stroka* String_;
        i64 Int64_;
        ui64 Uint64_;
        double Double_;
        bool Bool_;
        TList* List_;
        TMap* Map_;
    };
    TNode* Attributes_ = nullptr;

    friend bool operator==(const TNode& lhs, const TNode& rhs);
    friend bool operator!=(const TNode& lhs, const TNode& rhs);
};

inline bool operator==(const TNode& lhs, const TNode& rhs)
{
    if (lhs.Type_ != rhs.Type_) {
        return false;
    }

    if (lhs.Attributes_) {
        if (rhs.Attributes_) {
            return lhs.Attributes_ == rhs.Attributes_;
        } else {
            return false;
        }
    } else {
        if (rhs.Attributes_) {
            return false;
        }
    }

    switch (lhs.Type_) {
        case TNode::STRING:
            return *lhs.String_ == *rhs.String_;
        case TNode::INT64:
            return lhs.Int64_ == rhs.Int64_;
        case TNode::UINT64:
            return lhs.Uint64_ == rhs.Uint64_;
        case TNode::DOUBLE:
            return lhs.Double_ == rhs.Double_;
        case TNode::BOOL:
            return lhs.Bool_ == rhs.Bool_;
        case TNode::LIST:
            return *lhs.List_ == *rhs.List_;
        case TNode::MAP:
            // TODO: compare maps
            return false;
            //return *lhs.Map_ == *rhs.Map_;
        case TNode::ENTITY:
            return true;
        default:
            return false;
    }
}

inline bool operator!=(const TNode& lhs, const TNode& rhs)
{
    return !(lhs == rhs);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
