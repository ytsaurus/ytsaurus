#pragma once

#include "../misc/common.h"
#include "../misc/enum.h"
#include "../misc/ptr.h"

// TODO: move
#include "../misc/assert.h"
#include "../misc/foreach.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

typedef TStringBuf TYPath;

////////////////////////////////////////////////////////////////////////////////

DECLARE_ENUM(ENodeType,
    (String)
    (Int64)
    (Double)
    (Map)
    (List)
);

struct INode;
struct IStringNode;
struct IInt64Node;
struct IDoubleNode;
struct IListNode;
struct IMapNode;

template<class T>
struct TScalarTypeTraits
{ };

////////////////////////////////////////////////////////////////////////////////

struct INode
    : virtual TRefCountedBase
{
    typedef TIntrusivePtr<INode> TPtr;
    typedef TIntrusiveConstPtr<INode> TConstPtr;

    virtual ~INode()
    { }

    virtual ENodeType GetType() const = 0;
    
    virtual INode::TPtr AsMutable() const = 0;

    // TODO: consider
    // virtual const IMapNode* GetAttributes() const = 0;

#define DECLARE_AS_METHODS(name) \
    virtual TIntrusiveConstPtr<I ## name ## Node> As ## name() const = 0; \
    virtual TIntrusivePtr<I ## name ## Node> As ## name() = 0;

    DECLARE_AS_METHODS(String)
    DECLARE_AS_METHODS(Int64)
    DECLARE_AS_METHODS(Double)
    DECLARE_AS_METHODS(List)
    DECLARE_AS_METHODS(Map)

#undef DECLARE_AS_METHODS

    virtual INode::TConstPtr GetParent() const = 0;
    virtual void SetParent(INode::TConstPtr parent) = 0;

    virtual bool Navigate(
        const TYPath& path,
        INode::TConstPtr* node,
        TYPath* tailPath) const = 0;

    template<class T>
    T GetValue() const
    {
        return TScalarTypeTraits<T>::GetValue(this);
    }

    template<class T>
    void SetValue(const T& value)
    {
        TScalarTypeTraits<T>::SetValue(this, value);
    }
};

template<class T>
struct IScalarNode
    : INode
{
    typedef T TValue;

    virtual TValue GetValue() const = 0;
    virtual void SetValue(const TValue& value) = 0;
};

struct ICompositeNode
    : INode
{
    virtual int GetChildCount() const = 0;
    // TODO: iterators?
};

struct IListNode
    : ICompositeNode
{
    typedef TIntrusivePtr<IListNode> TPtr;
    typedef TIntrusiveConstPtr<IListNode> TConstPtr;

    virtual INode::TConstPtr FindChild(int index) const = 0;
    virtual void AddChild(INode::TPtr node, int beforeIndex = -1) = 0;
    virtual bool RemoveChild(int index) = 0;

    INode::TConstPtr GetChild(int index) const
    {
        auto child = FindChild(index);
        YASSERT(~child != NULL);
        return child;
    }
};

struct IMapNode
    : ICompositeNode
{
    typedef TIntrusivePtr<IMapNode> TPtr;
    typedef TIntrusiveConstPtr<IMapNode> TConstPtr;

    virtual yvector<Stroka> GetChildNames() const = 0;
    virtual INode::TConstPtr FindChild(const Stroka& name) const = 0;
    virtual bool AddChild(INode::TPtr node, const Stroka& name) = 0;
    virtual bool RemoveChild(const Stroka& name) = 0;

    INode::TConstPtr GetChild(const Stroka& name) const
    {
        auto child = FindChild(name);
        YASSERT(~child != NULL);
        return child;
    }
};

////////////////////////////////////////////////////////////////////////////////

#define DECLARE_SCALAR_TYPE(name, type) \
    struct I ## name ## Node \
        : IScalarNode<type> \
    { \
        typedef TIntrusivePtr<I ## name ## Node> TPtr; \
        typedef TIntrusiveConstPtr<I ## name ## Node> TConstPtr; \
    }; \
    \
    template<> \
    struct TScalarTypeTraits<type> \
    { \
        typedef I ## name ## Node TNode; \
        \
        static type GetValue(const INode* node) \
        { \
            return node->As ## name()->GetValue(); \
        } \
        \
        static void SetValue(INode* node, const type& value) \
        { \
            node->As ## name()->SetValue(value); \
        } \
    };

DECLARE_SCALAR_TYPE(String, Stroka)
DECLARE_SCALAR_TYPE(Int64, i64)
DECLARE_SCALAR_TYPE(Double, double)

#undef DECLARE_SCALAR_TYPE

class TYPathNavigator
{
public:
    TYPathNavigator(INode::TConstPtr root)
        : Root(root)
    { }

    INode::TConstPtr Navigate(const TYPath& path)
    {
        if (path.Empty()) {
            ythrow yexception() << "YPath cannot be empty, use \"/\" to denote the root";
        }

        if (path[0] != '/') {
            ythrow yexception() << "YPath must start with \"'\"";
        }

        TYPath currentPath = path;
        INode::TConstPtr currentNode = Root;

        while (!currentPath.Empty()) {
            INode::TConstPtr nextNode;
            TYPath nextPath;
            if (!currentNode->Navigate(currentPath, &nextNode, &nextPath)) {
                int charsParsed = currentPath.begin() - path.begin();
                YASSERT(charsParsed >= 0 && charsParsed <= static_cast<int>(path.length()));
                Stroka pathParsed(path.SubString(0, charsParsed));
                // TODO: escaping
                ythrow yexception() << Sprintf("Unable to resolve an YPath %s at %s",
                    ~currentPath,
                    ~pathParsed);
            }
            currentNode = nextNode;
            currentPath = nextPath;
        }

        return NULL;
    }

private:
    INode::TConstPtr Root;

};

struct IYsonEvents
{
    virtual ~IYsonEvents()
    { }
    
    virtual void BeginScalar() = 0;
    virtual void StringValue(const Stroka& value) = 0;
    virtual void Int64Value(i64 value) = 0;
    virtual void DoubleValue(double value) = 0;
    virtual void EndScalar() = 0;

    virtual void BeginList() = 0;
    virtual void ListChild(int index) = 0;
    virtual void EndList() = 0;

    virtual void BeginMap() = 0;
    virtual void MapChild(const Stroka& name) = 0;
    virtual void EndMap() = 0;
};

typedef TStringBuf TYson;

class TYsonParser
{
public:
    TYsonParser(IYsonEvents* events);

    void Parse(const TYson& yson);
};

class TYsonWriter
    : public IYsonEvents
{
public:
    TYsonWriter(TOutputStream& stream)
        : Stream(stream)
        , IsFirstChild(false)
        , Indent(0)
    { }

private:
    TOutputStream& Stream;
    bool IsFirstChild;
    int Indent;

    static const int IndentSize = 4;

    void WriteIndent()
    {
        for (int i = 0; i < IndentSize * Indent; ++i) {
            Stream.Write(' ');
        }
    }

    virtual void BeginScalar()
    { }

    virtual void StringValue(const Stroka& value)
    {
        // TODO: escaping
        Stream.Write('"');
        Stream.Write(value);
        Stream.Write('"');
    }

    virtual void Int64Value(i64 value)
    {
        Stream.Write(ToString(value));
    }

    virtual void DoubleValue(double value)
    {
        Stream.Write(ToString(value));
    }

    virtual void EndScalar()
    { }

    virtual void BeginList()
    {
        Stream.Write("[\n");
        ++Indent;
        IsFirstChild = true;
    }

    virtual void ListChild(int index)
    {
        UNUSED(index);
        if (!IsFirstChild) {
            Stream.Write(",\n");
        }
        WriteIndent();
        IsFirstChild = false;
    }

    virtual void EndList()
    {
        Stream.Write('\n');
        --Indent;
        WriteIndent();
        Stream.Write(']');
        IsFirstChild = false;
    }

    virtual void BeginMap()
    {
        Stream.Write("{\n");
        ++Indent;
        IsFirstChild = true;
    }

    virtual void MapChild(const Stroka& name)
    {
        if (!IsFirstChild) {
            Stream.Write(",\n");
        }
        WriteIndent();
        // TODO: escaping
        Stream.Write(name);
        Stream.Write(": ");
        IsFirstChild = false;
    }

    virtual void EndMap()
    {
        Stream.Write('\n');
        --Indent;
        WriteIndent();
        Stream.Write('}');
        IsFirstChild = false;
    }
};

class TTreeParser
{
public:
    TTreeParser(IYsonEvents* events)
        : Events(events)
    { }

    void Parse(INode::TConstPtr root)
    {
        switch (root->GetType()) {
            case ENodeType::String:
            case ENodeType::Int64:
            case ENodeType::Double:
                ParseScalar(root);
                break;

            case ENodeType::List:
                ParseList(root->AsList());
                break;

            case ENodeType::Map:
                ParseMap(root->AsMap());
                break;

            default:
                YASSERT(false);
                break;
        }
    }

private:
    IYsonEvents* Events;

    void ParseScalar(INode::TConstPtr node)
    {
        Events->BeginScalar();
        switch (node->GetType()) {
            case ENodeType::String:
                Events->StringValue(node->GetValue<Stroka>());
                break;

            case ENodeType::Int64:
                Events->Int64Value(node->GetValue<i64>());
                break;

            case ENodeType::Double:
                Events->DoubleValue(node->GetValue<double>());
                break;

            default:
                YASSERT(false);
                break;
        }
        Events->EndScalar();
    }

    void ParseList(IListNode::TConstPtr node)
    {
        Events->BeginList();
        for (int i = 0; i < node->GetChildCount(); ++i) {
            auto child = node->GetChild(i);
            Events->ListChild(i);
            Parse(child);
        }
        Events->EndList();
    }

    void ParseMap(IMapNode::TConstPtr node)
    {
        Events->BeginMap();
        auto childNames = node->GetChildNames();
        FOREACH(const Stroka& name, childNames) {
            auto child = node->GetChild(name);
            Events->MapChild(name);
            Parse(child);
        }
        Events->EndMap();
    }

};

class TTreeBuilder
    : public IYsonEvents
{
public:
    TTreeBuilder();

    INode::TPtr GetRoot() const;

};

////////////////////////////////////////////////////////////////////////////////

template<class IBase>
class TNodeBase
    : public IBase
{
public:
    virtual ENodeType GetType() const = 0;
    
    virtual INode::TPtr AsMutable() const
    {
        return const_cast<TNodeBase*>(this);
    }

#define IMPLEMENT_AS_METHODS(name) \
    virtual TIntrusiveConstPtr<I ## name ## Node> As ## name() const \
    { \
        YASSERT(false); \
        return NULL; \
    } \
    \
    virtual TIntrusivePtr<I ## name ## Node> As ## name() \
    { \
        YASSERT(false); \
        return NULL; \
    }

    IMPLEMENT_AS_METHODS(String)
    IMPLEMENT_AS_METHODS(Int64)
    IMPLEMENT_AS_METHODS(Double)
    IMPLEMENT_AS_METHODS(List)
    IMPLEMENT_AS_METHODS(Map)

#undef IMPLEMENT_AS_METHODS

    virtual INode::TConstPtr GetParent() const
    {
        return Parent;
    }

    virtual void SetParent(INode::TConstPtr parent)
    {
        if (~parent == NULL) {
            Parent = NULL;
        } else {
            YASSERT(~Parent == NULL);
            Parent = parent;
        }
    }

    virtual bool Navigate(
        const TYPath& path,
        INode::TConstPtr* node,
        TYPath* tailPath) const
    {
        UNUSED(path);
        *node = NULL;
        *tailPath = TYPath();
        return false;
    }

private:
    INode::TConstPtr Parent;

};


struct INodeFactory
{
    virtual ~INodeFactory()
    { }

    virtual IStringNode::TPtr CreateString(const Stroka& value = Stroka()) = 0;
    virtual IInt64Node::TPtr CreateInt64(i64 value = 0) = 0;
    virtual IDoubleNode::TPtr CreateDouble(double value = 0) = 0;
    virtual IMapNode::TPtr CreateMap() = 0;
    virtual IListNode::TPtr CreateList() = 0;
};

namespace NEphemeral {

template<class TValue, class IBase>
class TScalarNode
    : public TNodeBase<IBase>
{
public:
    TScalarNode()
        : Value()
    { }

    virtual TValue GetValue() const
    {
        return Value;
    }

    virtual void SetValue(const TValue& value)
    {
        Value = value;
    }

private:
    TValue Value;

};

#define DECLARE_SCALAR_TYPE(name, type) \
    class T ## name ## Node \
        : public TScalarNode<type, I ## name ## Node> \
    { \
    public: \
        virtual ENodeType GetType() const \
        { \
            return ENodeType::name; \
        } \
        \
        virtual I ## name ## Node::TConstPtr As ## name() const \
        { \
            return const_cast<T ## name ## Node*>(this); \
        } \
        \
        virtual I ## name ## Node::TPtr As ## name() \
        { \
            return this; \
        } \
    };

DECLARE_SCALAR_TYPE(String, Stroka)
DECLARE_SCALAR_TYPE(Int64, i64)
DECLARE_SCALAR_TYPE(Double, double)

#undef DECLARE_SCALAR_TYPE


class TMapNode
    : public TNodeBase<IMapNode>
{
public:
    virtual ENodeType GetType() const
    {
        return ENodeType::Map;
    }

    virtual IMapNode::TConstPtr AsMap() const
    {
        return const_cast<TMapNode*>(this);
    }

    virtual IMapNode::TPtr AsMap()
    {
        return this;
    }

    virtual int GetChildCount() const
    {
        return Map.ysize();
    }

    virtual yvector<Stroka> GetChildNames() const
    {
        yvector<Stroka> result;
        FOREACH(const auto& pair, Map) {
            result.push_back(pair.First());
        }
        return result;
    }

    virtual INode::TConstPtr FindChild(const Stroka& name) const
    {
        auto it = Map.find(name);
        return it == Map.end() ? NULL : it->Second();
    }

    virtual bool AddChild(INode::TPtr node, const Stroka& name)
    {
        if (Map.insert(MakePair(name, node)).Second()) {
            node->SetParent(this);
            return true;
        } else {
            return false;
        }
    }

    virtual bool RemoveChild(const Stroka& name)
    {
        return Map.erase(name) == 1;
    }

private:
    yhash_map<Stroka, INode::TConstPtr> Map;

};

class TListNode
    : public TNodeBase<IListNode>
{
public:
    virtual ENodeType GetType() const
    {
        return ENodeType::List;
    }

    virtual IListNode::TConstPtr AsList() const
    {
        return const_cast<TListNode*>(this);
    }

    virtual IListNode::TPtr AsList()
    {
        return this;
    }

    virtual int GetChildCount() const
    {
        return List.ysize();
    }

    virtual INode::TConstPtr FindChild(int index) const
    {
        return index >= 0 && index < List.ysize() ? List[index] : NULL;
    }

    virtual void AddChild(INode::TPtr node, int beforeIndex)
    {
        if (beforeIndex < 0) {
            List.push_back(node); 
        } else {
            List.insert(List.begin() + beforeIndex, node);
        }
        node->SetParent(this);
    }

    virtual bool RemoveChild(int index)
    {
        if (index < 0 || index >= List.ysize())
            return false;
        List.erase(List.begin() + index);
        return true;
    }

private:
    yvector<INode::TConstPtr> List;

};

struct TNodeFactory
    : INodeFactory
{
    static INodeFactory* Get()
    {
        return Singleton<TNodeFactory>();
    }

    virtual IStringNode::TPtr CreateString(const Stroka& value = Stroka())
    {
        IStringNode::TPtr node = ~New<TStringNode>();
        node->SetValue(value);
        return node;
    }

    virtual IInt64Node::TPtr CreateInt64(i64 value = 0)
    {
        IInt64Node::TPtr node = ~New<TInt64Node>();
        node->SetValue(value);
        return node;
    }

    virtual IDoubleNode::TPtr CreateDouble(double value = 0)
    {
        IDoubleNode::TPtr node = ~New<TDoubleNode>();
        node->SetValue(value);
        return node;
    }

    virtual IMapNode::TPtr CreateMap()
    {
        return ~New<TMapNode>();
    }

    virtual IListNode::TPtr CreateList()
    {
        return ~New<TListNode>();
    }
};


} // namespace NEphemeral

////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////


} // namespace NRegistry
} // namespace NYT

