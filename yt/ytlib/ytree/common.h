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
    (Entity)
);

struct INode;
struct IStringNode;
struct IInt64Node;
struct IDoubleNode;
struct IListNode;
struct IMapNode;
struct IEntityNode;

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

#define DECLARE_AS_METHODS(name) \
    virtual TIntrusiveConstPtr<I ## name ## Node> As ## name() const = 0; \
    virtual TIntrusivePtr<I ## name ## Node> As ## name() = 0;

    DECLARE_AS_METHODS(String)
    DECLARE_AS_METHODS(Int64)
    DECLARE_AS_METHODS(Double)
    DECLARE_AS_METHODS(List)
    DECLARE_AS_METHODS(Map)

#undef DECLARE_AS_METHODS

    virtual TIntrusiveConstPtr<IMapNode> GetAttributes() const = 0;
    virtual void SetAttributes(TIntrusiveConstPtr<IMapNode> attributes) = 0;

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

struct IEntityNode
    : INode
{
    typedef TIntrusivePtr<IEntityNode> TPtr;
    typedef TIntrusiveConstPtr<IEntityNode> TConstPtr;
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

    virtual void BeginTree() = 0;
    virtual void EndTree() = 0;

    virtual void StringValue(const Stroka& value) = 0;
    virtual void Int64Value(i64 value) = 0;
    virtual void DoubleValue(double value) = 0;
    virtual void EntityValue() = 0;

    virtual void BeginList() = 0;
    virtual void ListItem(int index) = 0;
    virtual void EndList() = 0;

    virtual void BeginMap() = 0;
    virtual void MapItem(const Stroka& name) = 0;
    virtual void EndMap() = 0;

    virtual void BeginAttributes() = 0;
    virtual void AttributesItem(const Stroka& name) = 0;
    virtual void EndAttributes() = 0;
};

typedef TStringBuf TYson;

class TYsonParser
{
public:
    TYsonParser(IYsonEvents* events)
        : Events(events)
    {
        Reset();
    }

    void Parse(TInputStream* stream)
    {
        try {
            Stream = stream;
            Events->BeginTree();
            ParseAny();
            int ch = ReadChar();
            if (ch != Eos) {
                ythrow yexception() << Sprintf("Unexpected symbol %s in YSON",
                    ~Stroka(static_cast<char>(ch)).Quote());
            }
            Events->EndTree();
        } catch (...) {
            Reset();
            throw;
        }
    }

private:
    static const int Eos = -1;
    static const int NoLookahead = -2;

    IYsonEvents* Events;
    TInputStream* Stream;
    int Lookahead;

    void Reset()
    {
        Stream = NULL;
        Lookahead = NoLookahead;
    }

    int ReadChar()
    {
        if (Lookahead == NoLookahead) {
            PeekChar();
        }

        int result = Lookahead;
        Lookahead = NoLookahead;
        return result;
    }

    void ExpectChar(char expectedCh)
    {
        int readCh = ReadChar();
        if (readCh == Eos) {
            // TODO:
            ythrow yexception() << Sprintf("Premature end-of-stream expecting %s in YSON",
                ~Stroka(expectedCh).Quote());
        }
        if (static_cast<char>(readCh) != expectedCh) {
            // TODO:
            ythrow yexception() << Sprintf("Found %s while expecting %s in YSON",
                ~Stroka(static_cast<char>(readCh)).Quote(),
                ~Stroka(expectedCh).Quote());
        }
    }

    int PeekChar()
    {
        if (Lookahead != NoLookahead) {
            return Lookahead;
        }

        char ch;
        if (Stream->ReadChar(ch)) {
            Lookahead = ch;
        } else {
            Lookahead = Eos;
        }

        return Lookahead;
    }

    static bool IsWhitespace(int ch)
    {
        return
            ch == '\n' ||
            ch == '\r' ||
            ch == '\t' ||
            ch == ' ';
    }

    void SkipWhitespaces()
    {
        while (IsWhitespace(PeekChar())) {
            ReadChar();
        }
    }

    Stroka ReadString()
    {
        Stroka result;
        if (PeekChar() == '"') {
            YVERIFY(ReadChar() == '"');
            while (true) {
                int ch = ReadChar();
                if (ch == Eos) {
                    // TODO:
                    ythrow yexception() << Sprintf("Premature end-of-stream while parsing String in YSON");
                }
                if (ch == '"')
                    break;
                result.append(static_cast<char>(ch));
            }
        } else {
            while (true) {
                int ch = PeekChar();
                if (!(ch >= 'a' && ch <= 'z' ||
                      ch >= 'A' && ch <= 'Z' ||
                      ch == '_' ||
                      ch >= '0' && ch <= '9' && !result.Empty()))
                      break;
                ReadChar();
                result.append(static_cast<char>(ch));
            }
        }
        return result;
    }

    Stroka ReadNumericLike()
    {
        Stroka result;
        while (true) {
            int ch = PeekChar();
            if (!(ch >= '0' && ch <= '9' ||
                  ch == '+' ||
                  ch == '-' ||
                  ch == '.' ||
                  ch == 'e' || ch == 'E'))
                break;
            ReadChar();
            result.append(static_cast<char>(ch));
        }
        if (result.Empty()) {
            // TODO:
            ythrow yexception() << Sprintf("Premature end-of-stream while parsing Numeric in YSON");
        }
        return result;
    }

    void ParseAny()
    {
        SkipWhitespaces();
        int ch = PeekChar();
        switch (ch) {
            case '[':
                ParseList();
                ParseAttributes();
                break;

            case '{':
                ParseMap();
                ParseAttributes();
                break;

            case '<':
                ParseEntity();
                ParseAttributes();
                break;

            default:
                if (ch >= '0' && ch <= '9' ||
                    ch == '+' ||
                    ch == '-')
                {
                    ParseNumeric();
                    ParseAttributes();
                } else if (ch >= 'a' && ch <= 'z' ||
                           ch >= 'A' && ch <= 'Z' ||
                           ch == '_' ||
                           ch == '"')
                {
                    ParseString();
                    ParseAttributes();
                } else {
                    // TODO:
                    ythrow yexception() << Sprintf("Unexpected %s in YSON",
                        ~Stroka(static_cast<char>(ch)).Quote());
                }
                break;
        }
    }

    void ParseAttributesItem()
    {
        SkipWhitespaces();
        Stroka name = ReadString();
        if (name.Empty()) {
            // TODO:
            ythrow yexception() << Sprintf("Empty attribute name in YSON");
        }
        SkipWhitespaces();
        ExpectChar(':');
        Events->AttributesItem(name);
        ParseAny();
    }

    void ParseAttributes()
    {
        SkipWhitespaces();
        if (PeekChar() != '<')
            return;
        YVERIFY(ReadChar() == '<');
        Events->BeginAttributes();
        while (true) {
            SkipWhitespaces();
            if (PeekChar() == '>')
                break;
            ParseAttributesItem();
            if (PeekChar() == '>')
                break;
            ExpectChar(',');
        }
        YVERIFY(ReadChar() == '>');
        Events->EndAttributes();
    }

    void ParseListItem(int index)
    {
        Events->ListItem(index);
        ParseAny();
    }

    void ParseList()
    {
        YVERIFY(ReadChar() == '[');
        Events->BeginList();
        for (int index = 0; true; ++index) {
            SkipWhitespaces();
            if (PeekChar() == ']')
                break;
            ParseListItem(index);
            if (PeekChar() == ']')
                break;
            ExpectChar(',');
        }
        YVERIFY(ReadChar() == ']');
        Events->EndList();
    }

    void ParseMapItem()
    {
        SkipWhitespaces();
        Stroka name = ReadString();
        if (name.Empty()) {
            // TODO:
            ythrow yexception() << Sprintf("Empty map item name in YSON");
        }
        SkipWhitespaces();
        ExpectChar(':');
        Events->MapItem(name);
        ParseAny();
    }

    void ParseMap()
    {
        YVERIFY(ReadChar() == '{');
        Events->BeginMap();
        while (true) {
            SkipWhitespaces();
            if (PeekChar() == '}')
                break;
            ParseMapItem();
            if (PeekChar() == '}')
                break;
            ExpectChar(',');
        }
        YVERIFY(ReadChar() == '}');
        Events->EndMap();
    }

    void ParseEntity()
    {
        Events->EntityValue();
    }

    void ParseString()
    {
        Stroka value = ReadString();
        Events->StringValue(value);
    }

    static bool IsIntegerLike(const Stroka& str)
    {
        for (int i = 0; i < static_cast<int>(str.length()); ++i) {
            char ch = str[i];
            if (ch == '.' || ch == 'e' || ch == 'E')
                return false;
        }
        return true;
    }

    void ParseNumeric()
    {
        Stroka str = ReadNumericLike();
        if (IsIntegerLike(str)) {
            try {
                i64 value = FromString<i64>(str);
                Events->Int64Value(value);
            } catch (...) {
                // TODO:
                ythrow yexception() << Sprintf("Failed to parse Int64 literal %s in YSON",
                    ~str.Quote());
            }
        } else {
            try {
                double value = FromString<double>(str);
                Events->DoubleValue(value);
            } catch (...) {
                // TODO:
                ythrow yexception() << Sprintf("Failed to parse Double literal %s in YSON",
                    ~str.Quote());
            }
        }
    }

};

class TYsonWriter
    : public IYsonEvents
{
public:
    TYsonWriter(TOutputStream* stream)
        : Stream(stream)
        , IsFirstItem(false)
        , IsEmptyEntity(false)
        , Indent(0)
    { }

private:
    TOutputStream* Stream;
    bool IsFirstItem;
    bool IsEmptyEntity;
    int Indent;

    static const int IndentSize = 4;

    void WriteIndent()
    {
        for (int i = 0; i < IndentSize * Indent; ++i) {
            Stream->Write(' ');
        }
    }

    void SetEmptyEntity()
    {
        IsEmptyEntity = true;
    }

    void ResetEmptyEntity()
    {
        IsEmptyEntity = false;
    }

    void FlushEmptyEntity()
    {
        if (IsEmptyEntity) {
            Stream->Write("<>");
            IsEmptyEntity = false;
        }
    }

    void BeginCollection(char openBracket)
    {
        Stream->Write(openBracket);
        IsFirstItem = true;
    }

    void CollectionItem()
    {
        if (IsFirstItem) {
            Stream->Write('\n');
            ++Indent;
        } else {
            FlushEmptyEntity();
            Stream->Write(",\n");
        }
        WriteIndent();
        IsFirstItem = false;
    }

    void EndCollection(char closeBracket)
    {
        FlushEmptyEntity();
        if (!IsFirstItem) {
            Stream->Write('\n');
            --Indent;
            WriteIndent();
        }
        Stream->Write(closeBracket);
        IsFirstItem = false;
    }


    virtual void BeginTree()
    { }

    virtual void EndTree()
    {
        FlushEmptyEntity();
    }


    virtual void StringValue(const Stroka& value)
    {
        // TODO: escaping
        Stream->Write('"');
        Stream->Write(value);
        Stream->Write('"');
    }

    virtual void Int64Value(i64 value)
    {
        Stream->Write(ToString(value));
    }

    virtual void DoubleValue(double value)
    {
        Stream->Write(ToString(value));
    }

    virtual void EntityValue()
    {
        SetEmptyEntity();
    }


    virtual void BeginList()
    {
        BeginCollection('[');
    }

    virtual void ListItem(int index)
    {
        UNUSED(index);
        CollectionItem();
    }

    virtual void EndList()
    {
        EndCollection(']');
    }

    virtual void BeginMap()
    {
        BeginCollection('{');
    }

    virtual void MapItem(const Stroka& name)
    {
        CollectionItem();
        // TODO: escaping
        Stream->Write(name);
        Stream->Write(": ");
    }

    virtual void EndMap()
    {
        EndCollection('}');
    }


    virtual void BeginAttributes()
    {
        if (IsEmptyEntity) {
            ResetEmptyEntity();
        } else {
            Stream->Write(' ');
        }
        BeginCollection('<');
    }

    virtual void AttributesItem(const Stroka& name)
    {
        CollectionItem();
        // TODO: escaping
        Stream->Write(name);
        Stream->Write(": ");
        IsFirstItem = false;
    }

    virtual void EndAttributes()
    {
        EndCollection('>');
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
        Events->BeginTree();
        ParseAny(root);
        Events->EndTree();
    }

private:
    IYsonEvents* Events;

    void ParseAny(INode::TConstPtr node)
    {
        switch (node->GetType()) {
            case ENodeType::String:
            case ENodeType::Int64:
            case ENodeType::Double:
            case ENodeType::Entity:
                ParseScalar(node);
                break;

            case ENodeType::List:
                ParseList(node->AsList());
                break;

            case ENodeType::Map:
                ParseMap(node->AsMap());
                break;

            default:
                YASSERT(false);
                break;
        }

        auto attributes = node->GetAttributes();
        if (~attributes != NULL) {
            ParseAttributes(attributes);
        }
    }

    void ParseScalar(INode::TConstPtr node)
    {
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

            case ENodeType::Entity:
                Events->EntityValue();
                break;

            default:
                YASSERT(false);
                break;
        }
    }

    void ParseList(IListNode::TConstPtr node)
    {
        Events->BeginList();
        for (int i = 0; i < node->GetChildCount(); ++i) {
            auto child = node->GetChild(i);
            Events->ListItem(i);
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
            Events->MapItem(name);
            Parse(child);
        }
        Events->EndMap();
    }

    void ParseAttributes(IMapNode::TConstPtr node)
    {
        Events->BeginAttributes();
        auto childNames = node->GetChildNames();
        FOREACH(const Stroka& name, childNames) {
            auto child = node->GetChild(name);
            Events->AttributesItem(name);
            Parse(child);
        }
        Events->EndAttributes();
    }

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
    virtual IEntityNode::TPtr CreateEntity() = 0;
};

class TTreeBuilder
    : public IYsonEvents
{
public:
    TTreeBuilder(INodeFactory* factory)
        : Factory(factory)
    { }

    INode::TPtr GetRoot() const
    {
        YASSERT(Stack.ysize() == 1);
        return Stack[0];
    }

private:
    INodeFactory* Factory;
    yvector<INode::TPtr> Stack;

    virtual void BeginTree()
    {
        YASSERT(Stack.ysize() == 0);
    }

    virtual void EndTree()
    {
        YASSERT(Stack.ysize() == 1);
    }


    virtual void StringValue(const Stroka& value)
    {
        Push(~Factory->CreateString(value));
    }

    virtual void Int64Value(i64 value)
    {
        Push(~Factory->CreateInt64(value));
    }

    virtual void DoubleValue(double value)
    {
        Push(~Factory->CreateDouble(value));
    }

    virtual void EntityValue()
    {
        Push(~Factory->CreateEntity());
    }


    virtual void BeginList()
    {
        Push(~Factory->CreateList());
        Push(NULL);
    }

    virtual void ListItem(int index)
    {
        UNUSED(index);
        AddToList();
    }

    virtual void EndList()
    {
        AddToList();
    }

    void AddToList()
    {
        auto child = Pop();
        auto list = Peek()->AsList();
        if (~child != NULL) {
            list->AddChild(child);
        }
    }


    virtual void BeginMap()
    {
        Push(~Factory->CreateMap());
        Push(NULL);
        Push(NULL);
    }

    virtual void MapItem(const Stroka& name)
    {
        AddToMap();
        Push(~Factory->CreateString(name));
    }

    virtual void EndMap()
    {
        AddToMap();
    }

    void AddToMap()
    {
        auto child = Pop();
        auto name = Pop();
        auto map = Peek()->AsMap();
        if (~child != NULL) {
            map->AddChild(child, name->GetValue<Stroka>());
        }
    }

    
    virtual void BeginAttributes()
    {
        BeginMap();
    }

    virtual void AttributesItem(const Stroka& name)
    {
        MapItem(name);
    }

    virtual void EndAttributes()
    {
        EndMap();
        auto attributes = Pop()->AsMap();
        auto node = Peek();
        node->SetAttributes(attributes);
    }


    void Push(INode::TPtr node)
    {
        Stack.push_back(node);
    }

    INode::TPtr Pop()
    {
        YASSERT(!Stack.empty());
        auto result = Stack.back();
        Stack.pop_back();
        return result;
    }

    INode::TPtr Peek()
    {
        YASSERT(!Stack.empty());
        return Stack.back();
    }
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

    virtual IMapNode::TConstPtr GetAttributes() const
    {
        return Attributes;
    }

    virtual void SetAttributes(IMapNode::TConstPtr attributes)
    {
        if (~Attributes != NULL) {
            Attributes->AsMutable()->SetParent(NULL);
            Attributes = NULL;
        }
        Attributes = attributes;
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
    IMapNode::TConstPtr Attributes;

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

#define DECLARE_TYPE_OVERRIDES(name) \
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
    }

#define DECLARE_SCALAR_TYPE(name, type) \
    class T ## name ## Node \
        : public TScalarNode<type, I ## name ## Node> \
    { \
    public: \
        DECLARE_TYPE_OVERRIDES(name) \
    };

DECLARE_SCALAR_TYPE(String, Stroka)
DECLARE_SCALAR_TYPE(Int64, i64)
DECLARE_SCALAR_TYPE(Double, double)

class TMapNode
    : public TNodeBase<IMapNode>
{
public:
    DECLARE_TYPE_OVERRIDES(Map)

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
        auto it = Map.find(name);
        if (it == Map.end())
            return false;

        it->Second()->AsMutable()->SetParent(NULL);
        Map.erase(it);
        return true;
    }

private:
    yhash_map<Stroka, INode::TConstPtr> Map;

};

class TListNode
    : public TNodeBase<IListNode>
{
public:
    DECLARE_TYPE_OVERRIDES(List)

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

        List[index]->AsMutable()->SetParent(NULL);
        List.erase(List.begin() + index);
        return true;
    }

private:
    yvector<INode::TConstPtr> List;

};

class TEntityNode
    : public TNodeBase<IEntityNode>
{
public:
    DECLARE_TYPE_OVERRIDES(Entity)
};

#undef DECLARE_SCALAR_TYPE
#undef DECLARE_TYPE_OVERRIDES
   

class TNodeFactory
    : INodeFactory
{
public:
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

    virtual IEntityNode::TPtr CreateEntity()
    {
        return ~New<TEntityNode>();
    }
};

} // namespace NEphemeral

////////////////////////////////////////////////////////////////////////////////


} // namespace NRegistry
} // namespace NYT

