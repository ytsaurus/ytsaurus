#pragma once

#include "public.h"
#include "yson_consumer.h"
#include "yson_producer.h"
#include "yson_parser.h"
#include "tree_visitor.h"
#include "tree_builder.h"
#include "convert.h"

#include <ytlib/actions/callback.h>

#include <ytlib/misc/foreach.h>
#include <ytlib/misc/guid.h>
#include <ytlib/misc/string.h>

#include <ytlib/ytree/attribute_helpers.h>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

template <class T>
struct TFluentYsonUnwrapper
{
    typedef T TUnwrapped;

    static TUnwrapped Unwrap(const T& t)
    {
        return t;
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TFluentYsonVoid
{ };

template <>
struct TFluentYsonUnwrapper<TFluentYsonVoid>
{
    typedef void TUnwrapped;

    static TUnwrapped Unwrap(TFluentYsonVoid)
    { }
};

////////////////////////////////////////////////////////////////////////////////

class TFluentYsonBuilder
    : private TNonCopyable
{
private:
    template <class T>
    static void WriteScalar(IYsonConsumer* consumer, const T& value)
    {
        Consume(value, consumer);
    }

    static void WriteScalar(IYsonConsumer* consumer, const Stroka& value)
    {
        Consume(TRawString(value), consumer);
    }

public:
    class TFluentAny;
    template <class TParent> class TAny;
    template <class TParent> class TToAttributes;
    template <class TParent> class TAttributes;
    template <class TParent> class TList;
    template <class TParent> class TMap;

    template <class TParent>
    class TFluentBase
    {
    protected:
        IYsonConsumer* Consumer;
        TParent Parent;

        TFluentBase(IYsonConsumer* consumer, const TParent& parent)
            : Consumer(consumer)
            , Parent(parent)
        { }

        typedef typename TFluentYsonUnwrapper<TParent>::TUnwrapped TUnwrappedParent;

        TUnwrappedParent GetUnwrappedParent()
        {
            return TFluentYsonUnwrapper<TParent>::Unwrap(Parent);
        }

    };

    template <template <class TParent> class TThis, class TParent>
    class TFluentFragmentBase
        : public TFluentBase<TParent>
    {
    public:
        typedef TThis<TParent> TDeepThis;
        typedef TThis<TFluentYsonVoid> TShallowThis;

        explicit TFluentFragmentBase(IYsonConsumer* consumer, const TParent& parent = TParent())
            : TFluentBase<TParent>(consumer, parent)
        { }

        TDeepThis& Do(TYsonProducer producer)
        {
            producer.Run(this->Consumer);
            return *static_cast<TDeepThis*>(this);
        }
        
        TDeepThis& Do(TYsonCallback ysonCallback)
        {
            return Do(TYsonProducer(ysonCallback));
        }

        template <class TFunc>
        TDeepThis& Do(const TFunc& func)
        {
            func(TShallowThis(this->Consumer));
            return *static_cast<TDeepThis*>(this);
        }

        template <class TFunc>
        TDeepThis& DoIf(bool condition, const TFunc& func)
        {
            if (condition) {
                func(TShallowThis(this->Consumer));
            }
            return *static_cast<TDeepThis*>(this);
        }

        template <class TFunc, class TIterator>
        TDeepThis& DoFor(const TIterator& begin, const TIterator& end, const TFunc& func)
        {
            for (auto current = begin; current != end; ++current) {
                func(TShallowThis(this->Consumer), current);
            }
            return *static_cast<TDeepThis*>(this);
        }

        template <class TFunc, class TCollection>
        TDeepThis& DoFor(const TCollection& collection, const TFunc& func)
        {
            FOREACH (const auto& item, collection) {
                func(TShallowThis(this->Consumer), item);
            }
            return *static_cast<TDeepThis*>(this);
        }

    protected :
    };

    template <class TParent>
    class TAnyWithoutAttributes
        : public TFluentBase<TParent>
    {
    public:
        TAnyWithoutAttributes(IYsonConsumer* consumer, const TParent& parent)
            : TFluentBase<TParent>(consumer, parent)
        { }

        TUnwrappedParent Do(TYsonProducer producer)
        {
            producer.Run(this->Consumer);
            return this->GetUnwrappedParent();
        }

        template <class T>
        TUnwrappedParent Scalar(T value)
        {
            WriteScalar(this->Consumer, value);
            return this->GetUnwrappedParent();
        }

        TUnwrappedParent Node(const TYsonString& value)
        {
            Consume(value, this->Consumer);
            return this->GetUnwrappedParent();
        }

        TUnwrappedParent Node(INodePtr node)
        {
            VisitTree(node, this->Consumer);
            return this->GetUnwrappedParent();
        }

        TUnwrappedParent Entity()
        {
            this->Consumer->OnEntity();
            return this->GetUnwrappedParent();
        }

        template <class TCollection>
        TUnwrappedParent List(const TCollection& collection)
        {
            this->Consumer->OnBeginList();
            FOREACH (const auto& item, collection) {
                this->Consumer->OnListItem();
                WriteScalar(this->Consumer, item);
            }
            this->Consumer->OnEndList();
            return this->GetUnwrappedParent();
        }

        TList<TParent> BeginList()
        {
            this->Consumer->OnBeginList();
            return TList<TParent>(this->Consumer, this->Parent);
        }

        template <class TFunc>
        TUnwrappedParent DoList(const TFunc& func)
        {
            this->Consumer->OnBeginList();
            func(TList<TFluentYsonVoid>(this->Consumer));
            this->Consumer->OnEndList();
            return this->GetUnwrappedParent();
        }

        template <class TFunc, class TIterator>
        TUnwrappedParent DoListFor(const TIterator& begin, const TIterator& end, const TFunc& func)
        {
            this->Consumer->OnBeginList();
            for (auto current = begin; current != end; ++current) {
                func(TList<TFluentYsonVoid>(this->Consumer), current);
            }
            this->Consumer->OnEndList();
            return this->GetUnwrappedParent();
        }

        template <class TFunc, class TCollection>
        TUnwrappedParent DoListFor(const TCollection& collection, const TFunc& func)
        {
            this->Consumer->OnBeginList();
            FOREACH (const auto& item, collection) {
                func(TList<TFluentYsonVoid>(this->Consumer), item);
            }
            this->Consumer->OnEndList();
            return this->GetUnwrappedParent();
        }

        TMap<TParent> BeginMap()
        {
            this->Consumer->OnBeginMap();
            return TMap<TParent>(this->Consumer, this->Parent);
        }

        template <class TFunc>
        TUnwrappedParent DoMap(const TFunc& func)
        {
            this->Consumer->OnBeginMap();
            func(TMap<TFluentYsonVoid>(this->Consumer));
            this->Consumer->OnEndMap();
            return this->GetUnwrappedParent();
        }

        template <class TFunc, class TIterator>
        TUnwrappedParent DoMapFor(const TIterator& begin, const TIterator& end, const TFunc& func)
        {
            this->Consumer->OnBeginMap();
            for (auto current = begin; current != end; ++current) {
                func(TMap<TFluentYsonVoid>(this->Consumer), current);
            }
            this->Consumer->OnEndMap();
            return this->GetUnwrappedParent();
        }

        template <class TFunc, class TCollection>
        TUnwrappedParent DoMapFor(const TCollection& collection, const TFunc& func)
        {
            this->Consumer->OnBeginMap();
            FOREACH (const auto& item, collection) {
                func(TMap<TFluentYsonVoid>(this->Consumer), item);
            }
            this->Consumer->OnEndMap();
            return this->GetUnwrappedParent();
        }
    };

    template <class TParent>
    class TAny
        : public TAnyWithoutAttributes<TParent>
    {
    public:
        typedef TAnyWithoutAttributes<TParent> TBase;

        explicit TAny(IYsonConsumer* consumer, const TParent& parent)
            : TBase(consumer, parent)
        { }

        TAttributes<TBase> BeginAttributes()
        {
            this->Consumer->OnBeginAttributes();
            return TAttributes<TBase>(
                this->Consumer,
                TBase(this->Consumer, this->Parent));
        }
    };

    template <class TParent>
    class TAttributes
        : public TFluentFragmentBase<TAttributes, TParent>
    {
    public:
        typedef TAttributes<TParent> TThis;

        TAttributes(IYsonConsumer* consumer, const TParent& parent)
            : TFluentFragmentBase<TFluentYsonBuilder::TAttributes, TParent>(consumer, parent)
        { }

        TAny<TThis> Item(const Stroka& key)
        {
            this->Consumer->OnKeyedItem(key);
            return TAny<TThis>(this->Consumer, *this);
        }

        TThis& Items(IMapNodePtr map)
        {
            FOREACH (const auto& pair, map->GetChildren()) {
                this->Consumer->OnKeyedItem(pair.first);
                VisitTree(~pair.second, this->Consumer);
            }
            return *this;
        }

        TThis& Items(const IAttributeDictionary& attributes)
        {
            FOREACH (const auto& key, attributes.List()) {
                const auto& yson = attributes.GetYson(key);
                this->Consumer->OnKeyedItem(key);
                this->Consumer->OnRaw(yson.Data(), EYsonType::Node);
            }
            return *this;
        }

        TUnwrappedParent EndAttributes()
        {
            this->Consumer->OnEndAttributes();
            return this->GetUnwrappedParent();
        }
    };

    template <class TParent = TFluentYsonVoid>
    class TList
        : public TFluentFragmentBase<TList, TParent>
    {
    public:
        typedef TList<TParent> TThis;

        explicit TList(IYsonConsumer* consumer, const TParent& parent = TParent())
            : TFluentFragmentBase<TFluentYsonBuilder::TList, TParent>(consumer, parent)
        { }

        TAny<TThis> Item()
        {
            this->Consumer->OnListItem();
            return TAny<TThis>(this->Consumer, *this);
        }

        TThis& Items(IListNodePtr list)
        {
            FOREACH (const auto& item, list->GetChildren()) {
                this->Consumer->OnListItem();
                VisitTree(~item, this->Consumer);
            }
            return *this;
        }

        TUnwrappedParent EndList()
        {
            this->Consumer->OnEndList();
            return this->GetUnwrappedParent();
        }
    };

    template <class TParent = TFluentYsonVoid>
    class TMap
        : public TFluentFragmentBase<TMap, TParent>
    {
    public:
        typedef TMap<TParent> TThis;

        explicit TMap(IYsonConsumer* consumer, const TParent& parent = TParent())
            : TFluentFragmentBase<TFluentYsonBuilder::TMap, TParent>(consumer, parent)
        { }

        TAny<TThis> Item(const Stroka& key)
        {
            this->Consumer->OnKeyedItem(key);
            return TAny<TThis>(this->Consumer, *this);
        }

        TThis& Items(IMapNodePtr map)
        {
            FOREACH (const auto& pair, map->GetChildren()) {
                this->Consumer->OnKeyedItem(pair.first);
                VisitTree(~pair.second, this->Consumer);
            }
            return *this;
        }

        TThis& Items(IAttributeDictionary* attributes)
        {
            FOREACH (const auto& key, attributes->List()) {
                const auto& yson = attributes->GetYson(key);
                this->Consumer->OnKeyedItem(key);
                ParseYson(yson, this->Consumer);
            }
            return *this;
        }

        TUnwrappedParent EndMap()
        {
            this->Consumer->OnEndMap();
            return this->GetUnwrappedParent();
        }
    };

};

typedef TFluentYsonBuilder::TList<TFluentYsonVoid> TFluentList;
typedef TFluentYsonBuilder::TMap<TFluentYsonVoid> TFluentMap;

////////////////////////////////////////////////////////////////////////////////

static inline TFluentYsonBuilder::TAny<TFluentYsonVoid> BuildYsonFluently(IYsonConsumer* consumer)
{
    return TFluentYsonBuilder::TAny<TFluentYsonVoid>(consumer, TFluentYsonVoid());
}

static inline TFluentList BuildYsonListFluently(IYsonConsumer* consumer)
{
    return TFluentList(consumer);
}

static inline TFluentMap BuildYsonMapFluently(IYsonConsumer* consumer)
{
    return TFluentMap(consumer);
}

////////////////////////////////////////////////////////////////////////////////

class TFluentYsonWriterState
    : public TIntrinsicRefCounted
{
public:
    typedef TYsonString TValue;

    explicit TFluentYsonWriterState(EYsonFormat format)
        : Writer(&Output, format)
    { }

    TYsonString GetValue()
    {
        return TYsonString(Output.Str());
    }

    IYsonConsumer* GetConsumer()
    {
        return &Writer;
    }

private:
    TStringStream Output;
    TYsonWriter Writer;

};

////////////////////////////////////////////////////////////////////////////////

class TFluentYsonBuilderState
    : public TIntrinsicRefCounted
{
public:
    typedef INodePtr TValue;

    explicit TFluentYsonBuilderState(INodeFactoryPtr factory)
        : Builder(CreateBuilderFromFactory(factory))
    { }

    INodePtr GetValue()
    {
        return Builder->EndTree();
    }

    IYsonConsumer* GetConsumer()
    {
        return ~Builder;
    }

private:
    TAutoPtr<ITreeBuilder> Builder;

};

////////////////////////////////////////////////////////////////////////////////

template <class TState>
class TFluentYsonHolder
{
public:
    explicit TFluentYsonHolder(TIntrusivePtr<TState> state)
        : State(state)
    { }

    TIntrusivePtr<TState> GetState() const
    {
        return State;
    }

private:
    TIntrusivePtr<TState> State;

};

////////////////////////////////////////////////////////////////////////////////

template <class TState>
struct TFluentYsonUnwrapper< TFluentYsonHolder<TState> >
{
    typedef typename TState::TValue TUnwrapped;

    static TUnwrapped Unwrap(const TFluentYsonHolder<TState>& holder)
    {
        return holder.GetState()->GetValue();
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class TState>
TFluentYsonBuilder::TAny< TFluentYsonHolder<TState> > BuildYsonFluentlyWithState(
    TIntrusivePtr<TState> state)
{
    return TFluentYsonBuilder::TAny< TFluentYsonHolder<TState> >(
        state->GetConsumer(),
        TFluentYsonHolder<TState>(state));
}

inline TFluentYsonBuilder::TAny< TFluentYsonHolder<TFluentYsonWriterState> > BuildYsonStringFluently(
    EYsonFormat format = EYsonFormat::Binary)
{
    return BuildYsonFluentlyWithState(New<TFluentYsonWriterState>(format));
}

inline TFluentYsonBuilder::TAny< TFluentYsonHolder<TFluentYsonBuilderState> > BuildYsonNodeFluently(
    INodeFactoryPtr factory = GetEphemeralNodeFactory())
{
    return BuildYsonFluentlyWithState(New<TFluentYsonBuilderState>(factory));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

