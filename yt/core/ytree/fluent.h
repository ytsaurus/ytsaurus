#pragma once

#include "public.h"
#include <core/yson/consumer.h>
#include "yson_producer.h"
#include <core/yson/parser.h>
#include "tree_visitor.h"
#include "tree_builder.h"
#include "convert.h"

#include <core/actions/callback.h>

#include <core/misc/string.h>

#include <core/ytree/attribute_helpers.h>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

template <class T>
struct TFluentYsonUnwrapper
{
    typedef T TUnwrapped;

    static TUnwrapped Unwrap(T t)
    {
        return std::move(t);
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
    static void WriteValue(NYson::IYsonConsumer* consumer, const T& value)
    {
        Consume(value, consumer);
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
    public:
        operator NYson::IYsonConsumer* () const
        {
            return Consumer;
        }

    protected:
        NYson::IYsonConsumer* Consumer;
        TParent Parent;

        TFluentBase(NYson::IYsonConsumer* consumer, TParent parent)
            : Consumer(consumer)
            , Parent(std::move(parent))
        { }

        typedef typename TFluentYsonUnwrapper<TParent>::TUnwrapped TUnwrappedParent;

        TUnwrappedParent GetUnwrappedParent()
        {
            return TFluentYsonUnwrapper<TParent>::Unwrap(std::move(Parent));
        }

    };

    template <template <class TParent> class TThis, class TParent>
    class TFluentFragmentBase
        : public TFluentBase<TParent>
    {
    public:
        typedef TThis<TParent> TDeepThis;
        typedef TThis<TFluentYsonVoid> TShallowThis;
        typedef typename TFluentYsonUnwrapper<TParent>::TUnwrapped TUnwrappedParent;

        explicit TFluentFragmentBase(NYson::IYsonConsumer* consumer, TParent parent = TParent())
            : TFluentBase<TParent>(consumer, std::move(parent))
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

        TDeepThis& DoIf(bool condition, TYsonProducer producer)
        {
            if (condition) {
                producer.Run(this->Consumer);
            }
            return *static_cast<TDeepThis*>(this);
        }

        TDeepThis& DoIf(bool condition, TYsonCallback ysonCallback)
        {
            return DoIf(condition, TYsonProducer(ysonCallback));
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
            for (const auto& item : collection) {
                func(TShallowThis(this->Consumer), item);
            }
            return *static_cast<TDeepThis*>(this);
        }

    };

    template <class TParent>
    class TAnyWithoutAttributes
        : public TFluentBase<TParent>
    {
    public:
        typedef typename TFluentYsonUnwrapper<TParent>::TUnwrapped TUnwrappedParent;

        TAnyWithoutAttributes(NYson::IYsonConsumer* consumer, TParent parent)
            : TFluentBase<TParent>(consumer, std::move(parent))
        { }

        TUnwrappedParent Do(TYsonProducer producer)
        {
            producer.Run(this->Consumer);
            return this->GetUnwrappedParent();
        }

        template <class T>
        TUnwrappedParent Value(const T& value)
        {
            WriteValue(this->Consumer, value);
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
            for (const auto& item : collection) {
                this->Consumer->OnListItem();
                WriteValue(this->Consumer, item);
            }
            this->Consumer->OnEndList();
            return this->GetUnwrappedParent();
        }

        template <class TCollection>
        TUnwrappedParent ListLimited(const TCollection& collection, size_t maxSize)
        {
            this->Consumer->OnBeginAttributes();
            this->Consumer->OnKeyedItem("count");
            this->Consumer->OnInt64Scalar(collection.size());
            this->Consumer->OnEndAttributes();
            this->Consumer->OnBeginList();
            size_t printedSize = 0;
            for (const auto& item : collection) {
                if (printedSize >= maxSize)
                    break;
                this->Consumer->OnListItem();
                WriteValue(this->Consumer, item);
                ++printedSize;
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
            for (const auto& item : collection) {
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
            for (const auto& item : collection) {
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

        explicit TAny(NYson::IYsonConsumer* consumer, TParent parent)
            : TBase(consumer, std::move(parent))
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
        typedef typename TFluentYsonUnwrapper<TParent>::TUnwrapped TUnwrappedParent;

        TAttributes(NYson::IYsonConsumer* consumer, TParent parent)
            : TFluentFragmentBase<TFluentYsonBuilder::TAttributes, TParent>(consumer, std::move(parent))
        { }

        TAny<TThis> Item(const TStringBuf& key)
        {
            this->Consumer->OnKeyedItem(key);
            return TAny<TThis>(this->Consumer, *this);
        }

        template <size_t Size>
        TAny<TThis> Item(const char (&key)[Size])
        {
            return Item(TStringBuf(key, Size - 1));
        }

        TThis& Items(IMapNodePtr map)
        {
            for (const auto& pair : map->GetChildren()) {
                this->Consumer->OnKeyedItem(pair.first);
                VisitTree(pair.second, this->Consumer);
            }
            return *this;
        }

        TThis& Items(const IAttributeDictionary& attributes)
        {
            for (const auto& key : attributes.List()) {
                const auto& yson = attributes.GetYson(key);
                this->Consumer->OnKeyedItem(key);
                this->Consumer->OnRaw(yson.Data(), NYson::EYsonType::Node);
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
        typedef typename TFluentYsonUnwrapper<TParent>::TUnwrapped TUnwrappedParent;

        explicit TList(NYson::IYsonConsumer* consumer, TParent parent = TParent())
            : TFluentFragmentBase<TFluentYsonBuilder::TList, TParent>(consumer, std::move(parent))
        { }

        TAny<TThis> Item()
        {
            this->Consumer->OnListItem();
            return TAny<TThis>(this->Consumer, *this);
        }

        TThis& Items(IListNodePtr list)
        {
            for (auto item : list->GetChildren()) {
                this->Consumer->OnListItem();
                VisitTree(std::move(item), this->Consumer);
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
        typedef typename TFluentYsonUnwrapper<TParent>::TUnwrapped TUnwrappedParent;

        explicit TMap(NYson::IYsonConsumer* consumer, TParent parent = TParent())
            : TFluentFragmentBase<TFluentYsonBuilder::TMap, TParent>(consumer, std::move(parent))
        { }

        template <size_t Size>
        TAny<TThis> Item(const char (&key)[Size])
        {
            return Item(TStringBuf(key, Size - 1));
        }

        TAny<TThis> Item(const TStringBuf& key)
        {
            this->Consumer->OnKeyedItem(key);
            return TAny<TThis>(this->Consumer, *this);
        }

        TThis& Items(IMapNodePtr map)
        {
            for (const auto& pair : map->GetChildren()) {
                this->Consumer->OnKeyedItem(pair.first);
                VisitTree(pair.second, this->Consumer);
            }
            return *this;
        }

        TThis& Items(const IAttributeDictionary& attributes)
        {
            for (const auto& key : attributes.List()) {
                const auto& yson = attributes.GetYson(key);
                this->Consumer->OnKeyedItem(key);
                this->Consumer->OnRaw(yson.Data(), NYson::EYsonType::Node);
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

static inline TFluentYsonBuilder::TAny<TFluentYsonVoid> BuildYsonFluently(NYson::IYsonConsumer* consumer)
{
    return TFluentYsonBuilder::TAny<TFluentYsonVoid>(consumer, TFluentYsonVoid());
}

static inline TFluentList BuildYsonListFluently(NYson::IYsonConsumer* consumer)
{
    return TFluentList(consumer);
}

static inline TFluentMap BuildYsonMapFluently(NYson::IYsonConsumer* consumer)
{
    return TFluentMap(consumer);
}

////////////////////////////////////////////////////////////////////////////////

class TFluentYsonWriterState
    : public TIntrinsicRefCounted
{
public:
    typedef TYsonString TValue;

    explicit TFluentYsonWriterState(NYson::EYsonFormat format)
        : Writer(&Output, format)
    { }

    TYsonString GetValue()
    {
        return TYsonString(Output.Str());
    }

    NYson::IYsonConsumer* GetConsumer()
    {
        return &Writer;
    }

private:
    TStringStream Output;
    NYson::TYsonWriter Writer;

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

    NYson::IYsonConsumer* GetConsumer()
    {
        return Builder.get();
    }

private:
    std::unique_ptr<ITreeBuilder> Builder;

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
    NYson::EYsonFormat format = NYson::EYsonFormat::Binary)
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

