#pragma once

#include "public.h"
#include "yson_consumer.h"
#include "yson_producer.h"
#include "yson_parser.h"
#include "tree_visitor.h"
#include "convert.h"

#include <ytlib/actions/callback.h>

#include <ytlib/misc/foreach.h>
#include <ytlib/misc/guid.h>
#include <ytlib/misc/string.h>

#include <ytlib/ytree/attribute_helpers.h>

namespace NYT {
namespace NYTree {

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

    struct TNoParentTag
    { };

    template <class TParent>
    class TFluentBase
    {
    protected:
        TFluentBase(IYsonConsumer* consumer, const TParent& parent)
            : Consumer(consumer)
            , Parent(parent)
        { }

        IYsonConsumer* Consumer;
        TParent Parent;

    };

    template <template <class TParent> class TThis, class TParent>
    class TFluentFragmentBase
        : public TFluentBase<TParent>
    {
    public:
        typedef TThis<TParent> TDeepThis;
        typedef TThis<TNoParentTag> TShallowThis;

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
    };

    template <class TParent>
    class TAnyWithoutAttributes
        : public TFluentBase<TParent>
    {
    public:
        TAnyWithoutAttributes(IYsonConsumer* consumer, const TParent& parent)
            : TFluentBase<TParent>(consumer, parent)
        { }

        TParent Do(TYsonProducer producer)
        {
            producer.Run(this->Consumer);
            return this->Parent;
        }

        template <class T>
        TParent Scalar(T value)
        {
            WriteScalar(this->Consumer, value);
            return this->Parent;
        }

        TParent Node(const TYsonString& value)
        {
            Consume(value, this->Consumer);
            return this->Parent;
        }

        TParent Node(INodePtr node)
        {
            VisitTree(node, this->Consumer);
            return this->Parent;
        }

        TParent Entity()
        {
            this->Consumer->OnEntity();
            return this->Parent;
        }

        template <class TCollection>
        TParent List(const TCollection& collection)
        {
            this->Consumer->OnBeginList();
            FOREACH (const auto& item, collection) {
                this->Consumer->OnListItem();
                WriteScalar(this->Consumer, item);
            }
            this->Consumer->OnEndList();
            return this->Parent;
        }

        TList<TParent> BeginList()
        {
            this->Consumer->OnBeginList();
            return TList<TParent>(this->Consumer, this->Parent);
        }

        template <class TFunc>
        TParent DoList(const TFunc& func)
        {
            this->Consumer->OnBeginList();
            func(TList<TNoParentTag>(this->Consumer));
            this->Consumer->OnEndList();
            return this->Parent;
        }

        template <class TFunc, class TIterator>
        TParent DoListFor(const TIterator& begin, const TIterator& end, const TFunc& func)
        {
            this->Consumer->OnBeginList();
            for (auto current = begin; current != end; ++current) {
                func(TList<TNoParentTag>(this->Consumer), current);
            }
            this->Consumer->OnEndList();
            return this->Parent;
        }

        template <class TFunc, class TCollection>
        TParent DoListFor(const TCollection& collection, const TFunc& func)
        {
            this->Consumer->OnBeginList();
            FOREACH (const auto& item, collection) {
                func(TList<TNoParentTag>(this->Consumer), item);
            }
            this->Consumer->OnEndList();
            return this->Parent;
        }

        TMap<TParent> BeginMap()
        {
            this->Consumer->OnBeginMap();
            return TMap<TParent>(this->Consumer, this->Parent);
        }

        template <class TFunc>
        TParent DoMap(const TFunc& func)
        {
            this->Consumer->OnBeginMap();
            func(TMap<TNoParentTag>(this->Consumer));
            this->Consumer->OnEndMap();
            return this->Parent;
        }

        template <class TFunc, class TIterator>
        TParent DoMapFor(const TIterator& begin, const TIterator& end, const TFunc& func)
        {
            this->Consumer->OnBeginMap();
            for (auto current = begin; current != end; ++current) {
                func(TMap<TNoParentTag>(this->Consumer), current);
            }
            this->Consumer->OnEndMap();
            return this->Parent;
        }

        template <class TFunc, class TCollection>
        TParent DoMapFor(const TCollection& collection, const TFunc& func)
        {
            this->Consumer->OnBeginMap();
            FOREACH (const auto& item, collection) {
                func(TMap<TNoParentTag>(this->Consumer), item);
            }
            this->Consumer->OnEndMap();
            return this->Parent;
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

        TParent EndAttributes()
        {
            this->Consumer->OnEndAttributes();
            return this->Parent;
        }
    };

    template <class TParent = TNoParentTag>
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

        // TODO(panin): forbid this call for TParent = TNoParentTag
        TParent EndList()
        {
            this->Consumer->OnEndList();
            return this->Parent;
        }
    };

    template <class TParent = TNoParentTag>
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

        // TODO(panin): forbid this call for TParent = TNoParentTag
        TParent EndMap()
        {
            this->Consumer->OnEndMap();
            return this->Parent;
        }
    };

};

typedef TFluentYsonBuilder::TList<TFluentYsonBuilder::TNoParentTag> TFluentList;
typedef TFluentYsonBuilder::TMap<TFluentYsonBuilder::TNoParentTag> TFluentMap;

////////////////////////////////////////////////////////////////////////////////

static inline TFluentYsonBuilder::TAny<TFluentYsonBuilder::TNoParentTag> BuildYsonFluently(IYsonConsumer* consumer)
{
    return TFluentYsonBuilder::TAny<TFluentYsonBuilder::TNoParentTag>(consumer, TFluentYsonBuilder::TNoParentTag());
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

class TFluentYsonConsumer
    : public TIntrinsicRefCounted
{
public:
    typedef TIntrusivePtr<TFluentYsonConsumer> TPtr;

    TFluentYsonConsumer(EYsonFormat format)
        : Writer(&Output, format)
    { }

    const Stroka& GetString() const
    {
        return Output.Str();
    }

    IYsonConsumer* GetConsumer()
    {
        return &Writer;
    }

private:
    TStringStream Output;
    TYsonWriter Writer;
};

class TFluentYsonHolder
{
public:
    TFluentYsonHolder(TFluentYsonConsumer::TPtr consumer)
        : Consumer(consumer)
    { }

    const Stroka& ToString() const
    {
        return Consumer->GetString();
    }
    
    TYsonString GetYsonString() const
    {
        return TYsonString(ToString());
    }

private:
    TFluentYsonConsumer::TPtr Consumer;
};

inline TFluentYsonBuilder::TAny<TFluentYsonHolder> BuildYsonFluently(
    EYsonFormat format = EYsonFormat::Binary)
{
    auto consumer = New<TFluentYsonConsumer>(format);
    TFluentYsonHolder holder(consumer);
    return TFluentYsonBuilder::TAny<TFluentYsonHolder>(
        consumer->GetConsumer(),
        holder);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

