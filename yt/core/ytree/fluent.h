#pragma once

#include "public.h"
#include "tree_visitor.h"
#include "tree_builder.h"
#include "convert.h"

#include <yt/core/yson/consumer.h>
#include <yt/core/yson/producer.h>
#include <yt/core/yson/parser.h>

#include <yt/core/actions/callback.h>

#include <yt/core/misc/string.h>

#include <yt/core/ytree/helpers.h>

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

template <class TFluent, class TFunc, class... TArgs>
void InvokeFluentFunc(TFunc func, NYson::IYsonConsumer* consumer, TArgs&&... args)
{
    func(TFluent(consumer), std::forward<TArgs>(args)...);
}

////////////////////////////////////////////////////////////////////////////////

class TFluentYsonBuilder
    : private TNonCopyable
{
private:
    template <class T>
    static void WriteValue(NYson::IYsonConsumer* consumer, const T& value)
    {
        Serialize(value, consumer);
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
        typedef typename TFluentYsonUnwrapper<TParent>::TUnwrapped TUnwrappedParent;

        operator NYson::IYsonConsumer* () const
        {
            return Consumer;
        }

        TUnwrappedParent Finish()
        {
            return GetUnwrappedParent();
        }

    protected:
        NYson::IYsonConsumer* Consumer;
        TParent Parent;
        bool Unwrapped_ = false;

        TFluentBase(NYson::IYsonConsumer* consumer, TParent parent)
            : Consumer(consumer)
            , Parent(std::move(parent))
        { }

        TUnwrappedParent GetUnwrappedParent()
        {
            YCHECK(!Unwrapped_);
            Unwrapped_ = true;
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

        template <class TFunc>
        TDeepThis& Do(TFunc func)
        {
            InvokeFluentFunc<TShallowThis>(func, this->Consumer);
            return *static_cast<TDeepThis*>(this);
        }

        template <class TFunc>
        TDeepThis& DoIf(bool condition, TFunc func)
        {
            if (condition) {
                InvokeFluentFunc<TShallowThis>(func, this->Consumer);
            }
            return *static_cast<TDeepThis*>(this);
        }

        template <class TFunc, class TIterator>
        TDeepThis& DoFor(TIterator begin, TIterator end, TFunc func)
        {
            for (auto current = begin; current != end; ++current) {
                InvokeFluentFunc<TShallowThis>(func, this->Consumer, current);
            }
            return *static_cast<TDeepThis*>(this);
        }

        template <class TFunc, class TCollection>
        TDeepThis& DoFor(const TCollection& collection, TFunc func)
        {
            for (const auto& item : collection) {
                InvokeFluentFunc<TShallowThis>(func, this->Consumer, item);
            }
            return *static_cast<TDeepThis*>(this);
        }
    };

    template <class TParent>
    class TAnyWithoutAttributes
        : public TFluentBase<TParent>
    {
    public:
        typedef TAnyWithoutAttributes<TParent> TDeepThis;
        typedef TAnyWithoutAttributes<TFluentYsonVoid> TShallowThis;
        typedef typename TFluentYsonUnwrapper<TParent>::TUnwrapped TUnwrappedParent;

        TAnyWithoutAttributes(NYson::IYsonConsumer* consumer, TParent parent = TParent())
            : TFluentBase<TParent>(consumer, std::move(parent))
        { }

        template <class TFunc>
        TUnwrappedParent Do(TFunc func)
        {
            InvokeFluentFunc<TShallowThis>(func, this->Consumer);
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
        TUnwrappedParent DoList(TFunc func)
        {
            this->Consumer->OnBeginList();
            InvokeFluentFunc<TList<TFluentYsonVoid>>(func, this->Consumer);
            this->Consumer->OnEndList();
            return this->GetUnwrappedParent();
        }

        template <class TFunc, class TIterator>
        TUnwrappedParent DoListFor(TIterator begin, TIterator end, TFunc func)
        {
            this->Consumer->OnBeginList();
            for (auto current = begin; current != end; ++current) {
                InvokeFluentFunc<TList<TFluentYsonVoid>>(func, this->Consumer, current);
            }
            this->Consumer->OnEndList();
            return this->GetUnwrappedParent();
        }

        template <class TFunc, class TCollection>
        TUnwrappedParent DoListFor(const TCollection& collection, TFunc func)
        {
            this->Consumer->OnBeginList();
            for (const auto& item : collection) {
                InvokeFluentFunc<TList<TFluentYsonVoid>>(func, this->Consumer, item);
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
        TUnwrappedParent DoMap(TFunc func)
        {
            this->Consumer->OnBeginMap();
            InvokeFluentFunc<TMap<TFluentYsonVoid>>(func, this->Consumer);
            this->Consumer->OnEndMap();
            return this->GetUnwrappedParent();
        }

        template <class TFunc, class TIterator>
        TUnwrappedParent DoMapFor(TIterator begin, TIterator end, TFunc func)
        {
            this->Consumer->OnBeginMap();
            for (auto current = begin; current != end; ++current) {
                InvokeFluentFunc<TMap<TFluentYsonVoid>>(func, this->Consumer, current);
            }
            this->Consumer->OnEndMap();
            return this->GetUnwrappedParent();
        }

        template <class TFunc, class TCollection>
        TUnwrappedParent DoMapFor(const TCollection& collection, TFunc func)
        {
            this->Consumer->OnBeginMap();
            for (const auto& item : collection) {
                InvokeFluentFunc<TMap<TFluentYsonVoid>>(func, this->Consumer, item);
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

        explicit TAny(NYson::IYsonConsumer* consumer, TParent parent = TParent())
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

    template <class TParent = TFluentYsonVoid>
    class TAttributes
        : public TFluentFragmentBase<TAttributes, TParent>
    {
    public:
        typedef TAttributes<TParent> TThis;
        typedef typename TFluentYsonUnwrapper<TParent>::TUnwrapped TUnwrappedParent;

        explicit TAttributes(NYson::IYsonConsumer* consumer, TParent parent = TParent())
            : TFluentFragmentBase<TFluentYsonBuilder::TAttributes, TParent>(consumer, std::move(parent))
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

        TThis& Items(const IMapNodePtr& map)
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
                this->Consumer->OnRaw(yson);
            }
            return *this;
        }

        TThis& Items(const NYson::TYsonString& attributes)
        {
            YCHECK(attributes.GetType() == NYson::EYsonType::MapFragment);
            this->Consumer->OnRaw(attributes);
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

        TThis& Items(const IListNodePtr& list)
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

        TThis& Items(const IMapNodePtr& map)
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
                this->Consumer->OnRaw(yson);
            }
            return *this;
        }

        TThis& Items(const NYson::TYsonString& attributes)
        {
            YCHECK(attributes.GetType() == NYson::EYsonType::MapFragment);
            this->Consumer->OnRaw(attributes);
            return *this;
        }

        TUnwrappedParent EndMap()
        {
            this->Consumer->OnEndMap();
            return this->GetUnwrappedParent();
        }
    };
};

////////////////////////////////////////////////////////////////////////////////

template <NYson::EYsonType>
struct TFluentType;

template <>
struct TFluentType<NYson::EYsonType::Node>
{
    template <class T>
    using TValue = TFluentYsonBuilder::TAny<T>;
};

template <>
struct TFluentType<NYson::EYsonType::MapFragment>
{
    template <class T>
    using TValue = TFluentYsonBuilder::TMap<T>;
};

template <>
struct TFluentType<NYson::EYsonType::ListFragment>
{
    template <class T>
    using TValue = TFluentYsonBuilder::TList<T>;
};

using TFluentList = TFluentYsonBuilder::TList<TFluentYsonVoid>;
using TFluentMap = TFluentYsonBuilder::TMap<TFluentYsonVoid>;
using TFluentAttributes = TFluentYsonBuilder::TAttributes<TFluentYsonVoid>;

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

static inline TFluentAttributes BuildYsonAttributesFluently(NYson::IYsonConsumer* consumer)
{
    return TFluentAttributes(consumer);
}

////////////////////////////////////////////////////////////////////////////////

class TFluentYsonWriterState
    : public TIntrinsicRefCounted
{
public:
    typedef NYson::TYsonString TValue;

    TFluentYsonWriterState(NYson::EYsonFormat format, NYson::EYsonType type)
        : Writer(&Output, format, type)
        , Type(type)
    { }

    NYson::TYsonString GetValue()
    {
        return NYson::TYsonString(Output.Str(), Type);
    }

    NYson::IYsonConsumer* GetConsumer()
    {
        return &Writer;
    }

private:
    TStringStream Output;
    NYson::TYsonWriter Writer;
    NYson::EYsonType Type;

};

////////////////////////////////////////////////////////////////////////////////

class TFluentYsonBuilderState
    : public TIntrinsicRefCounted
{
public:
    typedef INodePtr TValue;

    explicit TFluentYsonBuilderState(INodeFactory* factory)
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
    const std::unique_ptr<ITreeBuilder> Builder;

};

////////////////////////////////////////////////////////////////////////////////

template <class TState>
class TFluentYsonHolder
{
public:
    explicit TFluentYsonHolder(TIntrusivePtr<TState> state)
        : State(std::move(state))
    { }

    TIntrusivePtr<TState> GetState() const
    {
        return State;
    }

private:
    const TIntrusivePtr<TState> State;

};

////////////////////////////////////////////////////////////////////////////////

template <class TState>
struct TFluentYsonUnwrapper<TFluentYsonHolder<TState>>
{
    typedef typename TState::TValue TUnwrapped;

    static TUnwrapped Unwrap(const TFluentYsonHolder<TState>& holder)
    {
        return holder.GetState()->GetValue();
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class TState, NYson::EYsonType type>
auto BuildYsonFluentlyWithState(TIntrusivePtr<TState> state)
{
    typedef typename TFluentType<type>::template TValue<TFluentYsonHolder<TState>> TReturnType;
    return TReturnType(
        state->GetConsumer(),
        TFluentYsonHolder<TState>(state));
}

template <NYson::EYsonType type = NYson::EYsonType::Node>
auto BuildYsonStringFluently(NYson::EYsonFormat format = NYson::EYsonFormat::Binary)
{
    return BuildYsonFluentlyWithState<TFluentYsonWriterState, type>(New<TFluentYsonWriterState>(format, type));
}

inline auto BuildYsonNodeFluently(INodeFactory* factory = GetEphemeralNodeFactory())
{
    return BuildYsonFluentlyWithState<TFluentYsonBuilderState, NYson::EYsonType::Node>(New<TFluentYsonBuilderState>(factory));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

