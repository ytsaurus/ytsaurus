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

/*
// WHAT IS THIS
//
// Fluent adapters encapsulate invocation of IYsonConsumer methods in a
// convenient structured manner. Key advantage of fluent-like code is that
// attempt of building syntactically incorrect YSON structure will result
// in a compile-time error.
//
// Each fluent object is associated with a context that defines possible YSON
// tokens that may appear next. For example, TFluentMap is a fluent object
// that corresponds to a location within YSON map right before a key-value
// pair or the end of the map.
//
// More precisely, each object that may be obtained by a sequence of fluent
// method calls has the full history of its enclosing YSON composite types in
// its single template argument hereinafter referred to as TParent. This allows
// us not to forget the original context after opening and closing the embedded
// composite structure.
//
// It is possible to invoke a separate YSON building procedure by calling
// one of convenience Do* methods. There are two possibilities here: it is
// possible to delegate invocation context either as a fluent object (like
// TFluentMap, TFluentList, TFluentAttributes or TFluentAny) or as a raw
// IYsonConsumer*. The latter is discouraged since it is impossible to check
// if a given side-built YSON structure fits current fluent context.
// For example it is possible to call Do() method inside YSON map passing
// consumer to a procedure that will treat context like it is in a list.
// Passing typed fluent builder saves you from such a misbehaviour.
//
// TFluentXxx corresponds to an internal class of TXxx
// without any history hidden in template argument. It allows you to
// write procedures of form:
//
//   void BuildSomeAttributesInYson(TFluentMap fluent) { ... }
//
// without thinking about the exact way how this procedure is nested in other
// procedures.
//
// An important notation: we will refer to a function whose first argument
// is either TFluentXxx or raw IYsonConsumer* as TFuncXxx.
//
//
// BRIEF LIST OF AVAILABLE METHODS
//
// Only the most popular methods are covered here. Refer to the code for the
// rest of them.
//
// TAny:
// * Value(T value) -> TParent, serialize `value` using underlying consumer.
//   T should be such that free function Serialize(IYsonConsumer*, const T&) is
//   defined;
// * BeginMap() -> TMapType, open map;
// * BeginList() -> TListType, open list;
// * BeginAttributes() -> TAttributes, open attributes;
//
// * Do(TFuncAny func) -> TAny, delegate invocation to a separate procedure.
// * DoIf(bool condition, TFuncAny func) -> TAny, same as Do() but invoke
//   `func` only if `condition` is true;
// * DoFor(TCollection collection, TFuncAny func) -> TAny, same as Do()
//   but iterate over `collection` and pass each of its elements as a second
//   argument to `func`. Instead of passing a collection you may it is possible
//   to pass two iterators as an argument;
//
// * DoMap(TFuncMap func) -> TAny, open a map, delegate invocation to a separate
//   procedure and close map;
// * DoMapFor(TCollection collection, TFuncMap func) -> TAny, open a map, iterate
//   over `collection` and pass each of its elements as a second argument to `func`
//   and close map;
// * DoList(TFuncList func) -> TAny, same as DoMap();
// * DoListFor(TCollection collection, TFuncList func) -> TAny; same as DoMapFor().
//
//
// TMapType:
// * Item(TStringBuf key) -> TAny, open an element keyed with `key`;
// * EndMap() -> TParent, close map;
// * Do(TFuncMap func) -> TMapType, same as Do() for TAny;
// * DoIf(bool condition, TFuncMap func) -> TMapType, same as DoIf() for TAny;
// * DoFor(TCollection collection, TFuncMap func) -> TMapType, same as DoFor() for TAny.
//
//
// TListType:
// * Item() -> TAny, open an new list element;
// * EndList() -> TParent, close list;
// * Do(TFuncList func) -> TListType, same as Do() for TAny;
// * DoIf(bool condition, TFuncList func) -> TListType, same as DoIf() for TAny;
// * DoFor(TCollection collection, TListMap func) -> TListType, same as DoFor() for TAny.
//
//
// TAttributes:
// * Item(TStringBuf key) -> TAny, open an element keyed with `key`.
// * EndAttributes() -> TParentWithoutAttributes, close attributes. Note that
//   this method leads to a context that is forces not to have attributes,
//   preventing us from putting attributes twice before an object.
// * Do(TFuncAttributes func) -> TAttributes, same as Do() for TAny;
// * DoIf(bool condition, TFuncAttributes func) -> TAttributes, same as DoIf()
//   for TAny;
// * DoFor(TCollection collection, TListAttributes func) -> TAttributes, same as DoFor()
//   for TAny.
//
 */

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
    template <class TParent> class TListType;
    template <class TParent> class TMapType;

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

        template <class TFuncAny>
        TUnwrappedParent Do(TFuncAny func)
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

        TListType<TParent> BeginList()
        {
            this->Consumer->OnBeginList();
            return TListType<TParent>(this->Consumer, this->Parent);
        }

        template <class TFuncList>
        TUnwrappedParent DoList(TFuncList func)
        {
            this->Consumer->OnBeginList();
            InvokeFluentFunc<TListType<TFluentYsonVoid>>(func, this->Consumer);
            this->Consumer->OnEndList();
            return this->GetUnwrappedParent();
        }

        template <class TFuncList, class TIterator>
        TUnwrappedParent DoListFor(TIterator begin, TIterator end, TFuncList func)
        {
            this->Consumer->OnBeginList();
            for (auto current = begin; current != end; ++current) {
                InvokeFluentFunc<TListType<TFluentYsonVoid>>(func, this->Consumer, current);
            }
            this->Consumer->OnEndList();
            return this->GetUnwrappedParent();
        }

        template <class TFuncList, class TCollection>
        TUnwrappedParent DoListFor(const TCollection& collection, TFuncList func)
        {
            this->Consumer->OnBeginList();
            for (const auto& item : collection) {
                InvokeFluentFunc<TListType<TFluentYsonVoid>>(func, this->Consumer, item);
            }
            this->Consumer->OnEndList();
            return this->GetUnwrappedParent();
        }

        TMapType<TParent> BeginMap()
        {
            this->Consumer->OnBeginMap();
            return TMapType<TParent>(this->Consumer, this->Parent);
        }

        template <class TFuncMap>
        TUnwrappedParent DoMap(TFuncMap func)
        {
            this->Consumer->OnBeginMap();
            InvokeFluentFunc<TMapType<TFluentYsonVoid>>(func, this->Consumer);
            this->Consumer->OnEndMap();
            return this->GetUnwrappedParent();
        }

        template <class TFuncMap, class TIterator>
        TUnwrappedParent DoMapFor(TIterator begin, TIterator end, TFuncMap func)
        {
            this->Consumer->OnBeginMap();
            for (auto current = begin; current != end; ++current) {
                InvokeFluentFunc<TMapType<TFluentYsonVoid>>(func, this->Consumer, current);
            }
            this->Consumer->OnEndMap();
            return this->GetUnwrappedParent();
        }

        template <class TFuncMap, class TCollection>
        TUnwrappedParent DoMapFor(const TCollection& collection, TFuncMap func)
        {
            this->Consumer->OnBeginMap();
            for (const auto& item : collection) {
                InvokeFluentFunc<TMapType<TFluentYsonVoid>>(func, this->Consumer, item);
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
    class TListType
        : public TFluentFragmentBase<TListType, TParent>
    {
    public:
        typedef TListType<TParent> TThis;
        typedef typename TFluentYsonUnwrapper<TParent>::TUnwrapped TUnwrappedParent;

        explicit TListType(NYson::IYsonConsumer* consumer, TParent parent = TParent())
            : TFluentFragmentBase<TFluentYsonBuilder::TListType, TParent>(consumer, std::move(parent))
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
    class TMapType
        : public TFluentFragmentBase<TMapType, TParent>
    {
    public:
        typedef TMapType<TParent> TThis;
        typedef typename TFluentYsonUnwrapper<TParent>::TUnwrapped TUnwrappedParent;

        explicit TMapType(NYson::IYsonConsumer* consumer, TParent parent = TParent())
            : TFluentFragmentBase<TFluentYsonBuilder::TMapType, TParent>(consumer, std::move(parent))
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
    using TValue = TFluentYsonBuilder::TMapType<T>;
};

template <>
struct TFluentType<NYson::EYsonType::ListFragment>
{
    template <class T>
    using TValue = TFluentYsonBuilder::TListType<T>;
};

using TFluentList = TFluentYsonBuilder::TListType<TFluentYsonVoid>;
using TFluentMap = TFluentYsonBuilder::TMapType<TFluentYsonVoid>;
using TFluentAttributes = TFluentYsonBuilder::TAttributes<TFluentYsonVoid>;
using TFluentAny = TFluentYsonBuilder::TAny<TFluentYsonVoid>;

////////////////////////////////////////////////////////////////////////////////

static inline TFluentAny BuildYsonFluently(NYson::IYsonConsumer* consumer)
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

