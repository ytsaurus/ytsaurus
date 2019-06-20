#pragma once

#include "common.h"
#include "serialize.h"

#include <mapreduce/yt/node/serialize.h>
#include <mapreduce/yt/node/node_builder.h>

#include <library/yson/consumer.h>
#include <library/yson/writer.h>

#include <util/generic/noncopyable.h>
#include <util/generic/ptr.h>
#include <util/stream/str.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T>
struct TFluentYsonUnwrapper
{
    using TUnwrapped = T;

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
    using TUnwrapped = void;

    static TUnwrapped Unwrap(TFluentYsonVoid)
    { }
};

////////////////////////////////////////////////////////////////////////////////

class TFluentYsonBuilder
    : private TNonCopyable
{
private:
    template <class T>
    static void WriteValue(IYsonConsumer* consumer, const T& value)
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
        operator IYsonConsumer* () const
        {
            return Consumer;
        }

    protected:
        IYsonConsumer* Consumer;
        TParent Parent;

        TFluentBase(IYsonConsumer* consumer, TParent parent)
            : Consumer(consumer)
            , Parent(std::move(parent))
        { }

        using TUnwrappedParent = typename TFluentYsonUnwrapper<TParent>::TUnwrapped;

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
        using TDeepThis = TThis<TParent>;
        using TShallowThis = TThis<TFluentYsonVoid>;
        using TUnwrappedParent = typename TFluentYsonUnwrapper<TParent>::TUnwrapped;

        explicit TFluentFragmentBase(IYsonConsumer* consumer, TParent parent = TParent())
            : TFluentBase<TParent>(consumer, std::move(parent))
        { }

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
        using TUnwrappedParent = typename TFluentYsonUnwrapper<TParent>::TUnwrapped;

        TAnyWithoutAttributes(IYsonConsumer* consumer, TParent parent)
            : TFluentBase<TParent>(consumer, std::move(parent))
        { }

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

        template <class TFunc>
        TUnwrappedParent DoList(const TFunc& func)
        {
            this->Consumer->OnBeginList();
            func(TListType<TFluentYsonVoid>(this->Consumer));
            this->Consumer->OnEndList();
            return this->GetUnwrappedParent();
        }

        template <class TFunc, class TIterator>
        TUnwrappedParent DoListFor(const TIterator& begin, const TIterator& end, const TFunc& func)
        {
            this->Consumer->OnBeginList();
            for (auto current = begin; current != end; ++current) {
                func(TListType<TFluentYsonVoid>(this->Consumer), current);
            }
            this->Consumer->OnEndList();
            return this->GetUnwrappedParent();
        }

        template <class TFunc, class TCollection>
        TUnwrappedParent DoListFor(const TCollection& collection, const TFunc& func)
        {
            this->Consumer->OnBeginList();
            for (const auto& item : collection) {
                func(TListType<TFluentYsonVoid>(this->Consumer), item);
            }
            this->Consumer->OnEndList();
            return this->GetUnwrappedParent();
        }

        TMapType<TParent> BeginMap()
        {
            this->Consumer->OnBeginMap();
            return TMapType<TParent>(this->Consumer, this->Parent);
        }

        template <class TFunc>
        TUnwrappedParent DoMap(const TFunc& func)
        {
            this->Consumer->OnBeginMap();
            func(TMapType<TFluentYsonVoid>(this->Consumer));
            this->Consumer->OnEndMap();
            return this->GetUnwrappedParent();
        }

        template <class TFunc, class TIterator>
        TUnwrappedParent DoMapFor(const TIterator& begin, const TIterator& end, const TFunc& func)
        {
            this->Consumer->OnBeginMap();
            for (auto current = begin; current != end; ++current) {
                func(TMapType<TFluentYsonVoid>(this->Consumer), current);
            }
            this->Consumer->OnEndMap();
            return this->GetUnwrappedParent();
        }

        template <class TFunc, class TCollection>
        TUnwrappedParent DoMapFor(const TCollection& collection, const TFunc& func)
        {
            this->Consumer->OnBeginMap();
            for (const auto& item : collection) {
                func(TMapType<TFluentYsonVoid>(this->Consumer), item);
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
        using TBase = TAnyWithoutAttributes<TParent>;

        explicit TAny(IYsonConsumer* consumer, TParent parent)
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
        using TThis = TAttributes<TParent>;
        using TUnwrappedParent = typename TFluentYsonUnwrapper<TParent>::TUnwrapped;

        explicit TAttributes(IYsonConsumer* consumer, TParent parent = TParent())
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

        //TODO: from TNode

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
        using TThis = TListType<TParent>;
        using TUnwrappedParent = typename TFluentYsonUnwrapper<TParent>::TUnwrapped;

        explicit TListType(IYsonConsumer* consumer, TParent parent = TParent())
            : TFluentFragmentBase<TFluentYsonBuilder::TListType, TParent>(consumer, std::move(parent))
        { }

        TAny<TThis> Item()
        {
            this->Consumer->OnListItem();
            return TAny<TThis>(this->Consumer, *this);
        }

        // TODO: from TNode

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
        using TThis = TMapType<TParent>;
        using TUnwrappedParent = typename TFluentYsonUnwrapper<TParent>::TUnwrapped;

        explicit TMapType(IYsonConsumer* consumer, TParent parent = TParent())
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

        // TODO: from TNode

        TUnwrappedParent EndMap()
        {
            this->Consumer->OnEndMap();
            return this->GetUnwrappedParent();
        }
    };

};

////////////////////////////////////////////////////////////////////////////////

using TFluentAny = TFluentYsonBuilder::TAny<TFluentYsonVoid>;
using TFluentList = TFluentYsonBuilder::TListType<TFluentYsonVoid>;
using TFluentMap = TFluentYsonBuilder::TMapType<TFluentYsonVoid>;
using TFluentAttributes = TFluentYsonBuilder::TAttributes<TFluentYsonVoid>;

////////////////////////////////////////////////////////////////////////////////

static inline TFluentAny BuildYsonFluently(IYsonConsumer* consumer)
{
    return TFluentAny(consumer, TFluentYsonVoid());
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
    : public TThrRefBase
{
public:
    using TValue = TString;

    explicit TFluentYsonWriterState(EYsonFormat format)
        : Writer(&Output, format)
    { }

    TString GetValue()
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

////////////////////////////////////////////////////////////////////////////////

class TFluentYsonBuilderState
    : public TThrRefBase
{
public:
    using TValue = TNode;

    explicit TFluentYsonBuilderState()
        : Builder(&Node)
    { }

    TNode GetValue()
    {
        return std::move(Node);
    }

    IYsonConsumer* GetConsumer()
    {
        return &Builder;
    }

private:
    TNode Node;
    TNodeBuilder Builder;
};

////////////////////////////////////////////////////////////////////////////////

template <class TState>
class TFluentYsonHolder
{
public:
    explicit TFluentYsonHolder(::TIntrusivePtr<TState> state)
        : State(state)
    { }

    ::TIntrusivePtr<TState> GetState() const
    {
        return State;
    }

private:
    ::TIntrusivePtr<TState> State;
};

////////////////////////////////////////////////////////////////////////////////

template <class TState>
struct TFluentYsonUnwrapper< TFluentYsonHolder<TState> >
{
    using TUnwrapped = typename TState::TValue;

    static TUnwrapped Unwrap(const TFluentYsonHolder<TState>& holder)
    {
        return std::move(holder.GetState()->GetValue());
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class TState>
TFluentYsonBuilder::TAny<TFluentYsonHolder<TState>>
BuildYsonFluentlyWithState(::TIntrusivePtr<TState> state)
{
    return TFluentYsonBuilder::TAny<TFluentYsonHolder<TState>>(
        state->GetConsumer(),
        TFluentYsonHolder<TState>(state));
}

inline TFluentYsonBuilder::TAny<TFluentYsonHolder<TFluentYsonWriterState>>
BuildYsonStringFluently(EYsonFormat format = YF_TEXT)
{
    ::TIntrusivePtr<TFluentYsonWriterState> state(new TFluentYsonWriterState(format));
    return BuildYsonFluentlyWithState(state);
}

inline TFluentYsonBuilder::TAny<TFluentYsonHolder<TFluentYsonBuilderState>>
BuildYsonNodeFluently()
{
    ::TIntrusivePtr<TFluentYsonBuilderState> state(new TFluentYsonBuilderState);
    return BuildYsonFluentlyWithState(state);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
