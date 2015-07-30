#pragma once

#include <mapreduce/yt/yson/consumer.h>
#include <mapreduce/yt/yson/writer.h>
#include <mapreduce/yt/yson/json_writer.h>
#include <mapreduce/yt/interface/common.h>
#include <mapreduce/yt/common/node_builder.h>
#include <mapreduce/yt/common/serialize.h>
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
        return MoveArg(t);
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
    template <class TParent> class TList;
    template <class TParent> class TMap;

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
            , Parent(MoveArg(parent))
        { }

        using TUnwrappedParent = typename TFluentYsonUnwrapper<TParent>::TUnwrapped;

        TUnwrappedParent GetUnwrappedParent()
        {
            return TFluentYsonUnwrapper<TParent>::Unwrap(MoveArg(Parent));
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
            : TFluentBase<TParent>(consumer, MoveArg(parent))
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
            : TFluentBase<TParent>(consumer, MoveArg(parent))
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
        using TBase = TAnyWithoutAttributes<TParent>;

        explicit TAny(IYsonConsumer* consumer, TParent parent)
            : TBase(consumer, MoveArg(parent))
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
            : TFluentFragmentBase<TFluentYsonBuilder::TAttributes, TParent>(consumer, MoveArg(parent))
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
    class TList
        : public TFluentFragmentBase<TList, TParent>
    {
    public:
        using TThis = TList<TParent>;
        using TUnwrappedParent = typename TFluentYsonUnwrapper<TParent>::TUnwrapped;

        explicit TList(IYsonConsumer* consumer, TParent parent = TParent())
            : TFluentFragmentBase<TFluentYsonBuilder::TList, TParent>(consumer, MoveArg(parent))
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
    class TMap
        : public TFluentFragmentBase<TMap, TParent>
    {
    public:
        using TThis = TMap<TParent>;
        using TUnwrappedParent = typename TFluentYsonUnwrapper<TParent>::TUnwrapped;

        explicit TMap(IYsonConsumer* consumer, TParent parent = TParent())
            : TFluentFragmentBase<TFluentYsonBuilder::TMap, TParent>(consumer, MoveArg(parent))
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
using TFluentList = TFluentYsonBuilder::TList<TFluentYsonVoid>;
using TFluentMap = TFluentYsonBuilder::TMap<TFluentYsonVoid>;
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
    using TValue = Stroka;

    explicit TFluentYsonWriterState(EYsonFormat format)
        : Writer(&Output, format)
    { }

    Stroka GetValue()
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
        return MoveArg(Node);
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

class TFluentJsonWriterState
    : public TThrRefBase
{
public:
    using TValue = Stroka;

    explicit TFluentJsonWriterState(EJsonFormat format)
        : Writer(&Output, YT_NODE, format)
    { }

    Stroka GetValue()
    {
        Writer.Flush();
        return Output.Str();
    }

    IYsonConsumer* GetConsumer()
    {
        return &Writer;
    }

private:
    TStringStream Output;
    TJsonWriter Writer;
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
    using TUnwrapped = typename TState::TValue;

    static TUnwrapped Unwrap(const TFluentYsonHolder<TState>& holder)
    {
        return MoveArg(holder.GetState()->GetValue());
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class TState>
TFluentYsonBuilder::TAny<TFluentYsonHolder<TState>>
BuildYsonFluentlyWithState(TIntrusivePtr<TState> state)
{
    return TFluentYsonBuilder::TAny<TFluentYsonHolder<TState>>(
        state->GetConsumer(),
        TFluentYsonHolder<TState>(state));
}

inline TFluentYsonBuilder::TAny<TFluentYsonHolder<TFluentYsonWriterState>>
BuildYsonStringFluently(EYsonFormat format = YF_TEXT)
{
    TIntrusivePtr<TFluentYsonWriterState> state(new TFluentYsonWriterState(format));
    return BuildYsonFluentlyWithState(state);
}

inline TFluentYsonBuilder::TAny<TFluentYsonHolder<TFluentJsonWriterState>>
BuildJsonStringFluently(EJsonFormat format = JF_TEXT)
{
    TIntrusivePtr<TFluentJsonWriterState> state(new TFluentJsonWriterState(format));
    return BuildYsonFluentlyWithState(state);
}

inline TFluentYsonBuilder::TAny<TFluentYsonHolder<TFluentYsonBuilderState>>
BuildYsonNodeFluently()
{
    TIntrusivePtr<TFluentYsonBuilderState> state(new TFluentYsonBuilderState);
    return BuildYsonFluentlyWithState(state);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
