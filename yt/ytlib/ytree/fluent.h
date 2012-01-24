#pragma once

#include "common.h"
#include "yson_consumer.h"
#include "yson_writer.h"

// For TVoid.
#include <ytlib/actions/action.h>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

class TFluentYsonBuilder
    : private TNonCopyable
{
public:
    class TFluentAny;
    template <class TParent> class TAny;
    template <class TParent> class TToAttributes;
    template <class TParent> class TAttributes;
    class TListCore;
    template <class TParent> class TList;
    class TMapCore;
    template <class TParent> class TMap;

    template <class TParent>
    class TFluentBase
    {
    public:
        TParent Do(TYsonProducer* producer)
        {
            producer->Do(this->Consumer);
            return this->Parent;
        }

    protected:
        TFluentBase(IYsonConsumer* consumer, const TParent& parent)
            : Consumer(consumer)
            , Parent(parent)
        { }

        IYsonConsumer* Consumer;
        TParent Parent;

    };

    template <class TParent>
    class TAny
        : public TFluentBase<TParent>
    {
    public:
        typedef TAny<TParent> TThis;

        TAny(IYsonConsumer* consumer, const TParent& parent, bool hasAttributes)
            : TFluentBase<TParent>(consumer, parent)
            , HasAttributes(hasAttributes)
        { }

        TParent Scalar(const Stroka& value)
        {
            this->Consumer->OnStringScalar(value, HasAttributes);
            return this->Parent;
        }

        // A stupid language with a stupid type system.
        TParent Scalar(const char* value)
        {
            return Scalar(Stroka(value));
        }

        TParent Scalar(i32 value)
        {
            return Scalar(static_cast<i64>(value));
        }

        TParent Scalar(ui32 value)
        {
            return Scalar(static_cast<i64>(value));
        }

        TParent Scalar(i64 value)
        {
            this->Consumer->OnInt64Scalar(value, HasAttributes);
            return this->Parent;
        }

        TParent Scalar(float value)
        {
            return Scalar(static_cast<double>(value));
        }

        TParent Scalar(double value)
        {
            this->Consumer->OnDoubleScalar(value, HasAttributes);
            return this->Parent;
        }

        TParent Scalar(bool value)
        {
            return Scalar(value ? Stroka("true") : Stroka("false"));
        }

        TParent Entity()
        {
            this->Consumer->OnEntity(HasAttributes);
            return this->Parent;
        }

        TList<TParent> BeginList()
        {
            this->Consumer->OnBeginList();
            return TList<TParent>(this->Consumer, this->Parent, HasAttributes);
        }

        template <class TFunc>
        TParent DoList(const TFunc& func)
        {
            this->Consumer->OnBeginList();
            func(TListCore(this->Consumer));
            this->Consumer->OnEndList();
            return this->Parent;
        }

        template <class TFunc, class TIterator>
        TParent DoListFor(const TIterator& begin, const TIterator& end, const TFunc& func)
        {
            this->Consumer->OnBeginList();
            for (auto current = begin; current != end; ++current) {
                func(TListCore(this->Consumer), current);
            }
            this->Consumer->OnEndList();
            return this->Parent;
        }

        template <class TFunc, class TCollection>
        TParent DoListFor(const TCollection& collection, const TFunc& func)
        {
            this->Consumer->OnBeginList();
            FOREACH (const auto& item, collection) {
                func(TListCore(this->Consumer), item);
            }
            this->Consumer->OnEndList();
            return this->Parent;
        }

        TMap<TParent> BeginMap()
        {
            this->Consumer->OnBeginMap();
            return TMap<TParent>(this->Consumer, this->Parent, HasAttributes);
        }

        template <class TFunc>
        TParent DoMap(const TFunc& func)
        {
            this->Consumer->OnBeginMap();
            func(TListCore(this->Consumer));
            this->Consumer->OnEndMap();
            return this->Parent;
        }

        template <class TFunc, class TIterator>
        TParent DoMapFor(const TIterator& begin, const TIterator& end, const TFunc& func)
        {
            this->Consumer->OnBeginMap();
            for (auto current = begin; current != end; ++current) {
                func(TMapCore(this->Consumer), current);
            }
            this->Consumer->OnEndMap();
            return this->Parent;
        }

        template <class TFunc, class TCollection>
        TParent DoMapFor(const TCollection& collection, const TFunc& func)
        {
            this->Consumer->OnBeginMap();
            FOREACH (const auto& item, collection) {
                func(TMapCore(this->Consumer), item);
            }
            this->Consumer->OnEndMap();
            return this->Parent;
        }

        TAny< TToAttributes<TParent> > WithAttributes()
        {
            return TAny< TToAttributes<TParent> >(
                this->Consumer,
                TToAttributes<TParent>(this->Consumer, this->Parent),
                true);
        }

        bool HasAttributes;

    };

    template <class TParent>
    class TToAttributes
        : public TFluentBase<TParent>
    {
    public:
        TToAttributes(IYsonConsumer* consumer, const TParent& parent)
            : TFluentBase<TParent>(consumer, parent)
        { }

        TAttributes<TParent> BeginAttributes()
        {
            this->Consumer->OnBeginAttributes();
            return TAttributes<TParent>(this->Consumer, this->Parent);
        }
    };

    template <class TParent>
    class TAttributes
        : public TFluentBase<TParent>
    {
    public:
        typedef TAttributes<TParent> TThis;

        TAttributes(IYsonConsumer* consumer, const TParent& parent)
            : TFluentBase<TParent>(consumer, parent)
        { }

        TAny<TThis> Item(const Stroka& name)
        {
            this->Consumer->OnAttributesItem(name);
            return TAny<TThis>(this->Consumer, *this, false);
        }

        TParent EndAttributes()
        {
            this->Consumer->OnEndAttributes();
            return this->Parent;
        }
    };

    class TListCore
        : public TFluentBase<TVoid>
    {
    public:
        TListCore(IYsonConsumer* consumer)
            : TFluentBase<TVoid>(consumer, TVoid())
        { }

        TAny<TListCore> Item()
        {
            this->Consumer->OnListItem();
            return TAny<TListCore>(this->Consumer, *this, false);
        }
    };

    template <class TParent>
    class TList
        : public TFluentBase<TParent>
    {
    public:
        typedef TList<TParent> TThis;

        TList(IYsonConsumer* consumer, const TParent& parent, bool hasAttributes)
            : TFluentBase<TParent>(consumer, parent)
            , HasAttributes(hasAttributes)
        { }

        TAny<TThis> Item()
        {
            this->Consumer->OnListItem();
            return TAny<TThis>(this->Consumer, *this, false);
        }

        template <class TFunc>
        TThis& Do(const TFunc& func)
        {
            func(TListCore(this->Consumer));
            return *this;
        }

        template <class TFunc>
        TThis& DoIf(bool condition, const TFunc& func)
        {
            if (condition) {
                func(TListCore(this->Consumer));
            }
            return *this;
        }

        template <class TFunc, class TIterator>
        TThis& DoFor(const TIterator& begin, const TIterator& end, const TFunc& func)
        {
            for (auto current = begin; current != end; ++current) {
                func(TListCore(this->Consumer), current);
            }
            return *this;
        }

        template <class TFunc, class TCollection>
        TThis& DoFor(const TCollection& collection, const TFunc& func)
        {
            FOREACH (const auto& item, collection) {
                func(TListCore(this->Consumer), item);
            }
            return *this;
        }

        TParent EndList()
        {
            this->Consumer->OnEndList(HasAttributes);
            return this->Parent;
        }

    private:
        bool HasAttributes;

    };

    class TMapCore
        : public TFluentBase<TVoid>
    {
    public:
        TMapCore(IYsonConsumer* consumer)
            : TFluentBase<TVoid>(consumer, TVoid())
        { }

        TAny<TMapCore> Item(const Stroka& name)
        {
            this->Consumer->OnMapItem(name);
            return TAny<TMapCore>(this->Consumer, *this, false);
        }
    };

    template <class TParent>
    class TMap
        : public TFluentBase<TParent>
    {
    public:
        typedef TMap<TParent> TThis;

        TMap(IYsonConsumer* consumer, const TParent& parent, bool hasAttributes)
            : TFluentBase<TParent>(consumer, parent)
            , HasAttributes(hasAttributes)
        { }

        TAny<TThis> Item(const Stroka& name)
        {
            this->Consumer->OnMapItem(name);
            return TAny<TThis>(this->Consumer, *this, false);
        }

        template <class TFunc>
        TThis& Do(const TFunc& func)
        {
            func(TMapCore(this->Consumer));
            return *this;
        }

        template <class TFunc>
        TThis& DoIf(bool condition, const TFunc& func)
        {
            if (condition) {
                func(TMapCore(this->Consumer));
            }
            return *this;
        }

        template <class TFunc, class TIterator>
        TThis& DoFor(const TIterator& begin, const TIterator& end, const TFunc& func)
        {
            for (auto current = begin; current != end; ++current) {
                func(TMapCore(this->Consumer), current);
            }
            return *this;
        }

        template <class TFunc, class TCollection>
        TThis& DoFor(const TCollection& collection, const TFunc& func)
        {
            FOREACH (const auto& item, collection) {
                func(TMapCore(this->Consumer), item);
            }
            return *this;
        }

        TParent EndMap()
        {
            this->Consumer->OnEndMap(HasAttributes);
            return this->Parent;
        }
    
    private:
        bool HasAttributes;

    };

};

typedef TFluentYsonBuilder::TListCore   TFluentList;
typedef TFluentYsonBuilder::TMapCore    TFluentMap;

////////////////////////////////////////////////////////////////////////////////

inline TFluentYsonBuilder::TAny<TVoid> BuildYsonFluently(IYsonConsumer* consumer)
{
    return TFluentYsonBuilder::TAny<TVoid>(consumer, TVoid(), false);
}

inline TFluentList BuildYsonListFluently(IYsonConsumer* consumer)
{
    return TFluentList(consumer);
}

inline TFluentMap BuildYsonMapFluently(IYsonConsumer* consumer)
{
    return TFluentMap(consumer);
}

////////////////////////////////////////////////////////////////////////////////

class TFluentYsonConsumer
    : public TRefCounted
{
public:
    typedef TIntrusivePtr<TFluentYsonConsumer> TPtr;

    TFluentYsonConsumer(EFormat format)
        : Writer(&Output, format)
    { }

    TYson GetYson() const
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

    operator TYson() const
    {
        return Consumer->GetYson();
    }

private:
    TFluentYsonConsumer::TPtr Consumer;
};

inline TFluentYsonBuilder::TAny<TFluentYsonHolder> BuildYsonFluently(
    EFormat format = EFormat::Binary)
{
    auto consumer = New<TFluentYsonConsumer>(format);
    TFluentYsonHolder holder(consumer);
    return TFluentYsonBuilder::TAny<TFluentYsonHolder>(
        consumer->GetConsumer(),
        holder,
        false);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

