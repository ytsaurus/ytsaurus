#pragma once

#include "common.h"
#include "yson_events.h"

// For TVoid.
#include "../actions/action.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

class TFluentYsonBuilder
    : private TNonCopyable
{
public:
    class TFluentTree;
    template<class TParent> class TAny;
    template<class TParent> class TToAttributes;
    template<class TParent> class TAttributes;
    template<class TParent> class TList;
    template<class TParent> class TMap;

    template<class TParent>
    class TFluentBase
    {
    protected:
        TFluentBase(IYsonConsumer* events, const TParent& parent)
            : Events(events)
            , Parent(parent)
        { }

        IYsonConsumer* Events;
        TParent Parent;

    };

    template<class TParent>
    class TAny
        : public TFluentBase<TParent>
    {
    public:
        typedef TAny<TParent> TThis;

        TAny(IYsonConsumer* events, const TParent& parent)
            : TFluentBase<TParent>(events, parent)
        { }

        TParent Scalar(const Stroka& value)
        {
            this->Events->OnStringScalar(value);
            return this->Parent;
        }

        // A stupid language with a stupid type system.
        TParent Scalar(const char* value)
        {
            this->Events->OnStringScalar(Stroka(value));
            return this->Parent;
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
            this->Events->OnInt64Scalar(value);
            return this->Parent;
        }

        TParent Scalar(float value)
        {
            return Scalar(static_cast<double>(value));
        }

        TParent Scalar(double value)
        {
            this->Events->OnDoubleScalar(value);
            return this->Parent;
        }

        TParent Scalar(bool value)
        {
            return Scalar(value ? Stroka("true") : Stroka("false"));
        }

        TParent EntityScalar()
        {
            this->Events->OnEntityScalar();
            return this->Parent;
        }

        TList<TParent> BeginList()
        {
            this->Events->OnBeginList();
            return TList<TParent>(this->Events, this->Parent);
        }

        TMap<TParent> BeginMap()
        {
            this->Events->OnBeginMap();
            return TMap<TParent>(this->Events, this->Parent);
        }

        TAny< TToAttributes<TParent> > WithAttributes()
        {
            return TAny< TToAttributes<TParent> >(this->Events, TToAttributes<TParent>(this->Events, this->Parent));
        }

        TParent Do(TYsonProducer::TPtr producer)
        {
            producer->Do(this->Events);
            return this->Parent;
        }

        template<class T>
        TParent Do(const T& func)
        {
            func(this->Events);
            return this->Parent;
        }
    };

    template<class TParent>
    class TToAttributes
        : public TFluentBase<TParent>
    {
    public:
        TToAttributes(IYsonConsumer* events, const TParent& parent)
            : TFluentBase<TParent>(events, parent)
        { }

        TAttributes<TParent> BeginAttributes()
        {
            this->Events->OnBeginAttributes();
            return TAttributes<TParent>(this->Events, this->Parent);
        }
    };

    template<class TParent>
    class TAttributes
        : public TFluentBase<TParent>
    {
    public:
        typedef TAttributes<TParent> TThis;

        TAttributes(IYsonConsumer* events, const TParent& parent)
            : TFluentBase<TParent>(events, parent)
        { }

        TAny<TThis> Item(const Stroka& name)
        {
            this->Events->OnAttributesItem(name);
            return TAny<TThis>(this->Events, *this);
        }

        TParent EndAttributes()
        {
            this->Events->OnEndAttributes();
            return this->Parent;
        }
    };

    template<class TParent>
    class TList
        : public TFluentBase<TParent>
    {
    public:
        typedef TList<TParent> TThis;

        TList(IYsonConsumer* events, const TParent& parent)
            : TFluentBase<TParent>(events, parent)
            , Index(0)
        { }

        TAny<TThis> Item()
        {
            this->Events->OnListItem(Index++);
            return TAny<TThis>(this->Events, *this);
        }

        TParent EndList()
        {
            this->Events->OnEndList();
            return this->Parent;
        }

    private:
        int Index;

    };

    template<class TParent>
    class TMap
        : public TFluentBase<TParent>
    {
    public:
        typedef TMap<TParent> TThis;

        TMap(IYsonConsumer* events, const TParent& parent)
            : TFluentBase<TParent>(events, parent)
        { }

        TAny<TThis> Item(const Stroka& name)
        {
            this->Events->OnMapItem(name);
            return TAny<TThis>(this->Events, *this);
        }

        TParent EndMap()
        {
            this->Events->OnEndMap();
            return this->Parent;
        }
    };

    static TAny<TVoid> Create(IYsonConsumer* events)
    {
        return TAny<TVoid>(events, TVoid());
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

