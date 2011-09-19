#pragma once

#include "common.h"
#include "yson_events.h"

// For TVoid.
#include "../actions/action.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

class TFluentYsonParser
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
        TFluentBase(IYsonEvents* events, const TParent& parent)
            : Events(events)
            , Parent(parent)
        { }

        IYsonEvents* Events;
        TParent Parent;

    };

    class TFluentTree
        : public TFluentBase<TVoid>
    {
    public:
        typedef TFluentTree TThis;

        TFluentTree(IYsonEvents* events)
            : TFluentBase<TVoid>(events, TVoid())
        { }

        TAny<TFluentTree> BeginTree()
        {
            Events->BeginTree();
            return TAny<TFluentTree>(Events, *this);
        }

        void EndTree()
        {
            Events->EndTree();
        }
    };

    template<class TParent>
    class TAny
        : public TFluentBase<TParent>
    {
    public:
        typedef TAny<TParent> TThis;

        TAny(IYsonEvents* events, const TParent& parent)
            : TFluentBase<TParent>(events, parent)
        { }

        TParent Value(const Stroka& value)
        {
            this->Events->StringValue(value);
            return this->Parent;
        }

        TParent Value(i32 value)
        {
            return Value(static_cast<i64>(value));
        }

        TParent Value(i64 value)
        {
            this->Events->Int64Value(value);
            return this->Parent;
        }

        TParent Value(float value)
        {
            return Value(static_cast<double>(value));
        }

        TParent Value(double value)
        {
            this->Events->DoubleValue(value);
            return this->Parent;
        }

        TParent Value(bool value)
        {
            return Value(value ? Stroka("true") : Stroka("false"));
        }

        TParent EntityValue()
        {
            this->Events->EntityValue();
            return this->Parent;
        }

        TList<TParent> BeginList()
        {
            this->Events->BeginList();
            return TList<TParent>(this->Events, this->Parent);
        }

        TMap<TParent> BeginMap()
        {
            this->Events->BeginMap();
            return TMap<TParent>(this->Events, this->Parent);
        }

        TAny< TToAttributes<TParent> > WithAttributes()
        {
            return TAny< TToAttributes<TParent> >(this->Events, TToAttributes<TParent>(this->Events, this->Parent));
        }
    };

    template<class TParent>
    class TToAttributes
        : public TFluentBase<TParent>
    {
    public:
        TToAttributes(IYsonEvents* events, const TParent& parent)
            : TFluentBase<TParent>(events, parent)
        { }

        TAttributes<TParent> BeginAttributes()
        {
            this->Events->BeginAttributes();
            return TAttributes<TParent>(this->Events, this->Parent);
        }
    };

    template<class TParent>
    class TAttributes
        : public TFluentBase<TParent>
    {
    public:
        typedef TAttributes<TParent> TThis;

        TAttributes(IYsonEvents* events, const TParent& parent)
            : TFluentBase<TParent>(events, parent)
        { }

        TAny<TThis> Item(const Stroka& name)
        {
            this->Events->AttributesItem(name);
            return TAny<TThis>(this->Events, *this);
        }

        TParent EndAttributes()
        {
            this->Events->EndAttributes();
            return this->Parent;
        }
    };

    template<class TParent>
    class TList
        : public TFluentBase<TParent>
    {
    public:
        typedef TList<TParent> TThis;

        TList(IYsonEvents* events, const TParent& parent)
            : TFluentBase<TParent>(events, parent)
            , Index(0)
        { }

        TAny<TThis> Item()
        {
            this->Events->ListItem(Index++);
            return TAny<TThis>(this->Events, *this);
        }

        TParent EndList()
        {
            this->Events->EndList();
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

        TMap(IYsonEvents* events, const TParent& parent)
            : TFluentBase<TParent>(events, parent)
        { }

        TAny<TThis> Item(const Stroka& name)
        {
            this->Events->MapItem(name);
            return TAny<TThis>(this->Events, *this);
        }

        TParent EndMap()
        {
            this->Events->EndMap();
            return this->Parent;
        }
    };

    static TFluentTree Create(IYsonEvents* events)
    {
        return TFluentTree(events);
    }

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

