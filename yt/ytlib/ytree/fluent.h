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
            : TFluentBase(events, TVoid())
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
            : TFluentBase(events, parent)
        { }

        TParent Value(const Stroka& value)
        {
            Events->StringValue(value);
            return Parent;
        }

        TParent Value(i32 value)
        {
            return Value(static_cast<i64>(value));
        }

        TParent Value(i64 value)
        {
            Events->Int64Value(value);
            return Parent;
        }

        TParent Value(float value)
        {
            return Value(static_cast<double>(value));
        }

        TParent Value(double value)
        {
            Events->DoubleValue(value);
            return Parent;
        }

        TParent Value(bool value)
        {
            return Value(value ? Stroka("true") : Stroka("false"));
        }

        TParent EntityValue()
        {
            Events->EntityValue();
            return Parent;
        }

        TList<TParent> BeginList()
        {
            Events->BeginList();
            return TList<TParent>(Events, Parent);
        }

        TMap<TParent> BeginMap()
        {
            Events->BeginMap();
            return TMap<TParent>(Events, Parent);
        }

        TAny< TToAttributes<TParent> > WithAttributes()
        {
            return TAny< TToAttributes<TParent> >(Events, TToAttributes<TParent>(Events, Parent));
        }
    };

    template<class TParent>
    class TToAttributes
        : public TFluentBase<TParent>
    {
    public:
        TToAttributes(IYsonEvents* events, const TParent& parent)
            : TFluentBase(events, parent)
        { }

        TAttributes<TParent> BeginAttributes()
        {
            Events->BeginAttributes();
            return TAttributes<TParent>(Events, Parent);
        }
    };

    template<class TParent>
    class TAttributes
        : public TFluentBase<TParent>
    {
    public:
        typedef TAttributes<TParent> TThis;

        TAttributes(IYsonEvents* events, const TParent& parent)
            : TFluentBase(events, parent)
        { }

        TAny<TThis> Item(const Stroka& name)
        {
            Events->AttributesItem(name);
            return TAny<TThis>(Events, *this);
        }

        TParent EndAttributes()
        {
            Events->EndAttributes();
            return Parent;
        }
    };

    template<class TParent>
    class TList
        : public TFluentBase<TParent>
    {
    public:
        typedef TList<TParent> TThis;

        TList(IYsonEvents* events, const TParent& parent)
            : TFluentBase(events, parent)
            , Index(0)
        { }

        TAny<TThis> Item()
        {
            Events->ListItem(Index++);
            return TAny<TThis>(Events, *this);
        }

        TParent EndList()
        {
            Events->EndList();
            return Parent;
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
            : TFluentBase(events, parent)
        { }

        TAny<TThis> Item(const Stroka& name)
        {
            Events->MapItem(name);
            return TAny<TThis>(Events, *this);
        }

        TParent EndMap()
        {
            Events->EndMap();
            return Parent;
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

