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
        TFluentBase(IYsonConsumer* consumer, const TParent& parent)
            : Consumer(consumer)
            , Parent(parent)
        { }

        IYsonConsumer* Consumer;
        TParent Parent;

    };

    template<class TParent>
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

        TParent EntityScalar()
        {
            this->Consumer->OnEntity(HasAttributes);
            return this->Parent;
        }

        TList<TParent> BeginList()
        {
            this->Consumer->OnBeginList();
            return TList<TParent>(this->Consumer, this->Parent, HasAttributes);
        }

        TMap<TParent> BeginMap()
        {
            this->Consumer->OnBeginMap();
            return TMap<TParent>(this->Consumer, this->Parent, HasAttributes);
        }

        TAny< TToAttributes<TParent> > WithAttributes()
        {
            return TAny< TToAttributes<TParent> >(
                this->Consumer,
                TToAttributes<TParent>(this->Consumer, this->Parent),
                true);
        }

        TParent Do(TYsonProducer::TPtr producer)
        {
            producer->Do(this->Consumer);
            return this->Parent;
        }

        template<class T>
        TParent Do(const T& func)
        {
            func(this->Consumer);
            return this->Parent;
        }

    private:
        bool HasAttributes;

    };

    template<class TParent>
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

    template<class TParent>
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

    template<class TParent>
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

        TParent EndList()
        {
            this->Consumer->OnEndList(HasAttributes);
            return this->Parent;
        }

    private:
        bool HasAttributes;

    };

    template<class TParent>
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

        TParent EndMap()
        {
            this->Consumer->OnEndMap(HasAttributes);
            return this->Parent;
        }
    
    private:
        bool HasAttributes;

    };

};

inline TFluentYsonBuilder::TAny<TVoid> BuildYsonFluently(IYsonConsumer* consumer)
{
    return TFluentYsonBuilder::TAny<TVoid>(consumer, TVoid(), false);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

