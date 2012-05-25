#pragma once

#include "public.h"
#include "yson_consumer.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

struct IAttributeProvider
{
    ~IAttributeProvider()
    { }

    virtual IAttributeDictionary& Attributes() = 0;
    virtual const IAttributeDictionary& Attributes() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct ISystemAttributeProvider
{
    ~ISystemAttributeProvider()
    { }

    //! Describes a system attribute.
    struct TAttributeInfo
    {
        Stroka Key;
        bool IsPresent;
        bool IsOpaque;

        TAttributeInfo(const char* key, bool isPresent = true, bool isOpaque = false)
            : Key(key)
            , IsPresent(isPresent)
            , IsOpaque(isOpaque)
        { }
    };

    //! Populates the list of all system attributes supported by this object.
    /*!
     *  \note
     *  Must not clear #attributes since additional items may be added in inheritors.
     */
    virtual void GetSystemAttributes(std::vector<TAttributeInfo>* attributes) = 0;

    //! Gets the value of a system attribute.
    /*!
     *  \returns False if there is no system attribute with the given key.
     */
    virtual bool GetSystemAttribute(const Stroka& key, IYsonConsumer* consumer) = 0;

    //! Sets the value of a system attribute.
    /*! 
     *  \returns False if the attribute cannot be set or
     *  there is no system attribute with the given key.
     */
    virtual bool SetSystemAttribute(const Stroka& key, const TYson& value) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
