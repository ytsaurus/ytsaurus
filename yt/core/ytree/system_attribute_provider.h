#pragma once

#include "public.h"

#include <core/yson/consumer.h>

#include <core/misc/error.h>

#include <core/actions/future.h>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

struct ISystemAttributeProvider
{
    virtual ~ISystemAttributeProvider()
    { }

    //! Describes a system attribute.
    struct TAttributeInfo
    {
        const char* Key;
        bool IsPresent;
        bool IsOpaque;
        bool IsCustom;

        TAttributeInfo(
            const char* key,
            bool isPresent = true,
            bool isOpaque = false,
            bool isCustom = false)
            : Key(key)
            , IsPresent(isPresent)
            , IsOpaque(isOpaque)
            , IsCustom(isCustom)
        { }
    };

    //! Populates the list of all system attributes supported by this object.
    /*!
     *  \note
     *  Must not clear #attributes since additional items may be added in inheritors.
     */
    virtual void ListSystemAttributes(std::vector<TAttributeInfo>* attributes) = 0;

    //! Populates the list of all builtin attributes supported by this object.
    void ListBuiltinAttributes(std::vector<TAttributeInfo>* attributes);

    //! Gets the value of a builtin attribute.
    /*!
     *  \returns False if there is no builtin attribute with the given key.
     */
    virtual bool GetBuiltinAttribute(const Stroka& key, NYson::IYsonConsumer* consumer) = 0;

    //! Asynchronously gets the value of a builtin attribute.
    /*!
     *  \returns Null if there is no such async builtin attribute with the given key.
     */
    virtual TFuture<void> GetBuiltinAttributeAsync(const Stroka& key, NYson::IYsonConsumer* consumer) = 0;

    //! Sets the value of a builtin attribute.
    /*!
     *  \returns False if there is no writable builtin attribute with the given key.
     */
    virtual bool SetBuiltinAttribute(const Stroka& key, const TYsonString& value) = 0;


    // Extension methods.

    //! Returns an instance of TAttributeInfo matching a given #key or |nullptr| if no such
    //! builtin attribute is known.
    TAttributeInfo* FindBuiltinAttributeInfo(const Stroka& key);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
