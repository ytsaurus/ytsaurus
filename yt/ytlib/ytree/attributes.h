#pragma once

#include "public.h"

#include <ytlib/misc/nullable.h>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

struct IAttributeDictionary
{
    virtual ~IAttributeDictionary();
    
    // Returns the list of all attribute names.
    virtual std::vector<Stroka> List() const = 0;

    //! Returns the value of the attribute (NULL indicates that the attribute is not found).
    virtual TNullable<TYsonString> FindYson(const Stroka& key) const = 0;

    //! Sets the value of the attribute.
    virtual void SetYson(const Stroka& key, const TYsonString& value) = 0;

    //! Removes the attribute.
    virtual bool Remove(const Stroka& key) = 0;

    // Extension methods

    //! Removes all attributes.
    void Clear();

    //! Returns the value of the attribute (throws an exception if the attribute is not found).
    TYsonString GetYson(const Stroka& key) const;

    template <class T>
    T Get(const Stroka& key) const;

    template <class T>
    T Get(const Stroka& key, const T& defaultValue) const;

    template <class T>
    typename TNullableTraits<T>::TNullableType Find(const Stroka& key) const;

    template <class T>
    void Set(const Stroka& key, const T& value);
    
    //! Constructs an instance from a map node (by serializing the values).
    static TAutoPtr<IAttributeDictionary> FromMap(IMapNodePtr node);

    //! Adds more attributes from another map node.
    void MergeFrom(const IMapNodePtr other);

    //! Adds more attributes from another attribute dictionary.
    void MergeFrom(const IAttributeDictionary& other);

    TAutoPtr<IAttributeDictionary> Clone() const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

