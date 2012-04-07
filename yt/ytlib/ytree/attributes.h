#pragma once

#include "public.h"
#include <ytlib/ytree/attributes.pb.h>

#include <ytlib/misc/nullable.h>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

struct IAttributeDictionary
{
    ~IAttributeDictionary()
    { }

    // Returns the list of all attribute names.
    virtual yhash_set<Stroka> List() const = 0;

    //! Returns the value of the attribute (NULL indicates that the attribute is not found).
    virtual TNullable<TYson> FindYson(const Stroka& key) const = 0;

    //! Sets the value of the attribute.
    virtual void SetYson(const Stroka& key, const TYson& value) = 0;

    //! Removes the attribute.
    virtual bool Remove(const Stroka& key) = 0;

    // Extension methods

    //! Removes all attributes.
    void Clear();

    //! Returns the value of the attribute (throws an exception if the attribute is not found).
    TYson GetYson(const Stroka& key) const;

    template <class T>
    typename TDeserializeTraits<T>::TReturnType Get(const Stroka& key) const;

    template <class T>
    T Get(const Stroka& key, const T& defaultValue) const;
    // If you need this method for ref-counted obects you should add special implementation.

    template <class T>
    typename TNullableTraits<
        typename TDeserializeTraits<T>::TReturnType
    >::TNullableType Find(const Stroka& key) const;

    template <class T>
    void Set(const Stroka& name, const T& value);
    
    //! Converts the instance into a map node (by copying and deserializing the values).
    IMapNodePtr ToMap() const;

    //! Constructs an instance from a map node (by serializing the values).
    static TAutoPtr<IAttributeDictionary> FromMap(IMapNodePtr node);

    //! Adds more attributes from another map node.
    void MergeFrom(const IMapNodePtr other);

    //! Adds more attributes from another attribute dictionary.
    void MergeFrom(const IAttributeDictionary& other);
};

TAutoPtr<IAttributeDictionary> CreateEphemeralAttributes();
const IAttributeDictionary& EmptyAttributes();

void ToProto(NProto::TAttributes* protoAttributes, const IAttributeDictionary& attributes);
TAutoPtr<IAttributeDictionary> FromProto(const NProto::TAttributes& protoAttributes);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

#define ATTRIBUTES_INL_H_
#include "attributes-inl.h"
#undef ATTRIBUTES_INL_H_
