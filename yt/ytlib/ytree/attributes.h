#pragma once

#include "public.h"
#include "attributes.pb.h"

#include <ytlib/misc/nullable.h>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

struct IAttributeDictionary
{
    ~IAttributeDictionary()
    { }

    // Returns the list of all attribute names.
    virtual yhash_set<Stroka> List() = 0;

    //! Returns the value of the attribute (NULL indicates that the attribute is not found).
    virtual TNullable<TYson> FindYson(const Stroka& key) = 0;

    //! Sets the value of the attribute.
    virtual void SetYson(const Stroka& key, const TYson& value) = 0;

    //! Removes the attribute.
    virtual bool Remove(const Stroka& key) = 0;

    // Extension methods

    // TODO(babenko): make const
    //! Returns the value of the attribute (throws an exception if the attribute is not found).
    TYson GetYson(const Stroka& key);

    // TODO(babenko): make const
    template <class T>
    typename TDeserializeTraits<T>::TReturnType Get(const Stroka& key);

    template <class T>
    typename TNullableTraits<
        typename TDeserializeTraits<T>::TReturnType
    >::TNullableType Find(const Stroka& key);

    template <class T>
    void Set(const Stroka& key, const T& value);
    
    //! Converts the instance into a map node (by copying and deserliazing the values).
    TIntrusivePtr<IMapNode> ToMap();

    //! Adds more attributes from another map node.
    void MergeFrom(const IMapNode* other);

    //! Adds more attributes from another attribute dictionary.
    // TODO(babenko): make const
    void MergeFrom(IAttributeDictionary* other);
};

TAutoPtr<IAttributeDictionary> CreateEphemeralAttributes();

// TODO(babenko): add const for attributes
void ToProto(NProto::TAttributes* protoAttributes, IAttributeDictionary& attributes);
TAutoPtr<IAttributeDictionary> FromProto(const NProto::TAttributes& protoAttributes);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

#define ATTRIBUTES_INL_H_
#include "attributes-inl.h"
#undef ATTRIBUTES_INL_H_
