#pragma once

#include "public.h"

#include <ytlib/misc/nullable.h>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

struct IMapNode;

struct IAttributeDictionary
    : public virtual TRefCounted
{
    typedef TIntrusivePtr<IAttributeDictionary> TPtr;

    // Returns the list of all attribute names.
    virtual yhash_set<Stroka> List() = 0;

    //! Returns the value of the attribute (empty TYson indicates that the attribute is not found).
    virtual TYson FindYson(const Stroka& name) = 0;

    //! Sets the value of the attribute.
    virtual void SetYson(const Stroka& name, const TYson& value) = 0;

    //! Removes the attribute.
    virtual bool Remove(const Stroka& name) = 0;


    // Extension methods

    //! Returns the value of the attribute (throws an exception if the attribute is not found).
    TYson GetYson(const Stroka& name);

    template <class T>
    typename TDeserializeTraits<T>::TReturnType Get(const Stroka& name);

    template <class T>
    typename TNullableTraits<
        typename TDeserializeTraits<T>::TReturnType
    >::TNullableType Find(const Stroka& name);
    
    //! Converts the instance into a map node (by copying and deserliazing the values).
    TIntrusivePtr<IMapNode> ToMap();

    //! Adds more attributes from another map node.
    void MergeFrom(const IMapNode* map);
};

IAttributeDictionary::TPtr CreateInMemoryAttributeDictionary();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

#define ATTRIBUTES_INL_H_
#include "attributes-inl.h"
#undef ATTRIBUTES_INL_H_
