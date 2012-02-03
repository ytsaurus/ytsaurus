#pragma once

#include "yson_consumer.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

struct IMapNode;

struct IAttributeDictionary
    : public virtual TRefCounted
{
    typedef TIntrusivePtr<IAttributeDictionary> TPtr;

    // Returns the list of all attribute names.
    virtual yhash_set<Stroka> ListAttributes() = 0;

    //! Returns the value of the attribute (empty TYson indicates that the attribute is not found).
    virtual TYson FindAttribute(const Stroka& name) = 0;

    //! Sets the value of the attribute.
    virtual void SetAttribute(const Stroka& name, const TYson& value) = 0;

    //! Removes the attribute.
    virtual bool RemoveAttribute(const Stroka& name) = 0;


    // Extension methods

    //! Returns the value of the attribute (throws an exception if the attribute is not found).
    TYson GetAttribute(const Stroka& name);

    TIntrusivePtr<IMapNode> ToMap();
    void Merge(const IMapNode* map);
};

IAttributeDictionary::TPtr CreateInMemoryAttributeDictionary();

////////////////////////////////////////////////////////////////////////////////

struct ISystemAttributeProvider
    : public virtual TRefCounted
{
    typedef TIntrusivePtr<ISystemAttributeProvider> TPtr;

    //! Describes a system attribute.
    struct TAttributeInfo
    {
        Stroka Name;
        bool IsPresent;
        bool IsOpaque;

        TAttributeInfo(const char* name, bool isPresent = true, bool isOpaque = false)
            : Name(name)
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
     *  \returns False if there is no system attribute with the given name.
     */
    virtual bool GetSystemAttribute(const Stroka& name, IYsonConsumer* consumer) = 0;

    //! Sets the value of a system attribute.
    /*! 
     *  \returns False if the attribute cannot be set or
     *  there is no system attribute with the given name.
     */
    virtual bool SetSystemAttribute(const Stroka& name, TYsonProducer* producer) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
