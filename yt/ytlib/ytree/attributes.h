#pragma once

#include "yson_consumer.h"
#include "attributes.pb.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

struct IMapNode;

struct IAttributeDictionary
// TODO(babenko): is ref counting needed here?
    : public virtual TRefCounted
{
    typedef TIntrusivePtr<IAttributeDictionary> TPtr;

    // TODO(babenko): rekey
    // ListAttributes -> Getkeys or List
    // FindAttribute -> FindYson
    // SetAttribute -> SetYson
    // RemoveAttribute -> Remove
    // GetAttribute -> GetYson

    // Returns the list of all attribute keys.
    virtual yhash_set<Stroka> ListAttributes() = 0;

    //! Returns the value of the attribute (empty TYson indicates that the attribute is not found).
    virtual TYson FindAttribute(const Stroka& key) = 0;

    //! Sets the value of the attribute.
    virtual void SetAttribute(const Stroka& key, const TYson& value) = 0;

    //! Removes the attribute.
    virtual bool RemoveAttribute(const Stroka& key) = 0;


    // Extension methods

    //! Returns the value of the attribute (throws an exception if the attribute is not found).
    // TODO(babenko): make const
    TYson GetAttribute(const Stroka& key);

    //! Converts the instance into a map node (by copying and deserializing the values).
    // TODO(babenko): make const
    TIntrusivePtr<IMapNode> ToMap();

    //! Adds more attributes from another map node.
    void MergeFrom(const IMapNode* other);

    //! Adds more attributes from another attribute dictionary.
    // TODO(babenko): make const
    void MergeFrom(IAttributeDictionary* other);
};

IAttributeDictionary::TPtr CreateEphemeralAttributes();
void ToProto(NProto::TAttributes* protoAttributes, IAttributeDictionary* attributes);
IAttributeDictionary::TPtr FromProto(const NProto::TAttributes& protoAttributes);

////////////////////////////////////////////////////////////////////////////////

struct IAttributeProvider
    : public virtual TRefCounted
{
    virtual IAttributeDictionary::TPtr GetAttributes() = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct ISystemAttributeProvider
    : public virtual TRefCounted
{
    typedef TIntrusivePtr<ISystemAttributeProvider> TPtr;

    //! Describes a system attribute.
    struct TAttributeInfo
    {
        Stroka key;
        bool IsPresent;
        bool IsOpaque;

        TAttributeInfo(const char* key, bool isPresent = true, bool isOpaque = false)
            : key(key)
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
    virtual bool SetSystemAttribute(const Stroka& key, TYsonProducer* producer) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
