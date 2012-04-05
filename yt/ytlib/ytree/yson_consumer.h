#pragma once

#include "public.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

//! A SAX-like interface to YTree.
/*!
 *  Describes a bunch of events that are raised during a YTree traversal.
 */
struct IYsonConsumer
{
    virtual ~IYsonConsumer()
    { }

    //! The current item is a string scalar (IStringNode).
    /*!
     *  \param value A scalar value.
     *  \param hasAttributes Tells if the scalar is followed by the description of its attributes.
     */
    virtual void OnStringScalar(const TStringBuf& value, bool hasAttributes = false) = 0;

    //! The current item is an integer scalar (IIntegerNode).
    /*!
     *  \param value A scalar value.
     *  \param hasAttributes Tells if the scalar is followed by the description of its attributes.
     */
    virtual void OnIntegerScalar(i64 value, bool hasAttributes = false) = 0;

    //! The current item is an FP scalar (IDoubleNode).
    /*!
     *  \param value A scalar value.
     *  \param hasAttributes Tells if the scalar is followed by the description of its attributes.
     */
    virtual void OnDoubleScalar(double value, bool hasAttributes = false) = 0;
    
    //! The current item is an entity (IEntityNode).
    /*!
     *  \param hasAttributes Tells if the entity is followed by the description of its attributes.
     */
    virtual void OnEntity(bool hasAttributes = false) = 0;

    //! Starts a list (IListNode).
    /*!
     *  The events describing a list are raised as follows:
     *  - #OnBeginList
     *  - For each item: #OnListItem followed by the description of the item
     *  - #OnEndList
     *  
     *  The list may also have attributes attached to it (see #OnEndList).
     */
    virtual void OnBeginList() = 0;

    //! Designates a list item.
    virtual void OnListItem() = 0;

    //! Ends the current list.
    /*!
     *  \param hasAttributes Tells if the list is followed by the description of its attributes.
     */
    virtual void OnEndList(bool hasAttributes = false) = 0;

    //! Starts a map (IMapNode).
    /*!
     *  The events describing a map are raised as follows:
     *  - #OnBeginMap
     *  - For each item: #OnMapItem followed by the description of the item
     *  - #OnEndMap
     *  
     *  The map may also have attributes attached to it (see #OnEndMap).
     */
    virtual void OnBeginMap() = 0;

    //! Designates a list item.
    /*!
     *  \param key Item key in the map.
     */
    virtual void OnMapItem(const TStringBuf& key) = 0;

    //! Ends the current map.
    /*!
     *  \param hasAttributes Tells if the map is followed by the description of its attributes.
     */
    virtual void OnEndMap(bool hasAttributes = false) = 0;

    //! Starts an attribute map.
    /*!
     *  This may only be called after #OnStringScalar, #OnIntegerScalar, #OnDoubleScalar, #OnEntity,
     *  #OnEndList or #OnEndMap with "hasAttirubtes" flag set to True.
     *  
     *  The events describing attributes are raised as follows:
     *  - #OnBeginAttributes
     *  - For each attribute: #OnAttributeItem followed by the description of the item
     *  - #OnEndAttributes
     *  
     */
    virtual void OnBeginAttributes() = 0;

    //! Designates an attribute.
    /*!
     *  \param key An attribute key.
     */
    virtual void OnAttributesItem(const TStringBuf& key) = 0;

    //! Ends the current attribute map.
    virtual void OnEndAttributes() = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

