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
     */
    virtual void OnStringScalar(const TStringBuf& value) = 0;

    //! The current item is an integer scalar (IIntegerNode).
    /*!
     *  \param value A scalar value.
     */
    virtual void OnIntegerScalar(i64 value) = 0;

    //! The current item is an FP scalar (IDoubleNode).
    /*!
     *  \param value A scalar value.
     */
    virtual void OnDoubleScalar(double value) = 0;
    
    //! The current item is an entity (IEntityNode).
    virtual void OnEntity() = 0;

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
    virtual void OnEndList() = 0;

    //! Starts a map (IMapNode).
    /*!
     *  The events describing a map are raised as follows:
     *  - #OnBeginMap
     *  - For each item: #OnKeyedItem followed by the description of the item
     *  - #OnEndMap
     */
    virtual void OnBeginMap() = 0;

    //! Designates a keyed item (in map or in attributes).
    /*!
     *  \param key Item key in the map.
     */
    virtual void OnKeyedItem(const TStringBuf& key) = 0;

    //! Ends the current map.
    virtual void OnEndMap() = 0;

    //! Starts attributes.
    /*!
     *  The events describing attributes are raised as follows:
     *  - #OnBeginAttributes
     *  - For each item: #OnKeyedItem followed by the description of the item
     *  - #OnEndAttributes
     */
    virtual void OnBeginAttributes() = 0;

    //! Ends the current attribute list.
    virtual void OnEndAttributes() = 0;

    //! Inserts YSON-serialized node or fragment.
    /*!
     *  \param yson Serialized data.
     *  \param type Type of data.
     */
    virtual void OnRaw(const TStringBuf& yson, EYsonType type) = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct TYsonConsumerBase
    : public virtual IYsonConsumer
{
    virtual void OnRaw(const TStringBuf& ysonNode, EYsonType type);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

