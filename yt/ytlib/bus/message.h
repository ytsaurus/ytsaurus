#pragma once

#include "common.h"
#include <ytlib/misc/ref_counted.h>
#include <ytlib/misc/ref.h>

namespace NYT {
namespace NBus {

////////////////////////////////////////////////////////////////////////////////

//! Represents a generic message transferred via Bus.
/*!
 *  A message is just an order sequence of parts.
 *  Each part is a blob.
 *  The message owns its parts.
 */
struct IMessage
    : public virtual TRefCounted
{
    typedef TIntrusivePtr<IMessage> TPtr;

    virtual const yvector<TSharedRef>& GetParts() = 0;
};

////////////////////////////////////////////////////////////////////////////////

//! Creates a message from a list of parts.
IMessage::TPtr CreateMessageFromParts(const yvector<TSharedRef>& parts);

//! Creates a message from a list of parts.
IMessage::TPtr CreateMessageFromParts(yvector<TSharedRef>&& parts);

//! Creates a message from a single part.
IMessage::TPtr CreateMessageFromPart(const TSharedRef& part);

//! Creates a message from a blob and a bunch of refs inside it.
IMessage::TPtr CreateMessageFromParts(TBlob&& blob, const yvector<TRef>& refs);

//! Creates a message by taking a slice of another message.
//! The slice goes up to the end of the original message.
IMessage::TPtr CreateMessageFromSlice(IMessage* message, int sliceStart);

////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NYT
