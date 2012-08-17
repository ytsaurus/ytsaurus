#pragma once

#include "public.h"

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
    : public virtual TIntrinsicRefCounted
{
    virtual const std::vector<TSharedRef>& GetParts() = 0;
};

////////////////////////////////////////////////////////////////////////////////

//! Creates a message from a list of parts.
IMessagePtr CreateMessageFromParts(const std::vector<TSharedRef>& parts);

//! Creates a message from a list of parts.
IMessagePtr CreateMessageFromParts(std::vector<TSharedRef>&& parts);

//! Creates a message from a single part.
IMessagePtr CreateMessageFromPart(const TSharedRef& part);

//! Creates a message from a blob and a bunch of refs inside it.
IMessagePtr CreateMessageFromParts(TBlob&& blob, const std::vector<TRef>& refs);

//! Creates a message by taking a slice of another message.
//! The slice goes up to the end of the original message.
IMessagePtr CreateMessageFromSlice(IMessagePtr message, int sliceStart);

//! Packs message into a single blob.
TSharedRef PackMessage(IMessagePtr message);

//! Reconstructs a message earlier produced by #PackMessage.
/*!
 *  \note
 *  Message parts will be pointing to ranges inside #packedBlob.
 */
IMessagePtr UnpackMessage(const TSharedRef& packedBlob);

////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NYT
