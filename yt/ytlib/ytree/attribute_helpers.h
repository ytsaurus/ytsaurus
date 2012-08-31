#pragma once

#include "public.h"

#include <ytlib/ytree/attributes.pb.h>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

//! Creates attributes dictionary in memory
TAutoPtr<IAttributeDictionary> CreateEphemeralAttributes();

//! Creates empty attributes dictionary with deprecated method Set
const IAttributeDictionary& EmptyAttributes();

//! Serialize attributes to consumer. Used in ConvertTo* functions.
void Serialize(const IAttributeDictionary& attributes, IYsonConsumer* consumer);

//! Protobuf conversion methods.
void ToProto(NProto::TAttributes* protoAttributes, const IAttributeDictionary& attributes);
TAutoPtr<IAttributeDictionary> FromProto(const NProto::TAttributes& protoAttributes);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

#define ATTRIBUTE_HELPERS_INL_H_
#include "attribute_helpers-inl.h"
#undef ATTRIBUTE_HELPERS_INL_H_
