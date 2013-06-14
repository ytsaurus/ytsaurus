#pragma once

#include "public.h"

#include <ytlib/misc/serialize.h>

#include <ytlib/yson/public.h>

#include <ytlib/ytree/attributes.pb.h>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

//! Creates attributes dictionary in memory
std::unique_ptr<IAttributeDictionary> CreateEphemeralAttributes();

//! Creates empty attributes dictionary with deprecated method Set
const IAttributeDictionary& EmptyAttributes();

//! Serialize attributes to consumer. Used in ConvertTo* functions.
void Serialize(const IAttributeDictionary& attributes, NYson::IYsonConsumer* consumer);

//! Protobuf conversion methods.
void ToProto(NProto::TAttributes* protoAttributes, const IAttributeDictionary& attributes);
std::unique_ptr<IAttributeDictionary> FromProto(const NProto::TAttributes& protoAttributes);

//! Binary serializer.
struct TAttributeDictionarySerializer
{
    static void Save(TStreamSaveContext& context, const IAttributeDictionary& obj);
    static void Load(TStreamLoadContext& context, IAttributeDictionary& obj);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class C>
struct TSerializerTraits<NYTree::IAttributeDictionary, C, void>
{
    typedef NYTree::TAttributeDictionarySerializer TSerializer;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define ATTRIBUTE_HELPERS_INL_H_
#include "attribute_helpers-inl.h"
#undef ATTRIBUTE_HELPERS_INL_H_
