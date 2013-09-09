#pragma once

#include "public.h"

#include <core/misc/serialize.h>

#include <core/yson/public.h>

#include <core/ytree/attributes.pb.h>

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

//! By-value binary serializer.
struct TAttributeDictionaryValueSerializer
{
    static void Save(TStreamSaveContext& context, const IAttributeDictionary& obj);
    static void Load(TStreamLoadContext& context, IAttributeDictionary& obj);
};

//! By-ref binary serializer.
struct TAttributeDictionaryRefSerializer
{
    static void Save(TStreamSaveContext& context, const std::unique_ptr<IAttributeDictionary>& obj);
    static void Load(TStreamLoadContext& context, std::unique_ptr<IAttributeDictionary>& obj);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class C>
struct TSerializerTraits<NYTree::IAttributeDictionary, C, void>
{
    typedef NYTree::TAttributeDictionaryValueSerializer TSerializer;
};

template <class C>
struct TSerializerTraits<std::unique_ptr<NYTree::IAttributeDictionary>, C, void>
{
    typedef NYTree::TAttributeDictionaryRefSerializer TSerializer;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define ATTRIBUTE_HELPERS_INL_H_
#include "attribute_helpers-inl.h"
#undef ATTRIBUTE_HELPERS_INL_H_
