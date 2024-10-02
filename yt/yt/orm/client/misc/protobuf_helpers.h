#pragma once

#include "public.h"

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <library/cpp/json/writer/json_value.h>

namespace NYT::NOrm::NClient {

////////////////////////////////////////////////////////////////////////////////

template <typename TField>
std::vector<TField> VectorFromProtoField(TField field);

template <template<typename> typename TRepeatedField, typename TFieldArg>
std::vector<TFieldArg> VectorFromProtoField(TRepeatedField<TFieldArg> proto);

template <class T>
::google::protobuf::RepeatedPtrField<T> VectorToProtoRepeated(std::vector<T> vector);

template <class TKey, class TValue>
::google::protobuf::Map<TKey, TValue> HashMapToProtoMap(const THashMap<TKey, TValue>& hashMap);

template <class TKey, class TValue>
THashMap<TKey, TValue> HashMapFromProtoMap(const ::google::protobuf::Map<TKey, TValue>& proto);

////////////////////////////////////////////////////////////////////////////////

NYTree::NProto::TAttributeDictionary JsonToAttributeDictionary(
    const ::NJson::TJsonValue& jsonValue);

::NJson::TJsonValue JsonFromAttributeDictionary(
    const NYTree::NProto::TAttributeDictionary& attributeDictionary);

////////////////////////////////////////////////////////////////////////////////

template <std::derived_from<google::protobuf::Message> TObject, class TValue>
void SetObjectLabel(TObject* object, const NYPath::TYPath& path, const TValue& value);

template <std::derived_from<google::protobuf::Message> TObject>
NYTree::INodePtr GetObjectLabel(const TObject& object, const NYPath::TYPath& path);

template <std::derived_from<google::protobuf::Message> TObject>
NYTree::INodePtr FindObjectLabel(const TObject& object, const NYPath::TYPath& path);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NClient

#define PROTOBUF_HELPERS_INL_H_
#include "protobuf_helpers-inl.h"
#undef PROTOBUF_HELPERS_INL_H_
