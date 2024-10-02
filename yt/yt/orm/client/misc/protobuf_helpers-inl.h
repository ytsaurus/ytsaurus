#ifndef PROTOBUF_HELPERS_INL_H_
#error "Direct inclusion of this file is not allowed, include protobuf_helpers.h"
// For the sake of sane code completion.
#include "protobuf_helpers.h"
#endif

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/ypath_client.h>

namespace NYT::NOrm::NClient {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
std::vector<T> VectorFromProtoField(T field)
{
    return {std::move(field)};
}

template <template<typename> typename T, typename U>
    requires std::same_as<T<U>, std::remove_const_t<::google::protobuf::RepeatedField<U>>> ||
        std::same_as<T<U>, std::remove_const_t<::google::protobuf::RepeatedPtrField<U>>>
std::vector<U> VectorFromProtoField(T<U> field)
{
    return {
        std::make_move_iterator(field.begin()),
        std::make_move_iterator(field.end()),
    };
}

template <class T>
::google::protobuf::RepeatedPtrField<T> VectorToProtoRepeated(std::vector<T> vector)
{
    return {
        std::make_move_iterator(vector.begin()),
        std::make_move_iterator(vector.end()),
    };
}

template <class TKey, class TValue>
::google::protobuf::Map<TKey, TValue> HashMapToProtoMap(const THashMap<TKey, TValue>& hashMap)
{
    ::google::protobuf::Map<TKey, TValue> result;
    for (const auto& [key, value] : hashMap) {
        InsertOrCrash(result, ::google::protobuf::MapPair<TKey, TValue>(key, value));
    }
    return result;
}

template <class TKey, class TValue>
THashMap<TKey, TValue> HashMapFromProtoMap(const ::google::protobuf::Map<TKey, TValue>& proto)
{
    THashMap<TKey, TValue> result;
    for (const auto& [key, value] : proto) {
        EmplaceOrCrash(result, key, value);
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

template <std::derived_from<google::protobuf::Message> TObject, class TValue>
void SetObjectLabel(TObject* object, const NYPath::TYPath& path, const TValue& value)
{
    auto labels = NYTree::ConvertToNode(object->labels());
    NYTree::SetNodeByYPath(labels, path, NYTree::ConvertToNode(value), /*force*/ true);
    *object->mutable_labels() = NYTree::ConvertTo<NYTree::NProto::TAttributeDictionary>(labels);
}

template <std::derived_from<google::protobuf::Message> TObject>
NYTree::INodePtr GetObjectLabel(const TObject& object, const NYPath::TYPath& path)
{
    auto labels = NYTree::ConvertToNode(object.labels());
    return NYTree::GetNodeByYPath(labels, path);
}

template <std::derived_from<google::protobuf::Message> TObject>
NYTree::INodePtr FindObjectLabel(const TObject& object, const NYPath::TYPath& path)
{
    auto labels = NYTree::ConvertToNode(object.labels());
    return NYTree::FindNodeByYPath(labels, path);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NClient
