#ifndef IMMUTABLE_CHUNK_META_INL_H_
#error "Direct inclusion of this file is not allowed, include immutable_chunk_meta.h"
// For the sake of sane code completion.
#include "immutable_chunk_meta.h"
#endif

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

template <class T>
bool TImmutableChunkMeta::HasExtension() const
{
    return HasExtension(TProtoExtensionTag<T>::Value);
}

template <class T>
std::optional<T> TImmutableChunkMeta::FindExtension() const
{
    auto ref = FindExtensionData(TProtoExtensionTag<T>::Value);
    if (!ref) {
        return {};
    }
    std::optional<T> result;
    result.emplace();
    DeserializeProto(&*result, ref);
    return result;
}

template <class T>
T TImmutableChunkMeta::GetExtension() const
{
    auto ref = GetExtensionData(TProtoExtensionTag<T>::Value);
    T result;
    DeserializeProto(&result, ref);
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
