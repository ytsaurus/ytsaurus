#pragma once

#include "public.h"

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/core/misc/protobuf_helpers.h>
#include <yt/yt/core/misc/property.h>

#include <library/cpp/yt/memory/ref.h>

#include <library/cpp/yt/small_containers/compact_vector.h>

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

//! A space-efficient immutable representation of NChunkClient::NProto::TChunkMeta.
//! Stores both the static part and extensions in a single contiguous allocation.
class TImmutableChunkMeta final
{
public:
    DEFINE_BYVAL_RO_PROPERTY(NChunkClient::EChunkType, Type, NChunkClient::EChunkType::Unknown);
    DEFINE_BYVAL_RO_PROPERTY(NChunkClient::EChunkFormat, Format, NChunkClient::EChunkFormat::Unknown);
    DEFINE_BYVAL_RO_PROPERTY(NChunkClient::EChunkFeatures, Features, NChunkClient::EChunkFeatures::None);
    DEFINE_BYVAL_RO_PROPERTY(int, ExtensionsByteSize, 0);

    static TImmutableChunkMetaPtr CreateNull();

    void operator delete(void* ptr);

    int GetExtensionCount() const;

    //! Returns the total number of bytes occupied by the instance
    //! (including allocator overhead).
    i64 GetTotalByteSize() const;

    template <class T>
    bool HasExtension() const;
    bool HasExtension(int tag) const;

    template <class T>
    std::optional<T> FindExtension() const;
    TRef FindExtensionData(int tag) const;

    template <class T>
    T GetExtension() const;
    TRef GetExtensionData(int tag) const;

private:
    struct TExtensionDescriptor
    {
        int Tag;
        int Offset;
    };

    TCompactVector<TExtensionDescriptor, NChunkClient::MaxMasterChunkMetaExtensions> ExtensionDescriptors_;

    TImmutableChunkMeta() = default;

    void* operator new(size_t instanceSize, size_t extensionsSize);

    TRef GetExtensionData(const TExtensionDescriptor& descriptor) const;

    friend void FromProto(
        TImmutableChunkMetaPtr* meta,
        const NChunkClient::NProto::TChunkMeta& protoMeta);
    friend void ToProto(
        NChunkClient::NProto::TChunkMeta* protoMeta,
        const TImmutableChunkMetaPtr& meta,
        const THashSet<int>* tags,
        bool setMetaExtensions);
};

////////////////////////////////////////////////////////////////////////////////

//! Deserializes TImmutableChunkMeta from protobuf message.
void FromProto(
    TImmutableChunkMetaPtr* meta,
    const NChunkClient::NProto::TChunkMeta& protoMeta);

//! Serializes TImmutableChunkMeta to protobuf message.
/*!
 *  If #tags are null then all of the extensions are copied.
 *  Otherwise just those with ids in #tags are copied.
*/
void ToProto(
    NChunkClient::NProto::TChunkMeta* protoMeta,
    const TImmutableChunkMetaPtr& meta,
    const THashSet<int>* tags = nullptr,
    bool setMetaExtensions = true);

////////////////////////////////////////////////////////////////////////////////

struct TImmutableChunkMetaSerializer
{
    static void Save(
        TStreamSaveContext& context,
        const TImmutableChunkMetaPtr& meta);
    static void Load(
        TStreamLoadContext& context,
        TImmutableChunkMetaPtr& meta);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class C>
struct TSerializerTraits<std::unique_ptr<NChunkServer::TImmutableChunkMeta>, C, void>
{
    using TSerializer = NChunkServer::TImmutableChunkMetaSerializer;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define IMMUTABLE_CHUNK_META_INL_H_
#include "immutable_chunk_meta-inl.h"
#undef IMMUTABLE_CHUNK_META_INL_H_
