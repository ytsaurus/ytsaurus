#include "immutable_chunk_meta.h"

#include <yt/yt_proto/yt/client/chunk_client/proto/chunk_meta.pb.h>

#include <library/cpp/yt/malloc/malloc.h>

namespace NYT::NChunkServer {

using namespace NChunkClient;

using NYT::ToProto;
using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

void* TImmutableChunkMeta::operator new(size_t instanceSize, size_t extensionsByteSize)
{
    YT_ASSERT(instanceSize == sizeof(TImmutableChunkMeta));
    return ::malloc(instanceSize + extensionsByteSize);
}

void TImmutableChunkMeta::operator delete(void* ptr)
{
    ::free(ptr);
}

TImmutableChunkMetaPtr TImmutableChunkMeta::CreateNull()
{
    return TImmutableChunkMetaPtr(new(0) TImmutableChunkMeta());
}

int TImmutableChunkMeta::GetExtensionCount() const
{
    return std::ssize(ExtensionDescriptors_);
}

i64 TImmutableChunkMeta::GetTotalByteSize() const
{
    return malloc_usable_size(reinterpret_cast<void*>(const_cast<TImmutableChunkMeta*>(this)));
}

bool TImmutableChunkMeta::HasExtension(int tag) const
{
    for (const auto& descriptor : ExtensionDescriptors_) {
        if (descriptor.Tag == tag) {
            return true;
        }
    }
    return false;
}

TRef TImmutableChunkMeta::FindExtensionData(int tag) const
{
    for (const auto& descriptor : ExtensionDescriptors_) {
        if (descriptor.Tag == tag) {
            return GetExtensionData(descriptor);
        }
    }
    return {};
}

TRef TImmutableChunkMeta::GetExtensionData(int tag) const
{
    auto ref = FindExtensionData(tag);
    YT_VERIFY(ref);
    return ref;
}

TRef TImmutableChunkMeta::GetExtensionData(const TExtensionDescriptor& descriptor) const
{
    auto thisOffset = descriptor.Offset;
    auto nextOffset = &descriptor == &ExtensionDescriptors_.back() ? ExtensionsByteSize_ : (&descriptor)[1].Offset;
    return TRef(
        reinterpret_cast<const char*>(this + 1) + thisOffset,
        static_cast<size_t>(nextOffset - thisOffset));
}

////////////////////////////////////////////////////////////////////////////////

void ValidateFromProto(const NChunkClient::NProto::TChunkMeta& protoMeta)
{
    // NB: FromProto should throw on unknown values, so manual checks is just to be safe (and future-proofed).
    if (FromProto<EChunkType>(protoMeta.type()) == EChunkType::Unknown) {
        THROW_ERROR TError("Unknown chunk type encountered while deserializing chunk meta")
            << TErrorAttribute("chunk_type", protoMeta.type());
    }

    // NB: FromProto should throw on unknown values, so manual checks is just to be safe (and future-proofed).
    if (FromProto<EChunkFormat>(protoMeta.format()) == EChunkFormat::Unknown) {
        THROW_ERROR TError("Unknown chunk format encountered while deserializing chunk meta")
            << TErrorAttribute("chunk_format", protoMeta.format());
    }

    // TODO(babenko): right now this doesn't throw (which is good) but doesn't do
    // what it should either. See below.
    Y_UNUSED(FromProto<EChunkFeatures>(protoMeta.features()));
}

void FromProto(
    TImmutableChunkMetaPtr* meta,
    const NChunkClient::NProto::TChunkMeta& protoMeta)
{
    size_t ExtensionsByteSize = 0;
    for (const auto& extension : protoMeta.extensions().extensions()) {
        ExtensionsByteSize += extension.data().size();
    }

    auto* rawMeta = new(ExtensionsByteSize) TImmutableChunkMeta();
    *meta = TImmutableChunkMetaPtr(rawMeta);
    rawMeta->Type_ = FromProto<EChunkType>(protoMeta.type());
    rawMeta->Format_ = FromProto<EChunkFormat>(protoMeta.format());
    // TODO(babenko): right now this converts anything unknown to
    // EChunkFeatures::Unknown which is not what should be done.
    rawMeta->Features_ = FromProto<EChunkFeatures>(protoMeta.features());

    rawMeta->ExtensionDescriptors_.reserve(protoMeta.extensions().extensions().size());
    auto* ptr = reinterpret_cast<char*>(rawMeta + 1);
    int offset = 0;
    for (const auto& extension : protoMeta.extensions().extensions()) {
        auto extensionSize = extension.data().size();
        ::memcpy(ptr, extension.data().data(), extensionSize);
        rawMeta->ExtensionDescriptors_.push_back(TImmutableChunkMeta::TExtensionDescriptor{
            .Tag = extension.tag(),
            .Offset = offset
        });
        ptr += extensionSize;
        offset += extensionSize;
    }
    rawMeta->ExtensionsByteSize_ = offset;
}

void ToProto(
    NChunkClient::NProto::TChunkMeta* protoMeta,
    const TImmutableChunkMetaPtr& meta,
    const THashSet<int>* tags,
    bool setMetaExtensions)
{
    protoMeta->set_type(ToProto(meta->Type_));
    protoMeta->set_format(ToProto(meta->Format_));
    protoMeta->set_features(ToProto(meta->Features_));

    if (!setMetaExtensions) {
        return;
    }

    for (const auto& descriptor : meta->ExtensionDescriptors_) {
        if (tags && !tags->contains(descriptor.Tag)) {
            continue;
        }
        auto* protoExtension = protoMeta->mutable_extensions()->add_extensions();
        protoExtension->set_tag(descriptor.Tag);
        auto data = meta->GetExtensionData(descriptor);
        protoExtension->set_data(data.Begin(), data.Size());
    }
}

////////////////////////////////////////////////////////////////////////////////

void TImmutableChunkMetaSerializer::Save(
    TStreamSaveContext& context,
    const TImmutableChunkMetaPtr& meta)
{
    NChunkClient::NProto::TChunkMeta protoMeta;
    ToProto(&protoMeta, meta);
    NYT::Save(context, protoMeta);
}

void TImmutableChunkMetaSerializer::Load(
    TStreamLoadContext& context,
    TImmutableChunkMetaPtr& meta)
{
    NChunkClient::NProto::TChunkMeta protoMeta;
    NYT::Load(context, protoMeta);
    FromProto(&meta, protoMeta);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
