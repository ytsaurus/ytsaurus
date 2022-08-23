#pragma once

#include "client.h"

#include <yt/yt/client/chunk_client/public.h>

#include <yt/yt/library/erasure/public.h>

#include <library/cpp/yt/memory/ref.h>

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

struct THunkDescriptor
{
    NChunkClient::TChunkId ChunkId;
    NErasure::ECodec ErasureCodec = NErasure::ECodec::None;
    int BlockIndex = -1;
    i64 BlockOffset = -1;
    i64 Length = -1;
    std::optional<i64> BlockSize;
};

////////////////////////////////////////////////////////////////////////////////

class TSerializableHunkDescriptor
    : public THunkDescriptor
    , public NYTree::TYsonStruct
{
public:
    TSerializableHunkDescriptor(const THunkDescriptor& descriptor);

    REGISTER_YSON_STRUCT(TSerializableHunkDescriptor);

    static void Register(TRegistrar registrar);
};

using TSerializableHunkDescriptorPtr = TIntrusivePtr<TSerializableHunkDescriptor>;

////////////////////////////////////////////////////////////////////////////////

struct TReadHunksOptions
    : public TTimeoutOptions
{
    NChunkClient::TChunkFragmentReaderConfigPtr Config;
};

////////////////////////////////////////////////////////////////////////////////

struct TWriteHunksOptions
    : public TTimeoutOptions
{ };

////////////////////////////////////////////////////////////////////////////////

struct TLockHunkStoreOptions
    : public TTimeoutOptions
{ };

////////////////////////////////////////////////////////////////////////////////

struct TUnlockHunkStoreOptions
    : public TTimeoutOptions
{ };

////////////////////////////////////////////////////////////////////////////////

//! Provides a set of private APIs.
/*!
 *  Only native clients are expected to implement this.
 */
struct IInternalClient
    : public virtual TRefCounted
{
    virtual TFuture<std::vector<TSharedRef>> ReadHunks(
        const std::vector<THunkDescriptor>& descriptors,
        const TReadHunksOptions& options = {}) = 0;

    virtual TFuture<std::vector<THunkDescriptor>> WriteHunks(
        const NYTree::TYPath& path,
        int tabletIndex,
        const std::vector<TSharedRef>& payloads,
        const TWriteHunksOptions& options = {}) = 0;

    virtual TFuture<void> LockHunkStore(
        const NYTree::TYPath& path,
        int tabletIndex,
        NTabletClient::TStoreId storeId,
        NTabletClient::TTabletId lockerTabletId,
        const TLockHunkStoreOptions& options = {}) = 0;
    virtual TFuture<void> UnlockHunkStore(
        const NYTree::TYPath& path,
        int tabletIndex,
        NTabletClient::TStoreId storeId,
        NTabletClient::TTabletId lockerTabletId,
        const TUnlockHunkStoreOptions& options = {}) = 0;
};

DEFINE_REFCOUNTED_TYPE(IInternalClient)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
