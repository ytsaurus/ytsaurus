#pragma once

#include <ytlib/misc/common.h>
#include <ytlib/misc/small_vector.h>

#include <ytlib/object_client/public.h>

///////////////////////////////////////////////////////////////////////////////

// Forward declarations.
namespace NYT {

class TBlobOutput;
class TFakeStringBufStore;

} // namespace NYT

///////////////////////////////////////////////////////////////////////////////

namespace NYT {
namespace NChunkClient {

///////////////////////////////////////////////////////////////////////////////

typedef NObjectClient::TObjectId TChunkId;
extern TChunkId NullChunkId;

typedef NObjectClient::TObjectId TChunkListId;
extern TChunkListId NullChunkListId;

typedef NObjectClient::TObjectId TChunkTreeId;
extern TChunkTreeId NullChunkTreeId;

//! Used as an expected upper bound in TSmallVector.
/*
 *  Maximum regular number of replicas is 16 (for LRC codec).
 *  Additional +8 enables some flexibility during balancing.
 */
const int TypicalReplicaCount = 24;

class TChunkReplica;
typedef TSmallVector<TChunkReplica, TypicalReplicaCount> TChunkReplicaList;

//! Represents an offset inside a chunk.
typedef i64 TBlockOffset;

//! A |(chunkId, blockIndex)| pair.
struct TBlockId;

DECLARE_ENUM(EChunkType,
    ((Unknown) (0))
    ((File)    (1))
    ((Table)   (2))
);

DECLARE_ENUM(EErrorCode,
    ((AllTargetNodesFailed)     (700))
    ((PipelineFailed)           (701))
    ((NoSuchSession)            (702))
    ((SessionAlreadyExists)     (703))
    ((ChunkAlreadyExists)       (704))
    ((WindowError)              (705))
    ((BlockContentMismatch)     (706))
    ((NoSuchBlock)              (707))
    ((NoSuchChunk)              (708))
    ((ChunkPrecachingFailed)    (709))
    ((OutOfSpace)               (710))
    ((IOError)                  (711))

    ((MasterCommunicationFailed)(712))
);

//! Values must be contiguous.
DECLARE_ENUM(EWriteSessionType,
    ((User)                     (0))
    ((Replication)              (1))
    ((Repair)                   (2))
);

DECLARE_ENUM(EReadSessionType,
    ((User)                     (0))
    ((Repair)                   (1))
);

////////////////////////////////////////////////////////////////////////////////

class TReplicationReaderConfig;
typedef TIntrusivePtr<TReplicationReaderConfig> TReplicationReaderConfigPtr;

class TClientBlockCacheConfig;
typedef TIntrusivePtr<TClientBlockCacheConfig> TClientBlockCacheConfigPtr;

class TEncodingWriterConfig;
typedef TIntrusivePtr<TEncodingWriterConfig> TEncodingWriterConfigPtr;

struct TEncodingWriterOptions;
typedef TIntrusivePtr<TEncodingWriterOptions> TEncodingWriterOptionsPtr;

class TDispatcherConfig;
typedef TIntrusivePtr<TDispatcherConfig> TDispatcherConfigPtr;

class TMultiChunkWriterConfig;
typedef TIntrusivePtr<TMultiChunkWriterConfig> TMultiChunkWriterConfigPtr;

struct TMultiChunkWriterOptions;
typedef TIntrusivePtr<TMultiChunkWriterOptions> TMultiChunkWriterOptionsPtr;

struct TMultiChunkReaderConfig;
typedef TIntrusivePtr<TMultiChunkReaderConfig> TMultiChunkReaderConfigPtr;

class TEncodingWriter;
typedef TIntrusivePtr<TEncodingWriter> TEncodingWriterPtr;

struct IAsyncWriter;
typedef TIntrusivePtr<IAsyncWriter> IAsyncWriterPtr;

struct IAsyncReader;
typedef TIntrusivePtr<IAsyncReader> IAsyncReaderPtr;

class TSequentialReader;
typedef TIntrusivePtr<TSequentialReader> TSequentialReaderPtr;

struct IBlockCache;
typedef TIntrusivePtr<IBlockCache> IBlockCachePtr;

class TSequentialReaderConfig;
typedef TIntrusivePtr<TSequentialReaderConfig> TSequentialReaderConfigPtr;

class TReplicationWriterConfig;
typedef TIntrusivePtr<TReplicationWriterConfig> TReplicationWriterConfigPtr;

class TErasureWriterConfig;
typedef TIntrusivePtr<TErasureWriterConfig> TErasureWriterConfigPtr;

class TFileReader;
typedef TIntrusivePtr<TFileReader> TFileReaderPtr;

class TFileWriter;
typedef TIntrusivePtr<TFileWriter> TFileWriterPtr;

template <class TChunkWriter>
class TMultiChunkSequentialWriter;

///////////////////////////////////////////////////////////////////////////////

struct TChunkIdWithIndex
{
    TChunkIdWithIndex();
    TChunkIdWithIndex(const TChunkId& id, int index);

    TChunkId Id;
    int Index;

    //! Indicates that an instance of TChunkIdWithIndex refers to the whole chunk,
    //! not to any of its replicas.
    static const int GenericPartIndex = -1;
};

bool operator == (const TChunkIdWithIndex& lhs, const TChunkIdWithIndex& rhs);
bool operator != (const TChunkIdWithIndex& lhs, const TChunkIdWithIndex& rhs);

Stroka ToString(const TChunkIdWithIndex& id);

///////////////////////////////////////////////////////////////////////////////

//! Returns |true| iff this is a erasure chunk.
bool IsErasureChunkId(const TChunkId& id);

//! Returns |true| iff this is a erasure chunk part.
bool IsErasureChunkPartId(const TChunkId& id);

//! Returns id for a part of a given erasure chunk.
TChunkId ErasurePartIdFromChunkId(const TChunkId& id, int index);

//! Returns the whole chunk id for a given erasure chunk part id.
TChunkId ErasureChunkIdFromPartId(const TChunkId& id);

//! Returns part index for a given erasure chunk part id.
int IndexFromErasurePartId(const TChunkId& id);

//! For usual chunks, preserves the id.
//! For erasure chunks, constructs the part id using the given replica index.
TChunkId EncodeChunkId(const TChunkIdWithIndex& idWithIndex);

//! For usual chunks, preserves the id and returns zero index.
//! For erasure chunks, constructs the whole chunk id and extracts index.
TChunkIdWithIndex DecodeChunkId(const TChunkId& id);

template <class TBuffer>
class TKey;

template <class TStrType>
class TKeyPart;

typedef TKey<TBlobOutput> TOwningKey;
typedef TKey<TFakeStringBufStore> TNonOwningKey;

struct TRefCountedChunkSpec;
typedef TIntrusivePtr<TRefCountedChunkSpec> TRefCountedChunkSpecPtr;

struct TChunkSlice;
typedef TIntrusivePtr<TChunkSlice> TChunkSlicePtr;

class TChannel;
typedef std::vector<TChannel> TChannels;

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

///////////////////////////////////////////////////////////////////////////////

DECLARE_PODTYPE(NYT::NChunkClient::TChunkIdWithIndex)

//! A hasher for TChunkIdWithIndex.
template <>
struct hash<NYT::NChunkClient::TChunkIdWithIndex>
{
    inline size_t operator()(const NYT::NChunkClient::TChunkIdWithIndex& value) const
    {
        return THash<NYT::NChunkClient::TChunkId>()(value.Id) * 497 +
            value.Index;
    }
};

///////////////////////////////////////////////////////////////////////////////
