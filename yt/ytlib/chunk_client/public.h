#pragma once

#include <core/misc/common.h>
#include <core/misc/small_vector.h>

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
    ((AddressNotFound)          (713))
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

