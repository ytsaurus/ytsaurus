#pragma once

#include <ytlib/misc/common.h>

#include <ytlib/object_client/public.h>

namespace NYT {
namespace NChunkClient {

///////////////////////////////////////////////////////////////////////////////

typedef i32 TNodeId;
const TNodeId InvalidNodeId = 0;
const TNodeId MaxNodeId = (1 << 28) - 1; // TNodeId must fit into 28 bits (see TChunkReplica)

typedef NObjectClient::TObjectId TChunkId;
extern TChunkId NullChunkId;

typedef NObjectClient::TObjectId TChunkListId;
extern TChunkListId NullChunkListId;

typedef NObjectClient::TObjectId TChunkTreeId;
extern TChunkTreeId NullChunkTreeId;

typedef TGuid TJobId;

//! Used as an expected upper bound in TSmallVector.
const int TypicalReplicationFactor = 4;

DECLARE_ENUM(EJobState,
    (Running)
    (Completed)
    (Failed)
);

DECLARE_ENUM(EJobType,
    (Replicate)
    (Remove)
);

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
    ((AllTargetNodesFailed)  (700))
    ((PipelineFailed)        (701))
    ((NoSuchSession)         (702))
    ((SessionAlreadyExists)  (703))
    ((ChunkAlreadyExists)    (704))
    ((WindowError)           (705))
    ((BlockContentMismatch)  (706))
    ((NoSuchBlock)           (707))
    ((NoSuchChunk)           (708))
    ((ChunkPrecachingFailed) (709))
    ((OutOfSpace)            (710))
    ((IOError)               (711))

    ((MasterCommunicationFailed)(712))
);

////////////////////////////////////////////////////////////////////////////////

struct TRemoteReaderConfig;
typedef TIntrusivePtr<TRemoteReaderConfig> TRemoteReaderConfigPtr;

struct TClientBlockCacheConfig;
typedef TIntrusivePtr<TClientBlockCacheConfig> TClientBlockCacheConfigPtr;

struct TEncodingWriterConfig;
typedef TIntrusivePtr<TEncodingWriterConfig> TEncodingWriterConfigPtr;

struct TEncodingWriterOptions;
typedef TIntrusivePtr<TEncodingWriterOptions> TEncodingWriterOptionsPtr;

struct TDispatcherConfig;
typedef TIntrusivePtr<TDispatcherConfig> TDispatcherConfigPtr;

struct TMultiChunkWriterConfig;
typedef TIntrusivePtr<TMultiChunkWriterConfig> TMultiChunkWriterConfigPtr;

struct TMultiChunkWriterOptions;
typedef TIntrusivePtr<TMultiChunkWriterOptions> TMultiChunkWriterOptionsPtr;

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

struct TSequentialReaderConfig;
typedef TIntrusivePtr<TSequentialReaderConfig> TSequentialReaderConfigPtr;

struct TReplicationWriterConfig;
typedef TIntrusivePtr<TReplicationWriterConfig> TReplicationWriterConfigPtr;

struct TErasureWriterConfig;
typedef TIntrusivePtr<TErasureWriterConfig> TErasureWriterConfigPtr;

class TFileReader;
typedef TIntrusivePtr<TFileReader> TFileReaderPtr;

class TFileWriter;
typedef TIntrusivePtr<TFileWriter> TFileWriterPtr;

struct TNodeDescriptor;

class TNodeDirectory;
typedef TIntrusivePtr<TNodeDirectory> TNodeDirectoryPtr;

///////////////////////////////////////////////////////////////////////////////

struct TChunkIdWithIndex
{
    TChunkIdWithIndex();
    TChunkIdWithIndex(const TChunkId& id, int index);

    TChunkId Id;
    int Index;
};

Stroka ToString(const TChunkIdWithIndex& id);

///////////////////////////////////////////////////////////////////////////////

//! Returns |true| iff this is a erasure chunk.
bool IsErasureChunkId(const TChunkId& id);

//! Returns |true| iff this is a erasure chunk part.
bool IsEasureChunkPartId(const TChunkId& id);

//! Returns id for a part of a given erasure chunk.
TChunkId PartIdFromErasureChunkId(const TChunkId& id, int index);

//! Returns the whole chunk id for a given erasure chunk part id.
TChunkId ChunkIdFromErasurePartId(const TChunkId& id);

//! Returns part index for a given erasure chunk part id.
int PartIndexFromErasurePartId(const TChunkId& id);

//! For usual chunks, preserves the id and returns zero index.
//! For erasure chunks, constructs the whole chunk id and extracts index.
TChunkIdWithIndex DecodeChunkId(const TChunkId& id);
template <class TChunkWriter>
class TMultiChunkSequentialWriter;

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
