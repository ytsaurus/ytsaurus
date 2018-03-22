#pragma once

#include <yt/ytlib/misc/public.h>

#include <yt/ytlib/object_client/public.h>

#include <yt/ytlib/node_tracker_client/public.h>

#include <yt/core/misc/public.h>
#include <yt/core/misc/small_vector.h>
#include <yt/core/misc/nullable.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TChunkInfo;
class TChunkSpec;
class TChunkMeta;
class TMiscExt;

class TReadRange;

class TReqFetch;

class TDataStatistics;

class TReqExportChunks;
class TRspExportChunks;

class TReqImportChunks;
class TRspImportChunks;

class TReqExecuteBatch;
class TRspExecuteBatch;

class TMediumDirectory;

class TDataSource;
class TDataSourceDirectoryExt;

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

struct TBlock;

using TChunkId = NObjectClient::TObjectId;
extern const TChunkId NullChunkId;

using TChunkListId = NObjectClient::TObjectId;
extern const TChunkListId NullChunkListId;

using TChunkTreeId = NObjectClient::TObjectId;
extern const TChunkTreeId NullChunkTreeId;

using TMediumId = NObjectClient::TObjectId;

using TReadSessionId = NObjectClient::TObjectId;

struct TSessionId;

const int DefaultPartIndex = -1;

const int MinReplicationFactor = 1;
const int MaxReplicationFactor = 10;
const int DefaultReplicationFactor = 3;

//! Estimated memory overhead per chunk reader.
const i64 ChunkReaderMemorySize = 16_KB;

//! Used as an expected upper bound in SmallVector.
/*
 *  Maximum regular number of replicas is 16 (for LRC codec).
 *  Additional +8 enables some flexibility during balancing.
 */
const int TypicalReplicaCount = 24;

constexpr int MaxMediumCount = 7;
constexpr int DefaultStoreMediumIndex = 0;
constexpr int DefaultCacheMediumIndex = 1;
extern const TString DefaultStoreAccountName;
extern const TString DefaultStoreMediumName;
extern const TString DefaultCacheMediumName;
constexpr int MaxMediumPriority = 10;

const i64 DefaultMaxBlockSize = 16_MB;
const int MaxInputChunkReplicaCount = 16;

class TChunkReplica;
using TChunkReplicaList = SmallVector<TChunkReplica, TypicalReplicaCount>;

//! Represents an offset inside a chunk.
using TBlockOffset = i64;

//! A |(chunkId, blockIndex)| pair.
struct TBlockId;

DEFINE_BIT_ENUM(EBlockType,
    ((None)              (0x0000))
    ((CompressedData)    (0x0001))
    ((UncompressedData)  (0x0002))
);

DEFINE_ENUM(EChunkType,
    ((Unknown) (0))
    ((File)    (1))
    ((Table)   (2))
    ((Journal) (3))
);

DEFINE_ENUM(EIOEngineType,
    (ThreadPool)
    (Aio)
);

const int GenericChunkReplicaIndex = 16;  // no specific replica; the default one for regular chunks

// Journal chunks only:
const int ActiveChunkReplicaIndex   = 0; // the replica is currently being written
const int UnsealedChunkReplicaIndex = 1; // the replica is finished but not sealed yet
const int SealedChunkReplicaIndex   = 2; // the replica is finished and sealed

//! Valid indexes are in range |[0, ChunkReplicaIndexBound)|.
const int ChunkReplicaIndexBound = 32;

//! For pretty-printing only.
DEFINE_ENUM(EJournalReplicaType,
    ((Generic)   (GenericChunkReplicaIndex))
    ((Active)    (ActiveChunkReplicaIndex))
    ((Unsealed)  (UnsealedChunkReplicaIndex))
    ((Sealed)    (SealedChunkReplicaIndex))
);

const int AllMediaIndex = MaxMediumCount; // passed to various APIs to indicate that any medium is OK
const int InvalidMediumIndex = -1;

//! Valid indexes (including sentinels) are in range |[0, MediumIndexBound)|.
const int MediumIndexBound = AllMediaIndex + 1;

DEFINE_ENUM(EErrorCode,
    ((AllTargetNodesFailed)     (700))
    ((SendBlocksFailed)         (701))
    ((NoSuchSession)            (702))
    ((SessionAlreadyExists)     (703))
    ((ChunkAlreadyExists)       (704))
    ((WindowError)              (705))
    ((BlockContentMismatch)     (706))
    ((NoSuchBlock)              (707))
    ((NoSuchChunk)              (708))
    ((NoLocationAvailable)      (710))
    ((IOError)                  (711))
    ((MasterCommunicationFailed)(712))
    ((NoSuchChunkTree)          (713))
    ((NoSuchChunkList)          (717))
    ((MasterNotConnected)       (714))
    ((ChunkUnavailable)         (716))
    ((WriteThrottlingActive)    (718))
    ((NoSuchMedium)             (719))
    ((OptimisticLockFailure)    (720))
    ((InvalidBlockChecksum)     (721))
    ((BlockOutOfRange)          (722))
    ((ObjectNotReplicated)      (723))
);

//! Values must be contiguous.
DEFINE_ENUM(ESessionType,
    ((User)                     (0))
    ((Replication)              (1))
    ((Repair)                   (2))
);

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TReplicationReaderConfig)
DECLARE_REFCOUNTED_CLASS(TErasureReaderConfig)
DECLARE_REFCOUNTED_CLASS(TRemoteReaderOptions)
DECLARE_REFCOUNTED_CLASS(TEncodingWriterOptions)
DECLARE_REFCOUNTED_CLASS(TDispatcherConfig)
DECLARE_REFCOUNTED_CLASS(TMultiChunkWriterConfig)
DECLARE_REFCOUNTED_CLASS(TMultiChunkWriterOptions)
DECLARE_REFCOUNTED_CLASS(TMultiChunkReaderConfig)
DECLARE_REFCOUNTED_CLASS(TMultiChunkReaderOptions)
DECLARE_REFCOUNTED_CLASS(TFetchChunkSpecConfig)
DECLARE_REFCOUNTED_CLASS(TBlockFetcherConfig)
DECLARE_REFCOUNTED_CLASS(TReplicationWriterConfig)
DECLARE_REFCOUNTED_CLASS(TRemoteWriterOptions)
DECLARE_REFCOUNTED_CLASS(TErasureWriterConfig)
DECLARE_REFCOUNTED_CLASS(TEncodingWriterConfig)
DECLARE_REFCOUNTED_CLASS(TFetcherConfig)
DECLARE_REFCOUNTED_CLASS(TBlockCacheConfig)
DECLARE_REFCOUNTED_CLASS(TChunkScraperConfig)
DECLARE_REFCOUNTED_CLASS(TChunkTeleporterConfig)

DECLARE_REFCOUNTED_STRUCT(IFetcherChunkScraper)

DECLARE_REFCOUNTED_CLASS(TEncodingWriter)
DECLARE_REFCOUNTED_CLASS(TEncodingChunkWriter)
DECLARE_REFCOUNTED_CLASS(TBlockFetcher)
DECLARE_REFCOUNTED_CLASS(TSequentialBlockFetcher)

DECLARE_REFCOUNTED_STRUCT(IChunkReader)
DECLARE_REFCOUNTED_STRUCT(IChunkWriter)

DECLARE_REFCOUNTED_STRUCT(IChunkReaderAllowingRepair)

DECLARE_REFCOUNTED_STRUCT(IReaderBase)
DECLARE_REFCOUNTED_STRUCT(IReaderFactory)

DECLARE_REFCOUNTED_CLASS(TTrafficMeter)

DECLARE_REFCOUNTED_STRUCT(IChunkWriterBase)
DECLARE_REFCOUNTED_STRUCT(IMultiChunkWriter)

DECLARE_REFCOUNTED_STRUCT(IBlockCache)

DECLARE_REFCOUNTED_STRUCT(IIOEngine)

DECLARE_REFCOUNTED_CLASS(TFileReader)
DECLARE_REFCOUNTED_CLASS(TFileWriter)

DECLARE_REFCOUNTED_CLASS(TMemoryWriter)

DECLARE_REFCOUNTED_CLASS(TInputChunk)
DECLARE_REFCOUNTED_CLASS(TInputChunkSlice)

DECLARE_REFCOUNTED_STRUCT(TInputDataSlice)

DECLARE_REFCOUNTED_CLASS(TDataSourceDirectory)

DECLARE_REFCOUNTED_CLASS(TChunkScraper)
DECLARE_REFCOUNTED_CLASS(TScraperTask)
DECLARE_REFCOUNTED_CLASS(TThrottlerManager)
DECLARE_REFCOUNTED_CLASS(TChunkTeleporter)
DECLARE_REFCOUNTED_CLASS(TMediumDirectory)

class TReadLimit;

using TRefCountedChunkMeta = TRefCountedProto<NChunkClient::NProto::TChunkMeta>;
DECLARE_REFCOUNTED_TYPE(TRefCountedChunkMeta)

using TPlacementId = TGuid;

struct TDataSliceDescriptor;

struct TInterruptDescriptor;

class TCodecStatistics;

DECLARE_REFCOUNTED_CLASS(TKeySetWriter)

using TDataCenterName = TNullable<TString>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
