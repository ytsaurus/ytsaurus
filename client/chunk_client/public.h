#pragma once

#include <yt/core/misc/public.h>
#include <yt/core/misc/small_vector.h>

#include <yt/client/object_client/public.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TChunkInfo;
class TChunkSpec;
class TChunkMeta;
class TMiscExt;

class TDataStatistics;

class TReadRange;

class TMediumDirectory;

} // namespace NProto

DEFINE_ENUM(EErrorCode,
    ((AllTargetNodesFailed)      (700))
    ((SendBlocksFailed)          (701))
    ((NoSuchSession)             (702))
    ((SessionAlreadyExists)      (703))
    ((ChunkAlreadyExists)        (704))
    ((WindowError)               (705))
    ((BlockContentMismatch)      (706))
    ((NoSuchBlock)               (707))
    ((NoSuchChunk)               (708))
    ((NoLocationAvailable)       (710))
    ((IOError)                   (711))
    ((MasterCommunicationFailed) (712))
    ((NoSuchChunkTree)           (713))
    ((NoSuchChunkList)           (717))
    ((MasterNotConnected)        (714))
    ((ChunkUnavailable)          (716))
    ((WriteThrottlingActive)     (718))
    ((NoSuchMedium)              (719))
    ((OptimisticLockFailure)     (720))
    ((InvalidBlockChecksum)      (721))
    ((BlockOutOfRange)           (722))
    ((ObjectNotReplicated)       (723))
    ((MissingExtension)          (724))
    ((BandwidthThrottlingFailed) (725))
);

using TChunkId = NObjectClient::TObjectId;
extern const TChunkId NullChunkId;

using TChunkListId = NObjectClient::TObjectId;
extern const TChunkListId NullChunkListId;

using TChunkTreeId = NObjectClient::TObjectId;
extern const TChunkTreeId NullChunkTreeId;

constexpr int MinReplicationFactor = 1;
constexpr int MaxReplicationFactor = 10;
constexpr int DefaultReplicationFactor = 3;

constexpr int MaxMediumCount = 7;

//! Used as an expected upper bound in SmallVector.
/*
 *  Maximum regular number of replicas is 16 (for LRC codec).
 *  Additional +8 enables some flexibility during balancing.
 */
constexpr int TypicalReplicaCount = 24;

// All chunks:
constexpr int GenericChunkReplicaIndex = 16;  // no specific replica; the default one for regular chunks

// Journal chunks only:
constexpr int ActiveChunkReplicaIndex   = 0; // the replica is currently being written
constexpr int UnsealedChunkReplicaIndex = 1; // the replica is finished but not sealed yet
constexpr int SealedChunkReplicaIndex   = 2; // the replica is finished and sealed

//! For pretty-printing only.
DEFINE_ENUM(EJournalReplicaType,
    ((Generic)   (GenericChunkReplicaIndex))
    ((Active)    (ActiveChunkReplicaIndex))
    ((Unsealed)  (UnsealedChunkReplicaIndex))
    ((Sealed)    (SealedChunkReplicaIndex))
);

//! Valid indexes are in range |[0, ChunkReplicaIndexBound)|.
constexpr int ChunkReplicaIndexBound = 32;

constexpr int AllMediaIndex = MaxMediumCount; // passed to various APIs to indicate that any medium is OK
constexpr int InvalidMediumIndex = -1;
constexpr int DefaultStoreMediumIndex = 0;
constexpr int DefaultCacheMediumIndex = 1;

//! Valid indexes (including sentinels) are in range |[0, MediumIndexBound)|.
constexpr int MediumIndexBound = AllMediaIndex + 1;

class TChunkReplica;
using TChunkReplicaList = SmallVector<TChunkReplica, TypicalReplicaCount>;

extern const TString DefaultStoreAccountName;
extern const TString DefaultStoreMediumName;
extern const TString DefaultCacheMediumName;

DECLARE_REFCOUNTED_CLASS(TFetchChunkSpecConfig)
DECLARE_REFCOUNTED_CLASS(TFetcherConfig)
DECLARE_REFCOUNTED_CLASS(TEncodingWriterConfig)
DECLARE_REFCOUNTED_CLASS(TErasureReaderConfig)
DECLARE_REFCOUNTED_CLASS(TMultiChunkReaderConfig)
DECLARE_REFCOUNTED_CLASS(TBlockFetcherConfig)
DECLARE_REFCOUNTED_CLASS(TReplicationReaderConfig)
DECLARE_REFCOUNTED_CLASS(TReplicationWriterConfig)
DECLARE_REFCOUNTED_CLASS(TErasureWriterConfig)
DECLARE_REFCOUNTED_CLASS(TMultiChunkWriterConfig)
DECLARE_REFCOUNTED_CLASS(TEncodingWriterOptions)

struct TCodecDuration;
class TCodecStatistics;

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
