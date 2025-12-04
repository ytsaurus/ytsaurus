#pragma once

#include <yt/yt/client/object_client/public.h>

#include <yt/yt/core/misc/public.h>

#include <library/cpp/yt/containers/non_empty.h>

#include <library/cpp/yt/compact_containers/compact_flat_map.h>
#include <library/cpp/yt/compact_containers/compact_vector.h>
#include <library/cpp/yt/compact_containers/compact_map.h>

#include <span>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TChunkInfo;
class TChunkSpec;
class TChunkMeta;
class TBlocksExt;
class TMiscExt;

class TConfirmChunkReplicaInfo;

class TDataStatistics;

class TLegacyReadRange;

class TMediumDirectory;

} // namespace NProto

YT_DEFINE_ERROR_ENUM(
    ((AllTargetNodesFailed)                  (700))
    ((SendBlocksFailed)                      (701))
    ((NoSuchSession)                         (702))
    ((SessionAlreadyExists)                  (703))
    ((ChunkAlreadyExists)                    (704))
    ((WindowError)                           (705))
    ((BlockContentMismatch)                  (706))
    ((NoSuchBlock)                           (707))
    ((NoSuchChunk)                           (708))
    ((NoLocationAvailable)                   (710))
    ((IOError)                               (711))
    ((MasterCommunicationFailed)             (712))
    ((NoSuchChunkTree)                       (713))
    ((NoSuchChunkList)                       (717))
    ((MasterNotConnected)                    (714))
    ((ChunkUnavailable)                      (716))
    ((WriteThrottlingActive)                 (718))
    ((NoSuchMedium)                          (719))
    ((OptimisticLockFailure)                 (720))
    ((InvalidBlockChecksum)                  (721))
    ((MalformedReadRequest)                  (722))
    ((MissingExtension)                      (724))
    ((ReaderThrottlingFailed)                (725))
    ((ReaderTimeout)                         (726))
    ((NoSuchChunkView)                       (727))
    ((IncorrectChunkFileChecksum)            (728))
    ((BrokenChunkFileMeta)                   (729))
    ((IncorrectLayerFileSize)                (730))
    ((NoSpaceLeftOnDevice)                   (731))
    ((ConcurrentChunkUpdate)                 (732))
    ((InvalidInputChunk)                     (733))
    ((UnsupportedChunkFeature)               (734))
    ((IncompatibleChunkMetas)                (735))
    ((AutoRepairFailed)                      (736))
    ((ChunkBlockFetchFailed)                 (737))
    ((ChunkMetaFetchFailed)                  (738))
    ((RowsLookupFailed)                      (739))
    ((BlockChecksumMismatch)                 (740))
    ((NoChunkSeedsKnown)                     (741))
    ((NoChunkSeedsGiven)                     (742))
    ((ChunkIsLost)                           (743))
    ((ChunkReadSessionSlow)                  (744))
    ((NodeProbeFailed)                       (745))
    ((UnrecoverableRepairError)              (747))
    ((MissingJournalChunkRecord)             (748))
    ((LocationDiskFailed)                    (749))
    ((LocationCrashed)                       (750))
    ((LocationDiskWaitingReplacement)        (751))
    ((ChunkMetaCacheFetchFailed)             (752))
    ((LocationMediumIsMisconfigured)         (753))
    ((LocationDisabled)                      (755))
    ((DiskFailed)                            (756))
    ((DiskWaitingReplacement)                (757))
    ((LockFileIsFound)                       (758))
    ((DiskHealthCheckFailed)                 (759))
    ((TooManyChunksToFetch)                  (760))
    ((TotalMemoryLimitExceeded)              (761))
    ((ForbiddenErasureCodec)                 (762))
    ((ReadMetaTimeout)                       (763))
);

DEFINE_ENUM_WITH_UNDERLYING_TYPE(EUpdateMode, i8,
    ((None)                     (0))
    ((Append)                   (1))
    ((Overwrite)                (2))
);

using TChunkId = NObjectClient::TObjectId;
extern const TChunkId NullChunkId;

using TChunkViewId = NObjectClient::TObjectId;
extern const TChunkViewId NullChunkViewId;

using TChunkListId = NObjectClient::TObjectId;
extern const TChunkListId NullChunkListId;

using TChunkTreeId = NObjectClient::TObjectId;
extern const TChunkTreeId NullChunkTreeId;

using TChunkLocationUuid = TGuid;
constexpr auto EmptyChunkLocationUuid = TChunkLocationUuid(0, 0);
constexpr auto InvalidChunkLocationUuid = TChunkLocationUuid(-1, -1);

constexpr int MinReplicationFactor = 1;
constexpr int MaxReplicationFactor = 20;
constexpr int DefaultReplicationFactor = 3;
constexpr int DefaultIntermediateDataReplicationFactor = 2;

//! Medium index layout. Numbers denote indexes, |X| marks sentinel slots.
/*
 *   Non-sentinel media slots (MaxMediumCount allowed entries)    Sentinel-only slots
 *                        ∨ ∨ ∨                                         ∨ ∨ ∨
 *                              GenericMediumIndex=126                                   MediumIndexBound
 *   0                          ∨                                                        ∨
 *   |--------------------------XX---------------------------|---------------------------|
 *                               ^                           ^
 *                               AllMediaIndex=127           RealMediumIndexBound
 *                              ^^
 *                              Legacy sentinels embedded into the real range for compatibility
 *
 * "Real" media correspond to actual master medium objects. Sentinels are special indexes
 * that do not have associated medium objects and are used for special purposes across APIs.
 * Because GenericMediumIndex/AllMediaIndex sit inside the real range, |RealMediumIndexBound|
 * exceeds the plain count limit by 2, to keep room for |MaxMediumCount| real media.
 * One must also avoid using comparison for checking the real-ness of a medium and must
 * use |IsValidRealMediumIndex| instead.
 */
//! Maximum allowed number of real (non-sentinel) media objects on a cluster.
//! NB: The bound on medium *indexes* for associated master media objects is *not* equal to this value.
//! See the diagram above for clarification.
constexpr int MaxMediumCount = 64000;
//! The typical number of media on a cluster.
//! Used for initializing compact containers.
constexpr int TypicalMediumCount = 4;
//! Upper bound on medium indexes associated with real (non-sentinel) media.
//! NB: This value is intentionally |MaxMediumCount + 2| to account for the two in-range sentinels.
constexpr int RealMediumIndexBound = MaxMediumCount + 2;
//! All valid medium indexes (including sentinels) are in range |[0, MediumIndexBound)|.
//! The tail |[RealMediumIndexBound, MediumIndexBound)| is reserved for future sentinels.
constexpr int MediumIndexBound = MaxMediumCount + 100;

//! Returns a list of all sentinel medium indexes: |GenericMediumIndex|, |AllMediaIndex| and
//! every reserved slot in |[RealMediumIndexBound, MediumIndexBound)|.
constexpr std::span<const int> GetSentinelMediumIndexes();
//! Returns true if |mediumIndex| is a valid medium index for a master medium object,
//! i.e. it falls in range |[0, RealMediumIndexBound)| and is not equal to any of the
//! reserved sentinel values (see |IsSentinelMediumIndex|).
bool IsValidRealMediumIndex(int mediumIndex);

template <typename T>
using TMediumMap = THashMap<int, T>;

template <typename T>
using TCompactMediumMap = TCompactMap<int, T, TypicalMediumCount>;

//! Used as an expected upper bound in TCompactVector.
/*
 *  Maximum regular number of replicas is 16 (for LRC codec).
 *  Additional +8 enables some flexibility during balancing.
 */
constexpr int TypicalReplicaCount = 24;
constexpr int SlimTypicalReplicaCount = 3;
constexpr int GenericChunkReplicaIndex = 16;  // no specific replica; the default one for non-erasure chunks

//! Valid indexes are in range |[0, ChunkReplicaIndexBound)|.
constexpr int ChunkReplicaIndexBound = 32;

constexpr int GenericMediumIndex      = 126; // internal sentinel meaning "no specific medium"
constexpr int AllMediaIndex           = 127; // passed to various APIs to indicate that any medium is OK
constexpr int DefaultStoreMediumIndex =   0;
constexpr int DefaultSlotsMediumIndex =   0;

class TChunkReplicaWithMedium;
using TChunkReplicaWithMediumList = TCompactVector<TChunkReplicaWithMedium, TypicalReplicaCount>;
using TChunkReplicaWithMediumSlimList = TCompactVector<TChunkReplicaWithMedium, SlimTypicalReplicaCount>;

class TChunkReplicaWithLocation;
using TChunkReplicaWithLocationList = TCompactVector<TChunkReplicaWithLocation, TypicalReplicaCount>;

struct TWrittenChunkReplicasInfo;

class TChunkReplica;
using TChunkReplicaList = TCompactVector<TChunkReplica, TypicalReplicaCount>;
using TChunkReplicaSlimList = TCompactVector<TChunkReplica, SlimTypicalReplicaCount>;

using TPartitionTags = TNonEmpty<TCompactVector<int, 1>>;

extern const std::string DefaultStoreAccountName;
extern const std::string DefaultStoreMediumName;
extern const std::string DefaultCacheMediumName;
extern const std::string DefaultSlotsMediumName;

DECLARE_REFCOUNTED_STRUCT(IReaderBase)

DECLARE_REFCOUNTED_STRUCT(TFetchChunkSpecConfig)
DECLARE_REFCOUNTED_STRUCT(TFetcherConfig)
DECLARE_REFCOUNTED_STRUCT(TChunkSliceFetcherConfig)
DECLARE_REFCOUNTED_STRUCT(TEncodingWriterConfig)
DECLARE_REFCOUNTED_STRUCT(TErasureReaderConfig)
DECLARE_REFCOUNTED_STRUCT(TMultiChunkReaderConfig)
DECLARE_REFCOUNTED_STRUCT(TBlockFetcherConfig)
DECLARE_REFCOUNTED_STRUCT(TReplicationReaderConfig)
DECLARE_REFCOUNTED_STRUCT(TReplicationWriterConfig)
DECLARE_REFCOUNTED_STRUCT(TErasureWriterConfig)
DECLARE_REFCOUNTED_STRUCT(TMultiChunkWriterConfig)
DECLARE_REFCOUNTED_STRUCT(TEncodingWriterOptions)
DECLARE_REFCOUNTED_STRUCT(TBlockReordererConfig)
DECLARE_REFCOUNTED_STRUCT(TChunkFragmentReaderConfig)

struct TCodecDuration;
class TCodecStatistics;

class TLegacyReadLimit;
class TLegacyReadRange;
class TReadLimit;
class TReadRange;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EChunkAvailabilityPolicy,
    ((DataPartsAvailable)           (0))
    ((AllPartsAvailable)            (1))
    ((Repairable)                   (2))
);

// Keep in sync with SerializeChunkFormatAsTableChunkFormat.
DEFINE_ENUM_WITH_UNDERLYING_TYPE(EChunkFormat, i8,
    // Sentinels.
    ((Unknown)                             (-1))

    // File chunks.
    ((FileDefault)                          (1))

    // Table chunks.
    ((TableUnversionedSchemaful)            (3))
    ((TableUnversionedSchemalessHorizontal) (4))
    ((TableUnversionedColumnar)             (6))
    ((TableVersionedSimple)                 (2))
    ((TableVersionedColumnar)               (5))
    ((TableVersionedIndexed)                (8))
    ((TableVersionedSlim)                   (9))

    // Journal chunks.
    ((JournalDefault)                       (0))

    // Hunk chunks.
    ((HunkDefault)                          (7))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient

#define PUBLIC_INL_H_
#include "public-inl.h"
#undef PUBLIC_INL_H_
