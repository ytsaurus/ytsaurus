#pragma once

#include "common.h"

// Google Protobuf forward declarations.
namespace google::protobuf {

////////////////////////////////////////////////////////////////////////////////

class Descriptor;
class EnumDescriptor;
class MessageLite;
class Message;

template <class Element>
class RepeatedField;
template <class Element>
class RepeatedPtrField;

class Timestamp;

namespace io {

class ZeroCopyInputStream;
class ZeroCopyOutputStream;

} // namespace io

////////////////////////////////////////////////////////////////////////////////

} // namespace google::protobuf

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TError;
class TBloomFilter;
class TDataStatistics;

} // namespace NProto

struct TGuid;

template <class T>
class TErrorOr;

typedef TErrorOr<void> TError;

template <class T>
struct TErrorTraits;

class TStreamSaveContext;
class TStreamLoadContext;

struct TEntitySerializationContext;
class TEntityStreamSaveContext;
class TEntityStreamLoadContext;

template <class TSaveContext, class TLoadContext, class TSnapshotVersion = int>
class TCustomPersistenceContext;

using TStreamPersistenceContext = TCustomPersistenceContext<
    TStreamSaveContext,
    TStreamLoadContext
>;

struct TValueBoundComparer;
struct TValueBoundSerializer;

template <class T, class C, class = void>
struct TSerializerTraits;

class TChunkedMemoryPool;

template <class TKey, class TComparer>
class TSkipList;

class TBlobOutput;

class TStringBuilderBase;
class TStringBuilder;

struct ICheckpointableInputStream;
struct ICheckpointableOutputStream;

DECLARE_REFCOUNTED_CLASS(TSlruCacheConfig)
DECLARE_REFCOUNTED_CLASS(TAsyncExpiringCacheConfig)

DECLARE_REFCOUNTED_CLASS(TLogDigestConfig)
DECLARE_REFCOUNTED_STRUCT(IDigest)

DECLARE_REFCOUNTED_CLASS(THistoricUsageConfig)

class TSignalRegistry;

class TBloomFilterBuilder;
class TBloomFilter;

using TChecksum = ui64;
using TFingerprint = ui64;

constexpr TChecksum NullChecksum = 0;

template <class T, unsigned Size>
class SmallVector;

class TRef;
class TMutableRef;

namespace NDetail {

template <typename KeyT, typename ValueT>
struct DenseMapPair;

} // namespace NDetail

template <typename T>
struct TDenseMapInfo;

template <
	typename KeyT,
	typename ValueT,
	unsigned InlineBuckets = 4,
	typename KeyInfoT = TDenseMapInfo<KeyT>,
	typename BucketT = NDetail::DenseMapPair<KeyT, ValueT>
>
class SmallDenseMap;

template <class TProto>
class TRefCountedProto;

DECLARE_REFCOUNTED_CLASS(TProcessBase)

const ui32 YTCoreNoteType = 0x5f59545f; // = hex("_YT_") ;)
extern const TString YTCoreNoteName;

DECLARE_REFCOUNTED_STRUCT(ICoreDumper)

template <class T>
class TInternRegistry;

template <class T>
using TInternRegistryPtr = TIntrusivePtr<TInternRegistry<T>>;

template <class T>
class TInternedObjectData;

template <class T>
using TInternedObjectDataPtr = TIntrusivePtr<TInternedObjectData<T>>;

template <class T>
class TInternedObject;

DECLARE_REFCOUNTED_STRUCT(IMemoryChunkProvider)
DECLARE_REFCOUNTED_STRUCT(IMemoryUsageTracker)

class TStatistics;
class TSummary;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EErrorCode,
    ((OK)                    (0))
    ((Generic)               (1))
    ((Canceled)              (2))
    ((Timeout)               (3))
    ((FutureCombinerFailure) (4))
    ((FutureCombinerShortcut)(5))
);

DEFINE_ENUM(EProcessErrorCode,
    ((NonZeroExitCode)    (10000))
    ((Signal)             (10001))
    ((CannotResolveBinary)(10002))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
