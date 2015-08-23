#pragma once

#include "common.h"
#include "enum.h"

namespace NYT {

///////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TError;
class TBloomFilter;
class TDataStatistics;

}  // namespace NProto

///////////////////////////////////////////////////////////////////////////////

template <class T>
class TErrorOr;

typedef TErrorOr<void> TError;

template <class T>
struct TErrorTraits;

DECLARE_REFCOUNTED_STRUCT(TLeaseEntry)
typedef TLeaseEntryPtr TLease;

class TStreamSaveContext;
class TStreamLoadContext;

template <class TSaveContext, class TLoadContext>
class TCustomPersistenceContext;

using TStreamPersistenceContext = TCustomPersistenceContext<
    TStreamSaveContext,
    TStreamLoadContext>;

struct TValueBoundComparer;
struct TValueBoundSerializer;

template <class T, class C, class = void>
struct TSerializerTraits;

class TChunkedMemoryPool;

template <class TKey, class TComparer>
class TSkipList;

class TBlobOutput;

class TStringBuilder;

struct ICheckpointableInputStream;
struct ICheckpointableOutputStream;

DECLARE_REFCOUNTED_CLASS(TSlruCacheConfig)
DECLARE_REFCOUNTED_CLASS(TExpiringCacheConfig)

class TBloomFilterBuilder;
class TBloomFilter;

using TChecksum = ui64;
using TFingerprint = ui64;

template <class T, unsigned size>
class SmallVector;

template <class TProto>
class TRefCountedProto;

const i64 DefaultEnvelopePartSize = 1LL << 28;

///////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EErrorCode,
    ((OK)                 (0))
    ((Generic)            (1))
    ((Canceled)           (2))
    ((Timeout)            (3))
);

///////////////////////////////////////////////////////////////////////////////

} // namespace NYT
