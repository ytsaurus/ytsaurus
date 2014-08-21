#pragma once

#include "common.h"

namespace NYT {

///////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TLeaseEntry)
typedef TLeaseEntryPtr TLease;

class TStreamSaveContext;
class TStreamLoadContext;

template <class TSaveContext, class TLoadContext>
class TCustomPersistenceContext;

struct TValueBoundComparer;
struct TValueBoundSerializer;

template <class T, class C, class = void>
struct TSerializerTraits;

class TChunkedMemoryPool;

template <class TKey, class TComparer>
class TSkipList;

class TBlobOutput;
class TFakeStringBufStore;

class TStringBuilder;

struct ICheckpointableInputStream;
struct ICheckpointableOutputStream;

DECLARE_REFCOUNTED_CLASS(TSlruCacheConfig)

///////////////////////////////////////////////////////////////////////////////

} // namespace NYT
