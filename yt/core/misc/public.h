#pragma once

#include "common.h"
#include "enum.h"

namespace NYT {

///////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TError;

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

template <class, class> class TSingleton;
template <class> struct TStaticInstanceMixin;
template <class> struct TRefCountedInstanceMixin;
template <class> struct THeapInstanceMixin;

typedef ui64 TChecksum;

///////////////////////////////////////////////////////////////////////////////

DECLARE_ENUM(EErrorCode,
    ((OK)              (0))
    ((Generic)         (1))
    ((Timeout)         (104))
);

///////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define DECLARE_SINGLETON_MIXIN(type, mixin) private: \
    typedef ::NYT::TSingleton<type, mixin<type>> TSingleton; \
    friend struct mixin<type>; \
    template <class U, class... As> \
	friend ::NYT::TIntrusivePtr<U> NYT::New(As&&... args);
#define DECLARE_SINGLETON_DEFAULT_MIXIN(type) \
    DECLARE_SINGLETON_MIXIN(type, ::NYT::THeapInstanceMixin)
#define DECLARE_SINGLETON_PRIORITY(type, value) public: \
    static constexpr int Priority = value
#define DECLARE_SINGLETON_DELETE_AT_EXIT(type, flag) public: \
    static constexpr bool DeleteAtExit = flag
#define DECLARE_SINGLETON_RESET_AT_FORK(type, flag) public: \
    static constexpr bool ResetAtFork = flag

