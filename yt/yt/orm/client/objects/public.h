#pragma once

#include <yt/yt/orm/client/misc/public.h>

#include <yt/yt/client/transaction_client/public.h>

#include <yt/yt/core/logging/log.h>
#include <yt/yt/core/misc/public.h>
#include <yt/yt/core/ypath/public.h>

#include <library/cpp/containers/bitset/bitset.h>

namespace NYT::NOrm::NClient::NObjects {

////////////////////////////////////////////////////////////////////////////////

class TObjectKey;
using TObjectKeys = std::vector<TObjectKey>;

using TObjectId = TString;

using NYT::NTransactionClient::TTimestamp;
using NYT::NTransactionClient::NullTimestamp;

using TObjectTypeName = TString;
using TObjectTypeValue = int;

using TTransactionId = NYT::NTransactionClient::TTransactionId;
using NYT::NTransactionClient::NullTransactionId;

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_STRONG_TYPEDEF(TMasterInstanceTag, ui8);
YT_DEFINE_STRONG_TYPEDEF(TClusterTag, ui8);

inline constexpr TMasterInstanceTag UndefinedMasterInstanceTag = TMasterInstanceTag{0};
inline constexpr TMasterInstanceTag LowerMasterInstanceTag = TMasterInstanceTag{1};
inline constexpr TMasterInstanceTag UpperMasterInstanceTag = std::numeric_limits<TMasterInstanceTag>::max();

////////////////////////////////////////////////////////////////////////////////

struct TObjectTypeValues
{
    static constexpr TObjectTypeValue Null = -1;
    static constexpr TObjectTypeValue User = 9;
    static constexpr TObjectTypeValue Group = 10;
    static constexpr TObjectTypeValue Schema = 256;
    static constexpr TObjectTypeValue WatchLogConsumer = 257;
    static constexpr TObjectTypeValue Semaphore = 258;
    static constexpr TObjectTypeValue SemaphoreSet = 259;
};

struct TObjectTypeNames
{
    static const TObjectTypeName Null;
    static const TObjectTypeName User;
    static const TObjectTypeName Group;
    static const TObjectTypeName Schema;
    static const TObjectTypeName Semaphore;
    static const TObjectTypeName SemaphoreSet;
};

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EWatchLogState,
    (QueryStore)
    (Store)
    (None)
);

inline constexpr TStringBuf DefaultWatchLogName{"watch_log"};

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EHashType,
    (ArcadiaUtil)
    (Farm)
);

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IObjectTypeRegistry)
using IConstObjectTypeRegistryPtr = TIntrusivePtr<const IObjectTypeRegistry>;

struct TObjectFilter
{
    TString Query;

    bool operator==(const TObjectFilter& rhs) const = default;
};

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IAccessControlRegistry)
using IConstAccessControlRegistryPtr = TIntrusivePtr<const IAccessControlRegistry>;

////////////////////////////////////////////////////////////////////////////////

struct TTransactionContext;

////////////////////////////////////////////////////////////////////////////////

TObjectId GenerateUuid();

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ESelectObjectHistoryIndexMode,
    ((Default)  (0))
    ((Enabled)  (1))
    ((Disabled) (2))
);

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(ITagsRegistry)
using IConstTagsRegistryPtr = TIntrusivePtr<const ITagsRegistry>;

YT_DEFINE_STRONG_TYPEDEF(TTag, int)
using TTagSet = TBitSet<TTag>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NClient::NObjects
