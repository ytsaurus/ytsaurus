#pragma once

#include <yt/yt/orm/client/misc/public.h>

#include <yt/yt/client/transaction_client/public.h>

#include <yt/yt/core/logging/log.h>
#include <yt/yt/core/misc/public.h>
#include <yt/yt/core/ypath/public.h>

#include <library/cpp/containers/bitset/bitset.h>

namespace NYT::NOrm::NClient::NProto {

////////////////////////////////////////////////////////////////////////////////

class TPayload;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NClient::NProto

////////////////////////////////////////////////////////////////////////////////

namespace NYT::NOrm::NClient::NObjects {

////////////////////////////////////////////////////////////////////////////////

inline constexpr char FqidSeparator = '|';
inline constexpr char CompositeKeySeparator = ';';

class TObjectKey;
using TObjectKeys = std::vector<TObjectKey>;

using TObjectId = TString;

using NYT::NTransactionClient::TTimestamp;
using NYT::NTransactionClient::NullTimestamp;

using TObjectTypeName = std::string;
using TObjectTypeValue = int;

using TPermissionName = std::string;
using TPermissionValue = int;

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
    static const TObjectTypeName WatchLogConsumer;
    static const TObjectTypeName Semaphore;
    static const TObjectTypeName SemaphoreSet;
};

////////////////////////////////////////////////////////////////////////////////

struct TPermissionValues
{
    static constexpr TPermissionValue None = 0;
    static constexpr TPermissionValue Read = 1;
    static constexpr TPermissionValue Write = 2;
    static constexpr TPermissionValue Create = 3;
    static constexpr TPermissionValue Use = 6;
    static constexpr TPermissionValue Administer = 9;
};

struct TPermissionNames
{
    static const TPermissionName None;
    static const TPermissionName Read;
    static const TPermissionName Write;
    static const TPermissionName Create;
    static const TPermissionName Use;
    static const TPermissionName Administer;
};

////////////////////////////////////////////////////////////////////////////////

// NB! Unlike for access control permissions, plain enum is used here, since
// there are no plans to extend EAccessControlAction in specific ORMs.
DEFINE_ENUM(EAccessControlAction,
    ((None)  (0))
    ((Allow) (1))
    ((Deny)  (2))
);

// Builtin users.
inline const TObjectId NullObjectId = "";

// Builtin groups.
inline const TObjectId SuperusersGroupId = "superusers";

// Pseudo-subjects.
inline const TObjectId EveryoneSubjectId = "everyone";
extern const TObjectKey EveryoneSubjectKey;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EWatchLogState,
    (QueryStore)
    (Store)
    (None)
);

inline constexpr TStringBuf DefaultWatchLogName = "watch_log";

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EAttributeMigrationPhase,
    (Migrate)
    (Cleanup)
);

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
    std::string Query;

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

YT_DEFINE_STRONG_TYPEDEF(TTag, int);
using TTagSet = TBitSet<TTag>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NClient::NObjects
