#pragma once
#include "defs.h"
#include <contrib/ydb/library/actors/core/actor.h>
#include <contrib/ydb/library/actors/core/actorid.h>
#include <contrib/ydb/core/base/blobstorage.h>
#include <contrib/ydb/core/protos/msgbus.pb.h>
#include <contrib/ydb/core/protos/msgbus_kv.pb.h>

namespace NKikimr {

IActor* CreateKeyValueFlat(const TActorId &tablet, TTabletStorageInfo *info);

} //NKikimr
