#pragma once
#include "defs.h"
#include <library/cpp/actors/core/actor.h>
#include <library/cpp/actors/core/actorid.h>
#include <contrib/ydb/core/base/blobstorage.h>
#include <contrib/ydb/core/protos/msgbus.pb.h>
#include <contrib/ydb/core/protos/msgbus_kv.pb.h>

namespace NKikimr {

IActor* CreateKeyValueFlat(const TActorId &tablet, TTabletStorageInfo *info);

} //NKikimr
