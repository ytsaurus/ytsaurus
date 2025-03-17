#pragma once

#include <contrib/ydb/core/scheme/scheme_tabledefs.h>

namespace NKikimr::NChangeExchange {

TVector<ui64> MakePartitionIds(const TVector<TKeyDesc::TPartitionInfo>& partitions);

}
