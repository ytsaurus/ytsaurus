#pragma once

#include "public.h"
#include "seq_no_provider.h"
#include "spec.h"

#include <yt/yt/core/ytree/yson_struct.h>

#include <util/generic/map.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

struct TStreamSpecStorageState
    : public NYTree::TYsonStruct
{
    THashMap<TStreamId, TMap<TStreamSpecId, TStreamSpecPtr>> StreamSpecs;
    THashMap<TComputationId, NTableClient::TTableSchemaPtr> GroupBySchemas;

    TStreamSpecId NextStreamSpecId;

    REGISTER_YSON_STRUCT(TStreamSpecStorageState);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TStreamSpecStorageState);

////////////////////////////////////////////////////////////////////////////////

DEFINE_REFCOUNTED_TYPE(TVersionedStreamSpecStorageState);

////////////////////////////////////////////////////////////////////////////////

void UpdateStreamSpecStorageState(
    const TVersionedStreamSpecStorageStatePtr& versionedStorageState,
    const TPipelineSpec& spec,
    const IUniqueSeqNoProviderPtr& uniqueSeqNoProvider);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
