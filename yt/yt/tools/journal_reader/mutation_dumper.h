#pragma once

#include <yt/yt/ytlib/hydra/proto/hydra_manager.pb.h>

#include <yt/yt/client/tablet_client/public.h>
#include <yt/yt/client/table_client/public.h>

#include <yt/yt/core/misc/ref_counted.h>

#include <library/cpp/yt/memory/ref.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IMutationDumper)

struct IMutationDumper
    : public NYT::TRefCounted
{
    virtual void Dump(const NHydra::NProto::TMutationHeader& header, TSharedRef requestData) = 0;
};

DEFINE_REFCOUNTED_TYPE(IMutationDumper)

////////////////////////////////////////////////////////////////////////////////

IMutationDumperPtr CreateWriteRowsDumper(
    NTabletClient::TTabletId tabletId,
    NTableClient::TTableSchemaPtr schema);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
