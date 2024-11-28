#pragma once

#include <yt/yt/library/query/base/ast.h>
#include <yt/yt/library/query/base/query_preparer.h>

#include <yt/yt/client/tablet_client/public.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

void TransformWithIndexStatement(
    NAst::TAstHead* head,
    TMutableRange<NTabletClient::TTableMountInfoPtr> mountInfos);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
