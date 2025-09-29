#pragma once

#include <contrib/ydb/library/yql/providers/dq/provider/yql_dq_gateway.h>

#include <util/generic/ptr.h>
#include <util/thread/pool.h>

namespace NYT::NYqlPlugin::NNative {

////////////////////////////////////////////////////////////////////////////////

::TIntrusivePtr<NYql::IDqGateway> CreateDqGatewayWithOffloading(
    ::TIntrusivePtr<NYql::IDqGateway> underlying,
    IThreadPool* threadPool);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYqlPlugin::NNative
