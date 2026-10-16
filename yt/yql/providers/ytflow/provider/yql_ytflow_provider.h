#pragma once

#include "yql_ytflow_gateway.h"
#include "yql_ytflow_state.h"

#include <yql/essentials/core/yql_data_provider.h>

#include <util/generic/hash_set.h>
#include <util/generic/ptr.h>
#include <util/generic/strbuf.h>
#include <util/generic/string.h>


namespace NYql {

TDataProviderInitializer GetYtflowDataProviderInitializer(IYtflowGateway::TPtr gateway);

TIntrusivePtr<IDataProvider> CreateYtflowDataSource(TYtflowState::TPtr state);
TIntrusivePtr<IDataProvider> CreateYtflowDataSink(TYtflowState::TPtr state);

const THashSet<TStringBuf>& YtflowDataSourceFunctions();
const THashSet<TStringBuf>& YtflowDataSinkFunctions();

} // namespace NYql
