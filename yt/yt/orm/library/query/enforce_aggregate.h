#pragma once

#include <yt/yt/library/query/base/ast.h>

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

NQueryClient::NAst::TExpressionPtr EnforceAggregate(
    TObjectsHolder* objectsHolder,
    NQueryClient::NAst::TExpressionPtr expr);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
