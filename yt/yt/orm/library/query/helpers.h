#pragma once

#include "public.h"

#include <yt/yt/library/query/base/ast.h>

#include <yt/yt/library/query/misc/objects_holder.h>

namespace NYT::NOrm::NQuery {

////////////////////////////////////////////////////////////////////////////////

const ITypeResolver* GetTypeResolver(NTableClient::EValueType type);

const std::string& GetYsonExtractFunction(NTableClient::EValueType type);

////////////////////////////////////////////////////////////////////////////////

struct TExpressionParsedSource
    : public TObjectsHolder
{
    explicit TExpressionParsedSource(std::string source);

    std::string Source;
    NQueryClient::NAst::TExpressionPtr Expression;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NQuery
