#pragma once

#include "public.h"

namespace NYT::NOrm::NQuery {

////////////////////////////////////////////////////////////////////////////////

const ITypeResolver* GetTypeResolver(NTableClient::EValueType type);

const std::string& GetYsonExtractFunction(NTableClient::EValueType type);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NQuery
