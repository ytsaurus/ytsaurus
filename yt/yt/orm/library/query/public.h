#pragma once

#include <yt/yt/client/table_client/public.h>

namespace NYT::NOrm::NQuery {

////////////////////////////////////////////////////////////////////////////////

struct ITypeResolver
{
    virtual ~ITypeResolver() = default;

    virtual NTableClient::EValueType ResolveType(NYPath::TYPathBuf suffixPath = {}) const = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NQuery
