#pragma once

#include <yt/ytlib/table_client/helpers.h>
#include <yt/ytlib/table_client/unversioned_row.h>

namespace NYT {
namespace NTableClient {
namespace {

////////////////////////////////////////////////////////////////////////////////

TUnversionedOwningRow BuildKey(const Stroka& yson)
{
    return NTableClient::YsonToKey(yson);
}

///////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NTableClient
} // namespace NYT
