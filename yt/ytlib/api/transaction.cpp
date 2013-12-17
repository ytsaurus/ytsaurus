#include "stdafx.h"
#include "transaction.h"

namespace NYT {
namespace NApi {

////////////////////////////////////////////////////////////////////////////////

TColumnFilter::TColumnFilter()
    : All(true)
{ }

TColumnFilter::TColumnFilter(const std::vector<Stroka>& columns)
    : All(false)
    , Columns(columns.begin(), columns.end())
{ }

TColumnFilter::TColumnFilter(const TColumnFilter& other)
    : All(other.All)
    , Columns(other.Columns)
{ }

////////////////////////////////////////////////////////////////////////////////

TLookupOptions::TLookupOptions()
    : Timestamp(NTransactionClient::LastCommittedTimestamp)
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT
