#pragma once

#include <contrib/ydb/core/fq/libs/ydb/table_client.h>

namespace NFq {

IYdbTableClient::TPtr CreateLocalTableClient();

} // namespace NFq
