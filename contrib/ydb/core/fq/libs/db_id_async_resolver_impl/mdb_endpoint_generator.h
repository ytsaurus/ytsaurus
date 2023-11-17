#pragma once

#include <contrib/ydb/library/yql/providers/common/db_id_async_resolver/db_async_resolver.h>
#include <contrib/ydb/library/yql/providers/common/db_id_async_resolver/mdb_endpoint_generator.h>

namespace NFq {
    NYql::IMdbEndpointGenerator::TPtr MakeMdbEndpointGeneratorLegacy();
    NYql::IMdbEndpointGenerator::TPtr MakeMdbEndpointGeneratorGeneric(bool transformHost, bool useNativeProtocolForClickHouse = false);
    NYql::IMdbEndpointGenerator::TPtr MakeMdbEndpointGeneratorNoop();
}
