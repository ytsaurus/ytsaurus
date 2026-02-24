#include "row_level_security.h"

namespace NYT::NQueryClient {

Y_WEAK std::pair<ICGInstanceHolderPtr, NTableClient::TTableSchemaPtr> MakeRlsCGInstance(const NTableClient::TTableSchemaPtr& /*schema*/, const std::string& /*predicate*/) {
    YT_ABORT();
}

}
