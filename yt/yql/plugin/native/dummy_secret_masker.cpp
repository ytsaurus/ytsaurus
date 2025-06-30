#include "secret_masker.h"

#include <yt/yql/providers/yt/lib/secret_masker/dummy/dummy_secret_masker.h>

namespace NYT::NYqlPlugin {

////////////////////////////////////////////////////////////////////////////////

NYql::ISecretMasker::TPtr CreateSecretMasker() {
    return NYql::CreateDummySecretMasker();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYqlPlugin
