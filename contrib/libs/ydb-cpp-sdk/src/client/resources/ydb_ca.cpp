#include <library/cpp/resource/resource.h>

#include <ydb-cpp-sdk/client/resources/ydb_ca.h>

#include <src/version.h>

namespace NYdb::inline V3 {

std::string GetRootCertificate() {
    return NResource::Find(YDB_CERTIFICATE_FILE_KEY);
}

} // namespace NYdb
