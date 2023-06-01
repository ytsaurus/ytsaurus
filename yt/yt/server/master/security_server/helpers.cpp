#include "helpers.h"
#include "private.h"

#include <yt/yt/server/master/chunk_server/chunk_manager.h>
#include <yt/yt/server/master/chunk_server/medium_base.h>

#include <yt/yt/server/lib/security_server/proto/security_manager.pb.h>

#include <yt/yt/core/logging/fluent_log.h>
#include <yt/yt/core/misc/error.h>

#include <library/cpp/yt/misc/cast.h>

namespace NYT::NSecurityServer {

using namespace NChunkServer;
using namespace NLogging;
using namespace NYPath;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = SecurityServerLogger;

////////////////////////////////////////////////////////////////////////////////

void ValidateDiskSpace(i64 diskSpace)
{
    if (diskSpace < 0) {
        THROW_ERROR_EXCEPTION("Invalid disk space size: expected >= 0, found %v",
            diskSpace);
    }
}

////////////////////////////////////////////////////////////////////////////////

i64 GetOptionalNonNegativeI64ChildOrThrow(const NYTree::IMapNodePtr mapNode, const char* key)
{
    auto fieldNode = mapNode->FindChild(key);
    if (!fieldNode) {
        return 0;
    }

    auto result = fieldNode->AsInt64()->GetValue();
    if (result < 0) {
        THROW_ERROR_EXCEPTION("%Qv cannot be negative, found %v", key, result);
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

void LogAcdUpdate(const TString& attribute, const TYPath& path, const TYsonString& value)
{
    LogStructuredEventFluently(SecurityServerLogger, ELogLevel::Info)
        .Item("event").Value(EAccessControlEvent::ObjectAcdUpdated)
        .Item("attribute").Value(attribute)
        .Item("path").Value(path)
        .Item("value").Value(value);
}

////////////////////////////////////////////////////////////////////////////////

TAccessControlList DeserializeAcl(
    const TYsonString& serializedAcl,
    const ISecurityManagerPtr& securityManager)
{
    TAccessControlList acl;
    std::vector<TString> missingSubjects;
    Deserialize(
        acl,
        ConvertToNode(serializedAcl),
        securityManager,
        &missingSubjects);

    if (!missingSubjects.empty()) {
        YT_LOG_ALERT("Some subjects mentioned in ACL are missing (MissingSubjects: %v)",
            missingSubjects);
    }

    return acl;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer
