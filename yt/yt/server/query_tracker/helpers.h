#pragma once

#include "config.h"

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/public.h>

#include <yt/yt/client/security_client/public.h>

#include <yt/yt/core/compression/codec.h>
#include <yt/yt/core/misc/common.h>

#include <yt/yt/core/ypath/public.h>

#include <yt/yt/core/ytree/permission.h>

namespace NYT::NQueryTracker {

////////////////////////////////////////////////////////////////////////////////

constexpr i64 MaxDyntableStringSize = 16_MB;

//! Path to access control object namespace for QT.
inline const NYPath::TYPath QueriesAcoNamespacePath = "//sys/access_control_object_namespaces/queries";

//! Name of the access control object that manages access to privileged (non-regular) query types.
inline const std::string AdminAccessControlObjectName = "admin";

////////////////////////////////////////////////////////////////////////////////

std::string BuildFilterFactors(const std::string& query, const NYson::TYsonString& annotations, const NYson::TYsonString& accessControlObjects);

////////////////////////////////////////////////////////////////////////////////

template <typename TPartial>
NApi::TQuery PartialRecordToQuery(const TPartial& partialRecord);

THashSet<std::string> GetUserSubjects(const std::string& user, const NApi::IClientPtr& client);
void ConvertAcoToOldFormat(NApi::TQuery& query);

NSecurityClient::ESecurityAction CheckAccessControl(
    const std::string& user,
    const std::optional<NYson::TYsonString>& accessControlObjects,
    const NApi::IClientPtr& client,
    NYTree::EPermission permission);

////////////////////////////////////////////////////////////////////////////////

std::string Compress(const std::string& data, std::optional<ui64> maxCompressedStringSize = std::nullopt, int quality = 9);
std::string Decompress(const std::string& data);

////////////////////////////////////////////////////////////////////////////////

TEngineConfigBasePtr GetConfigByEngine(const TQueryTrackerDynamicConfigPtr& config, EQueryEngine engine);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryTracker
