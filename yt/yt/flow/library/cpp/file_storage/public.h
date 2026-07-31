#pragma once

#include <yt/yt/core/misc/error.h>

#include <library/cpp/yt/memory/ref_counted.h>
#include <library/cpp/yt/misc/strong_typedef.h>

#include <string>

namespace NYT::NFlow::NFileStorage {

////////////////////////////////////////////////////////////////////////////////

// Identifies an immutable materialized filesystem tree.
// Equal ids must produce byte-identical payloads.
YT_DEFINE_STRONG_TYPEDEF(TFileStorageObjectId, std::string);

DECLARE_REFCOUNTED_STRUCT(TFileStorageConfig);
DECLARE_REFCOUNTED_STRUCT(IFileStorageObject);
DECLARE_REFCOUNTED_STRUCT(IFileStorage);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NFileStorage
