#pragma once

#include "public.h"

#include <library/cpp/yt/misc/property.h>

namespace NYT::NS3 {

////////////////////////////////////////////////////////////////////////////////

//! Identifies an S3 object by bucket and key.
//!
//! Only s3:// URIs are accepted. Leading slashes in the object key are removed
//! so that the value can be used directly in an S3 request.
class TObjectDescriptor
{
public:
    TObjectDescriptor(std::string bucket, std::string key, bool allowEmptyKey = false);

    static TObjectDescriptor FromUri(const std::string& uri, bool allowEmptyKey = false);

    DEFINE_BYREF_RO_PROPERTY(std::string, Bucket);
    DEFINE_BYREF_RO_PROPERTY(std::string, Key);
};

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TObjectDescriptor& object, TStringBuf spec);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NS3
