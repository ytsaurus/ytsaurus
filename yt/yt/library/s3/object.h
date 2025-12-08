#pragma once

#include "public.h"

#include <library/cpp/yt/farmhash/farm_hash.h>

#include <library/cpp/yt/misc/property.h>

namespace NYT::NS3 {

////////////////////////////////////////////////////////////////////////////////

//! Represents an S3 object identified by bucket and key.
//! Can be constructed from an S3 URI of the form "s3://bucket/key".
//! Throws on invalid URIs and on empty bucket or key.
//! Leading slashes in key are removed upon construction.
class TObjectDescriptor
{
public:
    //! Constructs descriptor from bucket and key, normalizing the key.
    //! Can throw if bucket or key is empty.
    TObjectDescriptor(TString bucket, TString key, bool allowEmptyKey = false);

    //! Parses descriptor from URI, e.g. "s3://my-bucket/path/to/object".
    //! Can throw if URI schema is incorrect or bucket or key is empty.
    static TObjectDescriptor FromUri(const std::string& uri, bool allowEmptyKey = false);

    //! Returns hash of bucket+key combination.
    TFingerprint GetHash() const;

public:
    // TODO(achulkov2): Switch to std::string.
    DEFINE_BYREF_RO_PROPERTY(TString, Bucket);
    DEFINE_BYREF_RO_PROPERTY(TString, Key);

private:
    TFingerprint Hash_;

    // Remove leading '/' from Key_ if any and throw if bucket or key is empty.
    void NormalizeOrThrow(bool allowEmptyKey = false);
};

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TObjectDescriptor& object, TStringBuf spec);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NS3
