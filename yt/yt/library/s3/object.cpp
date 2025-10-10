#include "object.h"

#include <yt/yt/core/misc/error.h>

#include <contrib/libs/poco/Foundation/include/Poco/URI.h>

namespace NYT::NS3 {

////////////////////////////////////////////////////////////////////////////////

TObjectDescriptor::TObjectDescriptor(TString bucket, TString key, bool allowEmptyKey)
    : Bucket_(std::move(bucket))
    , Key_(std::move(key))
{
    NormalizeOrThrow(allowEmptyKey);
}

TObjectDescriptor TObjectDescriptor::FromUri(const std::string& uri, bool allowEmptyKey)
{
    Poco::URI parsedUri(uri);

    if (parsedUri.getScheme() != "s3") {
        THROW_ERROR_EXCEPTION("Failed to parse S3 URI %Qv: unexpected scheme %Qv",
            uri,
            parsedUri.getScheme());
    }

    try {
        return TObjectDescriptor(TString(parsedUri.getHost()), TString(parsedUri.getPath()), allowEmptyKey);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Failed to parse S3 URI %Qv", uri)
            << ex;
    }
}

void TObjectDescriptor::NormalizeOrThrow(bool allowEmptyKey)
{
    Key_.erase(0, Key_.find_first_not_of('/'));

    if (Bucket_.empty()) {
        THROW_ERROR_EXCEPTION("S3 object bucket should not be empty");
    }

    if (Key_.empty() && !allowEmptyKey) {
        THROW_ERROR_EXCEPTION("S3 object key should not be empty");
    }
}

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TObjectDescriptor& object, TStringBuf spec)
{
    return FormatValue(builder, Format("s3://%v/%v", object.Bucket(), object.Key()), spec);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NS3
