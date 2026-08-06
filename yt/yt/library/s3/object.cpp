#include "object.h"

#include <yt/yt/core/misc/error.h>

#include <contrib/libs/poco/Foundation/include/Poco/URI.h>

namespace NYT::NS3 {

////////////////////////////////////////////////////////////////////////////////

TObjectDescriptor::TObjectDescriptor(std::string bucket, std::string key, bool allowEmptyKey)
    : Bucket_(std::move(bucket))
    , Key_(std::move(key))
{
    Key_.erase(0, Key_.find_first_not_of('/'));

    THROW_ERROR_EXCEPTION_IF(Bucket_.empty(), "S3 object bucket should not be empty");
    THROW_ERROR_EXCEPTION_IF(Key_.empty() && !allowEmptyKey, "S3 object key should not be empty");
}

TObjectDescriptor TObjectDescriptor::FromUri(const std::string& uri, bool allowEmptyKey)
{
    Poco::URI parsedUri(uri);
    THROW_ERROR_EXCEPTION_IF(
        parsedUri.getScheme() != "s3",
        "Failed to parse S3 URI %Qv: unexpected scheme %Qv",
        uri,
        parsedUri.getScheme());
    THROW_ERROR_EXCEPTION_IF(
        !parsedUri.getQuery().empty() || !parsedUri.getFragment().empty(),
        "Failed to parse S3 URI %Qv: query and fragment are not supported",
        uri);

    try {
        return TObjectDescriptor(parsedUri.getHost(), parsedUri.getPath(), allowEmptyKey);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Failed to parse S3 URI %Qv", uri)
            << ex;
    }
}

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TObjectDescriptor& object, TStringBuf spec)
{
    FormatValue(builder, Format("s3://%v/%v", object.Bucket(), object.Key()), spec);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NS3
