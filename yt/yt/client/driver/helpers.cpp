#include "helpers.h"

#include <yt/yt/core/misc/error.h>

namespace NYT::NDriver {

using namespace NObjectClient;

///////////////////////////////////////////////////////////////////////////////

bool operator==(const TEtag& lhs, const TEtag& rhs)
{
    return lhs.Id == rhs.Id && lhs.Revision == rhs.Revision;
}

TErrorOr<TEtag> ParseEtag(TStringBuf etagString)
{
    static const TErrorOr<TEtag> parseError(TError("Failed to parse etag"));

    TStringBuf idString;
    TStringBuf revisionString;
    if (!etagString.TrySplit(':', idString, revisionString)) {
        return parseError;
    }

    TEtag result;

    if (!TObjectId::FromString(idString, &result.Id)) {
        return parseError;
    }

    if (!TryFromString(revisionString, result.Revision)) {
        return parseError;
    }

    return result;
}

TString ToString(const TEtag& Etag)
{
    return Format("%v:%v", Etag.Id, Etag.Revision);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDriver
