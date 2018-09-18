#pragma once

#include "public.h"

#include <yt/client/formats/format.h>

namespace NYT {
namespace NHttpProxy {

////////////////////////////////////////////////////////////////////////////////

TNullable<NFormats::TFormat> MimeTypeToFormat(const TString& mimeType);
TString FormatToMime(const NFormats::TFormat& format);

NFormats::TFormat GetDefaultFormatForDataType(NFormats::EDataType dataType);

NYTree::INodePtr ConvertBytesToNode(
    const TString& bytes,
    const NFormats::TFormat& format);

TNullable<TString> GetBestAcceptedType(
    NFormats::EDataType outputType,
    const TString& clientAcceptHeader);

TNullable<TString> GetBestAcceptedEncoding(const TString& clientAcceptEncodingHeader);
TNullable<EContentEncoding> EncodingToCompression(const TString& encoding);

////////////////////////////////////////////////////////////////////////////////

} // namespace NHttpProxy
} // namespace NYT
