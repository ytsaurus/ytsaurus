#pragma once

#include "public.h"

#include <yt/client/formats/format.h>

namespace NYT {
namespace NHttpProxy {

////////////////////////////////////////////////////////////////////////////////

std::optional<NFormats::TFormat> MimeTypeToFormat(const TString& mimeType);
TString FormatToMime(const NFormats::TFormat& format);

NFormats::TFormat GetDefaultFormatForDataType(NFormats::EDataType dataType);

NYTree::INodePtr ConvertBytesToNode(
    const TString& bytes,
    const NFormats::TFormat& format);

std::optional<TString> GetBestAcceptedType(
    NFormats::EDataType outputType,
    const TString& clientAcceptHeader);

////////////////////////////////////////////////////////////////////////////////

} // namespace NHttpProxy
} // namespace NYT
