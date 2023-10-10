#pragma once

#include "public.h"

#include <yt/yt/library/formats/format.h>

namespace NYT::NHttpProxy {

////////////////////////////////////////////////////////////////////////////////

NFormats::TFormat InferFormat(
    const TFormatManager& formatManager,
    const TString& ytHeaderName,
    const NFormats::TFormat& ytHeaderFormat,
    const std::optional<TString>& ytHeader,
    const TString& mimeHeaderName,
    const TString* mimeHeader,
    bool isOutput,
    NFormats::EDataType dataType);

NFormats::TFormat InferHeaderFormat(
    const TFormatManager& formatManager,
    const TString* ytHeader);

TString FormatToMime(const NFormats::TFormat& format);

NYTree::INodePtr ConvertBytesToNode(
    const TString& bytes,
    const NFormats::TFormat& format);

std::optional<TString> GetBestAcceptedType(
    NFormats::EDataType outputType,
    const TString& clientAcceptHeader);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttpProxy
