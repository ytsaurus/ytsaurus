#pragma once

#include "public.h"

#include <yt/yt/library/formats/format.h>

#include <yt/yt/server/lib/misc/public.h>

namespace NYT::NHttpProxy {

////////////////////////////////////////////////////////////////////////////////

NFormats::TFormat InferFormat(
    const NServer::TFormatManager& formatManager,
    const std::string& ytHeaderName,
    const NFormats::TFormat& ytHeaderFormat,
    const std::optional<std::string>& ytHeader,
    const std::string& mimeHeaderName,
    const std::string* mimeHeader,
    bool isOutput,
    NFormats::EDataType dataType);

NFormats::TFormat InferHeaderFormat(
    const NServer::TFormatManager& formatManager,
    const std::string* ytHeader);

TString FormatToMime(const NFormats::TFormat& format);

NYTree::INodePtr ConvertBytesToNode(
    TStringBuf bytes,
    const NFormats::TFormat& format);

std::optional<TString> GetBestAcceptedType(
    NFormats::EDataType outputType,
    const TString& clientAcceptHeader);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttpProxy
