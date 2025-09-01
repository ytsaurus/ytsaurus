#pragma once

#include "public.h"

#include <yt/yt/library/formats/format.h>

#include <yt/yt/server/lib/misc/public.h>

#include <yt/yt/core/http/public.h>

namespace NYT::NHttpProxy {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EFormatTarget,
    ((Input)  (0))
    ((Output) (1))
    ((Error)  (2))
);

NFormats::TFormat InferFormat(
    const NServer::TFormatManager& formatManager,
    const std::string& ytHeaderName,
    const NFormats::TFormat& ytHeaderFormat,
    const std::optional<std::string>& ytHeader,
    const std::string& mimeHeaderName,
    const std::string* mimeHeader,
    EFormatTarget target,
    NFormats::EDataType dataType);

NFormats::TFormat InferHeaderFormat(
    const NServer::TFormatManager& formatManager,
    const std::string* ytHeader);

TString FormatToMime(const NFormats::TFormat& format);

NYTree::INodePtr ConvertBytesToNode(
    TStringBuf bytes,
    const NFormats::TFormat& format);

////////////////////////////////////////////////////////////////////////////////

void FillFormattedYTErrorHeaders(
    const NHttp::IResponseWriterPtr& rsp,
    const TError& error,
    const NFormats::TFormat& format);
void FillFormattedYTErrorTrailers(
    const NHttp::IResponseWriterPtr& rsp,
    const TError& error,
    const NFormats::TFormat& format);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttpProxy
