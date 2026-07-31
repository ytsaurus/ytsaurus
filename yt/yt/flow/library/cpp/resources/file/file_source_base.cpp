#include "file_source_base.h"

#include <yt/yt/flow/library/cpp/common/registry.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

void ValidateFileSourceBasename(TStringBuf basename)
{
    THROW_ERROR_EXCEPTION_UNLESS(
        !basename.empty() &&
            basename != "." &&
            basename != ".." &&
            basename.find('/') == TStringBuf::npos &&
            basename.find('\\') == TStringBuf::npos &&
            basename.find('\0') == TStringBuf::npos,
        "File source basename %Qv must be a single normal path component",
        basename);
}

////////////////////////////////////////////////////////////////////////////////

TFileSourceBase::TFileSourceBase(TFileSourceContextPtr context)
    : Context_(std::move(context))
    , Parameters_(TRegistry::Get()->ParseFileSourceParameters(Context_->SourceSpec))
{ }

TFileSourceContextPtr TFileSourceBase::GetContext() const
{
    return Context_;
}

NYTree::TYsonStructPtr TFileSourceBase::GetParametersBase() const
{
    return Parameters_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
