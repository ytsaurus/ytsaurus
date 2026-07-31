#pragma once

#include <yt/yt/flow/library/cpp/common/file_source.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

void ValidateFileSourceBasename(TStringBuf basename);

class TFileSourceBase
    : public IFileSource
{
public:
    explicit TFileSourceBase(TFileSourceContextPtr context);

    TFileSourceContextPtr GetContext() const;

protected:
    NYTree::TYsonStructPtr GetParametersBase() const final;

private:
    const TFileSourceContextPtr Context_;
    const NYTree::TYsonStructPtr Parameters_;
};

DEFINE_REFCOUNTED_TYPE(TFileSourceBase);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
