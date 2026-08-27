#pragma once

#include <yt/yt/flow/library/cpp/common/file_source.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

class TFileSourceBase
    : public IFileSource
{
public:
    TFileSourceBase(
        TFileSourceContextPtr context,
        TDynamicFileSourceContextPtr dynamicContext);

    TFileSourceContextPtr GetContext() const;
    TDynamicFileSourceContextPtr GetDynamicContext() const;
    TFileSourceSpecPtr GetSpec() const;
    TDynamicFileSourceSpecPtr GetDynamicSpec() const;

protected:
    NYTree::TYsonStructPtr GetParametersBase() const final;
    NYTree::TYsonStructPtr GetDynamicParametersBase() const final;

private:
    const TFileSourceContextPtr Context_;
    const NYTree::TYsonStructPtr Parameters_;
    TAtomicIntrusivePtr<TDynamicFileSourceContext> DynamicContext_;
    TAtomicIntrusivePtr<NYTree::TYsonStruct> DynamicParameters_;
};

DEFINE_REFCOUNTED_TYPE(TFileSourceBase);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
