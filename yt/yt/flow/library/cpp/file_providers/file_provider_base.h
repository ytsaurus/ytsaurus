#pragma once

#include <yt/yt/flow/library/cpp/common/file_provider.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

class TFileProviderBase
    : public IFileProvider
{
public:
    TFileProviderBase(
        TFileProviderContextPtr context,
        TDynamicFileProviderContextPtr dynamicContext);

    TFileProviderContextPtr GetContext() const;
    TDynamicFileProviderContextPtr GetDynamicContext() const;
    TFileProviderSpecPtr GetSpec() const;
    TDynamicFileProviderSpecPtr GetDynamicSpec() const;

protected:
    NYTree::TYsonStructPtr GetParametersBase() const final;
    NYTree::TYsonStructPtr GetDynamicParametersBase() const final;

private:
    const TFileProviderContextPtr Context_;
    const NYTree::TYsonStructPtr Parameters_;
    TAtomicIntrusivePtr<TDynamicFileProviderContext> DynamicContext_;
    TAtomicIntrusivePtr<NYTree::TYsonStruct> DynamicParameters_;
};

DEFINE_REFCOUNTED_TYPE(TFileProviderBase);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
