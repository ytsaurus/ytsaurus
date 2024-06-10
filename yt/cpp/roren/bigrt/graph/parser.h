#pragma once

#include <yt/cpp/roren/interface/fwd.h>
#include <yt/cpp/roren/interface/private/fwd.h>
#include <yt/cpp/roren/interface/private/row_vtable.h>
#include <yt/cpp/roren/interface/private/row_vtable.h>

#include <yt/yt/library/tvm/service/public.h>

namespace NBigRT {
class TSupplierConfig;
} // namespace NBigRT

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

class TCompositeBigRtWriterFactory;

using TWriterRegistrator = std::function<void(TCompositeBigRtWriterFactory&, const NYT::NAuth::ITvmServicePtr& tvmService)>;

extern const TTypeTag<TWriterRegistrator> WriterRegistratorTag;
extern const TTypeTag<bool> BindToDictTag;
extern const TTypeTag<TString> InputTag;

struct TBigRtResharderDescription
{
    TString InputTag;

    // Optimized reshareder. All `ParDo`s merged into one.
    IRawParDoPtr Resharder;

    // Writers to write `Resharder`'s outputs.
    // Output #i goes to Writers[i].
    std::vector<IRawWritePtr> Writers;

    std::vector<TWriterRegistrator> WriterRegistratorList;
};

std::vector<TBigRtResharderDescription> ParseBigRtResharder(const TPipeline& pipeline);

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
