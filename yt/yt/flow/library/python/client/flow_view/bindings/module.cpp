#include <yt/yt/core/compression/codec.h>

#include <library/cpp/yt/memory/ref.h>
#include <library/cpp/yt/misc/enum.h>

#include <contrib/python/py3c/py3c.h>
#include <library/cpp/pybind/v2.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

//! Decompresses |data| (produced by an NCompression codec) back to the original bytes, using the same
//! library that compressed it. |codecName| is the NCompression::ECodec YSON literal (e.g. "zstd_2"), as
//! returned alongside the data by the get-flow-view command in compressed mode.
NPyBind::TPyObjectPtr Decompress(TStringBuf codecName, const TString& data)
{
    auto* codec = NCompression::GetCodec(ParseEnum<NCompression::ECodec>(codecName));
    auto decompressed = codec->Decompress(TSharedRef::FromString(data));
    // Hand the decompressed bytes straight to a Python bytes object, without an intermediate TString copy.
    return NPyBind::TPyObjectPtr(
        PyBytes_FromStringAndSize(decompressed.Begin(), decompressed.Size()),
        /*unref*/ true);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow

////////////////////////////////////////////////////////////////////////////////

MODULE_INIT_FUNC(bindings)
{
    DefFunc("decompress", &NYT::NFlow::Decompress);
    ::NPyBind::TPyModuleDefinition::InitModule("bindings");
    return ::NPyBind::TPyModuleDefinition::GetModule().M.RefGet();
}
