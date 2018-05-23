#include "config.h"

namespace NYT {
namespace NCrypto {

////////////////////////////////////////////////////////////////////////////////

TPemBlobConfig::TPemBlobConfig()
{
    RegisterParameter("file_name", FileName)
        .Optional();
    RegisterParameter("value", Value)
        .Optional();

    RegisterPostprocessor([&] {
        if (FileName && Value) {
            THROW_ERROR_EXCEPTION("Cannot specify both \"file_name\" and \"value\"");
        }
        if (!FileName && !Value) {
            THROW_ERROR_EXCEPTION("Must specify either \"file_name\" or \"value\"");
        }
    });
}

TString TPemBlobConfig::LoadBlob() const
{
    if (FileName) {
        return TFileInput(*FileName).ReadAll();
    } else if (Value) {
        return *Value;
    } else {
        THROW_ERROR_EXCEPTION("Neither \"file_name\" nor \"value\" is given");
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCrypto
} // namespace NYT

