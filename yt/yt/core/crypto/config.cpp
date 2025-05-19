#include "config.h"

#include <util/system/env.h>

namespace NYT::NCrypto {

////////////////////////////////////////////////////////////////////////////////

void TPemBlobConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("environment_variable", &TThis::EnvironmentVariable)
        .Optional();
    registrar.Parameter("file_name", &TThis::FileName)
        .Optional();
    registrar.Parameter("value", &TThis::Value)
        .Optional();

    registrar.Postprocessor([] (TThis* config) {
        if (!!config->EnvironmentVariable + !!config->FileName + !!config->Value != 1) {
            THROW_ERROR_EXCEPTION("Must specify one of \"environment_variable\", \"file_name\", or \"value\"");
        }
    });
}

TPemBlobConfigPtr TPemBlobConfig::CreateFileReference(const TString& fileName)
{
    auto config = New<TPemBlobConfig>();
    config->FileName = fileName;
    return config;
}

TString TPemBlobConfig::LoadBlob() const
{
    if (EnvironmentVariable) {
        return GetEnv(*EnvironmentVariable);
    } else if (FileName) {
        return TFileInput(*FileName).ReadAll();
    } else if (Value) {
        return *Value;
    } else {
        THROW_ERROR_EXCEPTION("Neither \"environment_variable\" nor \"file_name\" nor \"value\" is given");
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCrypto

