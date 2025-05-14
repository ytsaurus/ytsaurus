#include "config.h"

namespace NYT::NCrypto {

////////////////////////////////////////////////////////////////////////////////

void TPemBlobConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("file_name", &TThis::FileName)
        .Optional();
    registrar.Parameter("value", &TThis::Value)
        .Optional();

    registrar.Postprocessor([] (TThis* config) {
        if (config->FileName && config->Value) {
            THROW_ERROR_EXCEPTION("Cannot specify both \"file_name\" and \"value\"");
        }
        if (!config->FileName && !config->Value) {
            THROW_ERROR_EXCEPTION("Must specify either \"file_name\" or \"value\"");
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
    if (FileName) {
        return TFileInput(*FileName).ReadAll();
    } else if (Value) {
        return *Value;
    } else {
        THROW_ERROR_EXCEPTION("Neither \"file_name\" nor \"value\" is given");
    }
}

////////////////////////////////////////////////////////////////////////////////

void TSslContextCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("name", &TThis::Name)
        .NonEmpty();
    registrar.Parameter("value", &TThis::Value);
}


TSslContextCommandPtr TSslContextCommand::Create(std::string name, std::string value)
{
    auto command = New<TSslContextCommand>();
    command->Name = std::move(name);
    command->Value = std::move(value);
    return command;
}

////////////////////////////////////////////////////////////////////////////////

void TSslContextConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("ca", &TThis::CertificateAuthority)
        .Default();
    registrar.Parameter("cert_chain", &TThis::CertificateChain)
        .Optional();
    registrar.Parameter("private_key", &TThis::PrivateKey)
        .Optional();
    registrar.Parameter("ssl_configuration_commands", &TThis::SslConfigurationCommands)
        .Optional();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCrypto

