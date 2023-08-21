#include "config.h"

namespace NYT::NShell {

////////////////////////////////////////////////////////////////////////////////

void TShellParameters::Register(TRegistrar registrar)
{
    registrar.Parameter("shell_id", &TThis::ShellId)
        .Default();
    registrar.Parameter("shell_index", &TThis::ShellIndex)
        .Default();
    registrar.Parameter("operation", &TThis::Operation);
    registrar.Parameter("term", &TThis::Term)
        .Default();
    registrar.Parameter("keys", &TThis::Keys)
        .Default();
    registrar.Parameter("input_offset", &TThis::InputOffset)
        .Default();
    registrar.Parameter("height", &TThis::Height)
        .Default(0);
    registrar.Parameter("width", &TThis::Width)
        .Default(0);
    registrar.Parameter("inactivity_timeout", &TThis::InactivityTimeout)
        .Default(TDuration::Seconds(5 * 60));
    registrar.Parameter("environment", &TThis::Environment)
        .Default();
    registrar.Parameter("command", &TThis::Command)
        .Default();

    registrar.Postprocessor([] (TThis* config) {
        if (config->Operation != EShellOperation::Spawn && !config->ShellId) {
            THROW_ERROR_EXCEPTION(
                "Malformed request: shell id is not specified for %Qlv operation",
                config->Operation);
        }
        if (config->Operation == EShellOperation::Update && !config->Keys.empty() && !config->InputOffset) {
            THROW_ERROR_EXCEPTION(
                "Malformed request: input offset is not specified for %Qlv operation",
                config->Operation);
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TShellResult::Register(TRegistrar registrar)
{
    registrar.Parameter("shell_id", &TThis::ShellId);
    registrar.Parameter("shell_index", &TThis::ShellIndex);
    registrar.Parameter("output", &TThis::Output);
    registrar.Parameter("consumed_offset", &TThis::ConsumedOffset);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NShell
