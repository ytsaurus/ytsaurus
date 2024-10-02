#include "config.h"

namespace NYT::NOrm::NServer::NApi {

////////////////////////////////////////////////////////////////////////////////

void TObjectServiceConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("protobuf_format_unknown_fields_mode", &TThis::ProtobufFormatUnknownFieldsMode)
        .Default(NYson::EUnknownYsonFieldsMode::Skip);
    registrar.Parameter("enforce_read_permissions", &TThis::EnforceReadPermissions)
        .Default(false);
    registrar.Parameter("enable_request_cancelation", &TThis::EnableRequestCancelation)
        .Default(true);
    registrar.Parameter("enable_mutating_request_cancelation", &TThis::EnableMutatingRequestCancelation)
        .Default(false);

    registrar.Preprocessor([] (TThis* config) {
        config->EnablePerUserProfiling = true;
    });
    registrar.Postprocessor([] (TThis* config) {
        if (config->EnableMutatingRequestCancelation && !config->EnableRequestCancelation) {
            THROW_ERROR_EXCEPTION("\"enable_mutating_request_cancelation\" may be set only when "
                "\"enable_request_cancelation\" is set");
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NApi
