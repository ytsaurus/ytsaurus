#include "row_level_security.h"

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

Y_WEAK void FormatValue(TStringBuilderBase* /*builder*/, const TRlsReadSpec& /*rlsReadSpec*/, TStringBuf /*spec*/)
{
    YT_ABORT();
}

////////////////////////////////////////////////////////////////////////////////

Y_WEAK IRlsCheckerFactoryPtr CreateRlsCheckerFactory(const TRlsReadSpec& /*rlsReadSpec*/)
{
    YT_ABORT();
}

////////////////////////////////////////////////////////////////////////////////

Y_WEAK std::optional<TRlsReadSpec> TRlsReadSpec::BuildFromRowLevelAclAndTableSchema(
    const TTableSchemaPtr& /*tableSchema*/,
    const std::optional<std::vector<NSecurityClient::TRowLevelAccessControlEntry>>& /*rowLevelAcl*/,
    const NLogging::TLogger& /*logger*/)
{
    YT_ABORT();
}

Y_WEAK bool TRlsReadSpec::IsTrivialDeny() const
{
    YT_ABORT();
}


Y_WEAK const std::string& TRlsReadSpec::GetPredicate() const
{
    YT_ABORT();
}


Y_WEAK const TTableSchemaPtr& TRlsReadSpec::GetTableSchema() const
{
    YT_ABORT();
}

Y_WEAK void ToProto(
    NProto::TRlsReadSpec* /*protoRlsReadSpec*/,
    const TRlsReadSpec& /*rlsReadSpec*/)
{
    YT_ABORT();
}

void FromProto(
    TRlsReadSpec* /*rlsReadSpec*/,
    const NProto::TRlsReadSpec& /*protoRlsReadSpec*/)
{
    YT_ABORT();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
