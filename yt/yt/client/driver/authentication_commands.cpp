#include "authentication_commands.h"

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NDriver {

using namespace NConcurrency;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TSetUserPasswordCommand::TSetUserPasswordCommand()
{
    RegisterParameter("user", User_);
    RegisterParameter("current_password_sha256", CurrentPasswordSha256_)
        .Default();
    RegisterParameter("new_password_sha256", NewPasswordSha256_);
}

void TSetUserPasswordCommand::DoExecute(ICommandContextPtr context)
{
    WaitFor(context->GetClient()->SetUserPassword(
        User_,
        CurrentPasswordSha256_,
        NewPasswordSha256_,
        Options))
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

TIssueTokenCommand::TIssueTokenCommand()
{
    RegisterParameter("user", User_);
    RegisterParameter("password_sha256", PasswordSha256_)
        .Default();
    RegisterParameter("description", Options.Description)
        .Default();
}

void TIssueTokenCommand::DoExecute(ICommandContextPtr context)
{
    auto result = WaitFor(context->GetClient()->IssueToken(
        User_,
        PasswordSha256_,
        Options))
        .ValueOrThrow();

    context->ProduceOutputValue(ConvertToYsonString(result.Token));
}

////////////////////////////////////////////////////////////////////////////////

TRevokeTokenCommand::TRevokeTokenCommand()
{
    RegisterParameter("user", User_);
    RegisterParameter("password_sha256", PasswordSha256_)
        .Default();
    RegisterParameter("token_sha256", TokenSha256_);
}

void TRevokeTokenCommand::DoExecute(ICommandContextPtr context)
{
    WaitFor(context->GetClient()->RevokeToken(
        User_,
        PasswordSha256_,
        TokenSha256_,
        Options))
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

TListUserTokensCommand::TListUserTokensCommand()
{
    RegisterParameter("user", User_);
    RegisterParameter("password_sha256", PasswordSha256_)
        .Default();
    RegisterParameter("with_metadata", Options.WithMetadata)
        .Default(false);
}

void TListUserTokensCommand::DoExecute(ICommandContextPtr context)
{
    auto result = WaitFor(context->GetClient()->ListUserTokens(
        User_,
        PasswordSha256_,
        Options))
        .ValueOrThrow();

    if (Options.WithMetadata) {
        context->ProduceOutputValue(ConvertToYsonString(result.Metadata));
    } else {
        context->ProduceOutputValue(ConvertToYsonString(result.Tokens));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDriver
