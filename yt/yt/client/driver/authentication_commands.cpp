#include "authentication_commands.h"

namespace NYT::NDriver {

using namespace NConcurrency;

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

} // namespace NYT::NDriver
