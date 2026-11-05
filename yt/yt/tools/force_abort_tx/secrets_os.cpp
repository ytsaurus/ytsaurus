#include "secrets.h"

std::optional<std::string> TryGetClientSecret(std::string /*cluster*/)
{
    return std::nullopt;
}
