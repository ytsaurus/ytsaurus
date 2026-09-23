#include "porto_helpers.h"

#include <util/string/split.h>

namespace NYT::NContainers {

////////////////////////////////////////////////////////////////////////////////

namespace {

bool IsPathTraversalComponent(TStringBuf component)
{
    return component == "." || component == "..";
}

bool ContainsPathTraversal(TStringBuf value)
{
    for (const auto& component : StringSplitter(value).Split('/')) {
        if (IsPathTraversalComponent(component.Token())) {
            return true;
        }
    }

    return false;
}

} // namespace

TError CheckPortoContainerNameComponent(TStringBuf component)
{
    if (component.Contains('/')) {
        return TError("Container name component must not contain \"/\"")
            .With("component", component);
    }

    if (IsPathTraversalComponent(component)) {
        return TError("Container name component must not be a \".\" or \"..\" path segment")
            .With("component", component);
    }

    return {};
}

void ValidatePortoContainerNameComponent(TStringBuf component)
{
    CheckPortoContainerNameComponent(component).ThrowOnError();
}

TError CheckPortoContainerSubpath(TStringBuf subpath)
{
    if (subpath.empty()) {
        return {};
    }

    if (!subpath.StartsWith('/')) {
        return TError("Container subpath must start with \"/\"")
            .With("subpath", subpath);
    }

    if (ContainsPathTraversal(subpath)) {
        return TError("Container subpath must not contain a \".\" or \"..\" path segment")
            .With("subpath", subpath);
    }

    return {};
}

void ValidatePortoContainerSubpath(TStringBuf subpath)
{
    CheckPortoContainerSubpath(subpath).ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NContainers
