#pragma once

#include "storage.h"

#include <memory>

namespace NInterop {

////////////////////////////////////////////////////////////////////////////////

class IServer
{
public:
    virtual ~IServer() = default;

    virtual void Start() = 0;
    virtual void Shutdown() = 0;
};

using IServerPtr = std::shared_ptr<IServer>;

}   // namespace NInterop
