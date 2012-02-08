#pragma once

namespace NYT {

class TErrorContext
{
public:
    TErrorContext();
    virtual ~TErrorContext();

    virtual void ToString(char * buffer, int size) const = 0;
    virtual void SerializeToString(char * buffer, int size) const = 0;
};

int SetupErrorHandler();

} // namespace NYT