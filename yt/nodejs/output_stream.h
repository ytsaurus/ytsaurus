#pragma once

#include "stream_base.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TNodeJSOutputStream
    : public TNodeJSStreamBase
{
public:
    TNodeJSOutputStream();
    ~TNodeJSOutputStream();

public:
    // Synchronous JS API.
    v8::Handle<v8::Value> Pull();

    // Asynchronous JS API.

    // C++ API.
    void Write(const void* buffer, size_t length);

private:
    friend class TNodeJSDriverHost;

    static void DeleteCallback(char* data, void* hint);

    pthread_mutex_t Mutex;
    TQueue Queue;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
