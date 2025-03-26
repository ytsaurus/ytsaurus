#pragma once

#include <IO/BufferWithOwnMemory.h>
#include <IO/ReadBuffer.h>
#include <Common/AsyncTaskExecutor.h>
#include <DBPoco/Net/Socket.h>

namespace DB
{

/// Works with the ready DBPoco::Net::Socket. Blocking operations.
class ReadBufferFromPocoSocket : public BufferWithOwnMemory<ReadBuffer>
{
protected:
    DBPoco::Net::Socket & socket;

    /** For error messages. It is necessary to receive this address in advance, because,
      *  for example, if the connection is broken, the address will not be received anymore
      *  (getpeername will return an error).
      */
    DBPoco::Net::SocketAddress peer_address;

    ProfileEvents::Event read_event;

    bool nextImpl() override;

public:
    explicit ReadBufferFromPocoSocket(DBPoco::Net::Socket & socket_, size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE);
    explicit ReadBufferFromPocoSocket(DBPoco::Net::Socket & socket_, const ProfileEvents::Event & read_event_, size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE);

    bool poll(size_t timeout_microseconds) const;

    void setAsyncCallback(AsyncCallback async_callback_) { async_callback = std::move(async_callback_); }

private:
    AsyncCallback async_callback;
    std::string socket_description;
};

}
