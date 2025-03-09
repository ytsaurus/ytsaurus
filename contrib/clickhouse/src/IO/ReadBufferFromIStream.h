#pragma once

#include <IO/ReadBuffer.h>
#include <IO/BufferWithOwnMemory.h>

#include <DBPoco/Net/HTTPBasicStreamBuf.h>


namespace DB
{

class ReadBufferFromIStream : public BufferWithOwnMemory<ReadBuffer>
{
private:
    std::istream & istr;
    DBPoco::Net::HTTPBasicStreamBuf & stream_buf;
    bool eof = false;

    bool nextImpl() override;

public:
    explicit ReadBufferFromIStream(std::istream & istr_, size_t size = DBMS_DEFAULT_BUFFER_SIZE);
};

}
