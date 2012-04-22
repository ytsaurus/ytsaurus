#pragma once

#include "private.h"
#include <ytlib/table_client/public.h>
#include <ytlib/misc/blob_output.h>

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////

int SafeDup(int oldFd);
void SafeDup2(int oldFd, int newFd);
void SafeClose(int fd);
int SafePipe(int fd[2]);
void SafeMakeNonblocking(int fd);

////////////////////////////////////////////////////////////////////

struct TPipe 
{
    int ReadFd;
    int WriteFd;

    TPipe(int fd[2])
    : ReadFd(fd[0])
    , WriteFd(fd[1])
    { }

    TPipe()
    : ReadFd(-1)
    , WriteFd(-1)
    { }
};

////////////////////////////////////////////////////////////////////

struct IDataPipe
    : public virtual TRefCounted
{
    typedef TIntrusivePtr<IDataPipe> TPtr;

    /*!
     *  Called from job process after fork and before exec.
     *  Closes unused fds, remaps other to a proper number.
     */
    virtual void PrepareJobDescriptors() = 0;

    /*!
     *  Called from proxy process after fork.
     *  E.g. makes required pipes non-blocking.
     */
    virtual void PrepareProxyDescriptors() = 0;

    virtual int GetEpollDescriptor() const = 0;
    virtual int GetEpollFlags() const = 0;

    /*!
     *  \returns false if pipe is closed, otherwise true.
     */
    virtual bool ProcessData(ui32 epollEvent) = 0;
    virtual void Finish() = 0;
};

////////////////////////////////////////////////////////////////////

class TOutputPipe
    : public IDataPipe
{
public:
    TOutputPipe(TAutoPtr<TOutputStream> output, int jobDescriptor);

    void PrepareJobDescriptors();
    void PrepareProxyDescriptors();

    int GetEpollDescriptor() const;
    int GetEpollFlags() const;

    bool ProcessData(ui32 epollEvent);
    void Finish();

private:
    TAutoPtr<TOutputStream> OutputStream;
    int JobDescriptor;
    TPipe Pipe;
    bool IsFinished;
};

////////////////////////////////////////////////////////////////////

class TInputPipe
    : public IDataPipe
{
public:
    /*!
     *  \note Takes ownership of the input stream.
     *  \param jobDescriptor - number of underlying read descriptor in the job process.
     */
    TInputPipe(
        TAutoPtr<NTableClient::TTableProducer> tableProducer, 
        TAutoPtr<TBlobOutput> buffer, 
        TAutoPtr<NYTree::IYsonConsumer> cosumer,
        int jobDescriptor);

    void PrepareJobDescriptors();
    void PrepareProxyDescriptors();

    int GetEpollDescriptor() const;
    int GetEpollFlags() const;

    bool ProcessData(ui32 epollEvents);
    void Finish();

private:
    TPipe Pipe;
    int JobDescriptor;

    TAutoPtr<NTableClient::TTableProducer> TableProducer;
    TAutoPtr<TBlobOutput> Buffer;
    TAutoPtr<NYTree::IYsonConsumer> Consumer;
    int Position;

    bool HasData;
    bool IsFinished;
};

////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
