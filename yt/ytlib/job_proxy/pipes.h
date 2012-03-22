#pragma once

#include "common.h"

#include <ytlib/table_client/writer.h>

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

class TErrorPipe
    : public IDataPipe
{
public:
    TErrorPipe(TOutputStream* output, int jobDescriptor = 2);

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
    typedef TIntrusivePtr<TInputPipe> TPtr;

    /*!
     *  \note Takes ownership of the input stream.
     *  \param jobDescriptor - number of underlying read descriptor in the job process.
     */
    TInputPipe(TInputStream* input, int jobDescriptor);

    void PrepareJobDescriptors();
    void PrepareProxyDescriptors();

    int GetEpollDescriptor() const;
    int GetEpollFlags() const;

    bool ProcessData(ui32 epollEvents);
    void Finish();

private:
    TPipe Pipe;
    TAutoPtr<TInputStream> InputStream;
    int JobDescriptor;

    // ToDo: configurable?
    static const int BufferSize = 4096;
    char Buffer[BufferSize];
    int Position;
    int Length;

    bool HasData;
    bool IsFinished;
};

////////////////////////////////////////////////////////////////////

class TOutputPipe
    : public IDataPipe
{
public:
    typedef TIntrusivePtr<TOutputPipe> TPtr;

    TOutputPipe(NTableClient::ISyncWriter* writer, int jobDescriptor);

    /*!
     *  Called from job process after fork and before exec.
     */
    void PrepareJobDescriptors();
    void PrepareProxyDescriptors();

    int GetEpollDescriptor() const;
    int GetEpollFlags() const;

    bool ProcessData(ui32 epollEvents);
    void Finish();

private:
    static void* ThreadFunc(void* param);
    void ThreadMain();

    TPipe ReadingPipe;
    TPipe FinishPipe;

    Stroka ErrorString;
    int JobDescriptor;

    NTableClient::ISyncWriter::TPtr Writer;
    TThread OutputThread;
};

////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
