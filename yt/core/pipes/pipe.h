#pragma once

#include "public.h"

#include <yt/core/ytree/yson_serializable.h>

namespace NYT {
namespace NPipes {

////////////////////////////////////////////////////////////////////////////////

class TNamedPipe
    : public TRefCounted
{
public:
    ~TNamedPipe();
    static TNamedPipePtr Create(const Stroka& path);
    TAsyncReaderPtr CreateAsyncReader();
    TAsyncWriterPtr CreateAsyncWriter();
    Stroka GetPath() const;

private:
    const Stroka Path_;

    explicit TNamedPipe(const Stroka& path);
    void Open();
    DECLARE_NEW_FRIEND();
};

DEFINE_REFCOUNTED_TYPE(TNamedPipe);

////////////////////////////////////////////////////////////////////////////////

struct TNamedPipeConfig
    : public NYTree::TYsonSerializableLite
{
    Stroka Path;
    int FD;
    bool Write;

    TNamedPipeConfig()
        : TNamedPipeConfig(Stroka(), 0, false)
    { }

    TNamedPipeConfig(TNamedPipeConfig&& other)
    {
        Path = std::move(other.Path);
        FD = std::move(other.FD);
        Write = std::move(other.Write);
    }

    TNamedPipeConfig(const Stroka& path, int fd, bool write)
        : Path(path)
        , FD(fd)
        , Write(write)
    {
        RegisterParameter("path", Path);
        RegisterParameter("fd", FD);
        RegisterParameter("write", Write);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TPipe
    : public TNonCopyable
{
public:
    static const int InvalidFD = -1;

    TPipe();
    TPipe(TPipe&& pipe);
    ~TPipe();

    void operator=(TPipe&& other);

    void CloseReadFD();
    void CloseWriteFD();

    TAsyncReaderPtr CreateAsyncReader();
    TAsyncWriterPtr CreateAsyncWriter();

    int ReleaseReadFD();
    int ReleaseWriteFD();

    int GetReadFD() const;
    int GetWriteFD() const;

private:
    int ReadFD_ = InvalidFD;
    int WriteFD_ = InvalidFD;

    TPipe(int fd[2]);
    void Init(TPipe&& other);

    friend class TPipeFactory;
};

Stroka ToString(const TPipe& pipe);

////////////////////////////////////////////////////////////////////////////////

class TPipeFactory
{
public:
    explicit TPipeFactory(int minFD = 0);
    ~TPipeFactory();

    TPipe Create();

    void Clear();

private:
    const int MinFD_;
    std::vector<int> ReservedFDs_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NPipes
} // namespace NYT
