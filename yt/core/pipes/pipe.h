#pragma once

#include "public.h"

#include <yt/core/net/public.h>

#include <yt/core/ytree/yson_serializable.h>

namespace NYT::NPipes {

////////////////////////////////////////////////////////////////////////////////

class TNamedPipe
    : public TRefCounted
{
public:
    ~TNamedPipe();
    static TNamedPipePtr Create(const TString& path);

    NNet::IConnectionReaderPtr CreateAsyncReader();
    NNet::IConnectionWriterPtr CreateAsyncWriter();

    TString GetPath() const;

private:
    const TString Path_;

    explicit TNamedPipe(const TString& path);
    void Open();
    DECLARE_NEW_FRIEND();
};

DEFINE_REFCOUNTED_TYPE(TNamedPipe)

////////////////////////////////////////////////////////////////////////////////

struct TNamedPipeConfig
    : public NYTree::TYsonSerializableLite
{
    TString Path;
    int FD;
    bool Write;

    TNamedPipeConfig()
        : TNamedPipeConfig(TString(), 0, false)
    { }

    TNamedPipeConfig(TNamedPipeConfig&& other)
    {
        Path = std::move(other.Path);
        FD = std::move(other.FD);
        Write = std::move(other.Write);
    }

    TNamedPipeConfig(const TString& path, int fd, bool write)
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

    NNet::IConnectionReaderPtr CreateAsyncReader();
    NNet::IConnectionWriterPtr CreateAsyncWriter();

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

TString ToString(const TPipe& pipe);

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

} // namespace NYT::NPipes
