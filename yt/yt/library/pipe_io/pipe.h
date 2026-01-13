#pragma once

#include "public.h"

#include <yt/yt/core/net/public.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NPipeIO {

////////////////////////////////////////////////////////////////////////////////

class TNamedPipe
    : public TRefCounted
{
public:
    ~TNamedPipe();
    static TNamedPipePtr Create(const std::string& path, int permissions = 0660, std::optional<int> capacity = {});
    static TNamedPipePtr FromPath(const std::string& path);

    NNet::IConnectionReaderPtr CreateAsyncReader();
    NNet::IConnectionWriterPtr CreateAsyncWriter(NNet::EDeliveryFencedMode deliveryFencedMode = NNet::EDeliveryFencedMode::None);

    std::string GetPath() const;

private:
    const std::string Path_;
    const std::optional<int> Capacity_;

    //! Whether pipe was created by this class
    //! and should be removed in destructor.
    const bool Owning_;

    explicit TNamedPipe(const std::string& path, std::optional<int> capacity, bool owning);
    void Open(int permissions);
    DECLARE_NEW_FRIEND()
};

DEFINE_REFCOUNTED_TYPE(TNamedPipe)

////////////////////////////////////////////////////////////////////////////////

struct TNamedPipeConfig
    : public NYTree::TYsonStruct
{
    std::string Path;
    int FD = 0;
    bool Write = false;

    static TNamedPipeConfigPtr Create(std::string path, int fd, bool write);

    REGISTER_YSON_STRUCT(TNamedPipeConfig);

    static void Register(TRegistrar registrar);
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

    explicit TPipe(int fd[2]);
    void Init(TPipe&& other);

    friend class TPipeFactory;
};

void FormatValue(TStringBuilderBase* builder, const TPipe& pipe, TStringBuf spec);

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

} // namespace NYT::NPipeIO
