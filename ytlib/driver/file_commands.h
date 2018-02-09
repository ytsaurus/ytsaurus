#pragma once

#include "command.h"

#include <yt/ytlib/ypath/rich.h>

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

class TReadFileCommand
    : public TTypedCommand<NApi::TFileReaderOptions>
{
public:
    TReadFileCommand();

private:
    NYPath::TRichYPath Path;
    NYTree::INodePtr FileReader;

    virtual void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TWriteFileCommand
    : public TTypedCommand<NApi::TFileWriterOptions>
{
public:
    TWriteFileCommand();

private:
    NYPath::TRichYPath Path;
    NYTree::INodePtr FileWriter;
    bool ComputeMD5;

    virtual void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TGetFileFromCacheCommand
    : public TTypedCommand<NApi::TGetFileFromCacheOptions>
{
public:
    TGetFileFromCacheCommand();

private:
    TString MD5;

    virtual void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TPutFileToCacheCommand
    : public TTypedCommand<NApi::TPutFileToCacheOptions>
{
public:
    TPutFileToCacheCommand();

private:
    NYPath::TYPath Path;
    TString MD5;

    virtual void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT

