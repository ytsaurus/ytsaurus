#pragma once

#include <mapreduce/yt/interface/fwd.h>

#include <util/generic/fwd.h>

class TInputStream;
class TOutputStream;

namespace NYtBlob {
    class TBlobInfo;

    void Upload(
        const TString& path, const TString& filename,
        const ::NYT::TYPath& table, ::NYT::IClientBasePtr client);

    void Upload(
        TInputStream& in, const TString& filename,
        const ::NYT::TYPath& table, ::NYT::IClientBasePtr client);

    void Download(
        const TString& path, const TStringBuf filename,
        const ::NYT::TYPath& table, ::NYT::IClientBasePtr client,
        const bool storeEntireFileInMemoryWhileDownloading);
    bool TryDownload(
        const TString& path, const TStringBuf filename,
        const ::NYT::TYPath& table, ::NYT::IClientBasePtr client,
        const bool storeEntireFileInMemoryWhileDownloading);

    void Download(
        const TString& path, const TStringBuf filename,
        const ::NYT::TYPath& table, ::NYT::IClientBasePtr client);
    bool TryDownload(
        const TString& path, const TStringBuf filename,
        const ::NYT::TYPath& table, ::NYT::IClientBasePtr client);

    void Download(
        TOutputStream& out, const TStringBuf filename,
        const ::NYT::TYPath& table, ::NYT::IClientBasePtr client);
    bool TryDownload(
        TOutputStream& out, const TStringBuf filename,
        const ::NYT::TYPath& table, ::NYT::IClientBasePtr client);

    void CreateTable(const ::NYT::TYPath& table, ::NYT::IClientBasePtr client);
    void Finish(const ::NYT::TYPath& table, ::NYT::IClientBasePtr client);
    void Finish(const ::NYT::TYPath& table, ::NYT::IClientBasePtr client, const bool uniqueFiles);
    void Reset(const ::NYT::TYPath& table, ::NYT::IClientBasePtr client);

    void List(const ::NYT::TYPath& table, ::NYT::IClientBasePtr client,
              yvector<TBlobInfo>& infos);
    yvector<TBlobInfo> List(const ::NYT::TYPath& table, ::NYT::IClientBasePtr client);
}
