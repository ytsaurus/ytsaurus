#include "modes.h"

#include <library/cpp/getopt/small/modchooser.h>

int NFileYtTool::Main(const int argc, const char* argv[]) {
    TModChooser app;
    app.SetDescription("utility to upload/download files stored in YT table");
    app.AddMode(
        "upload",
        MainUpload,
        "upload files to YT table");
    app.AddMode(
        "download",
        MainDownload,
        "download files from YT table");
    app.AddMode(
        "create",
        MainCreate,
        "create YT table for further files upload");
    app.AddMode(
        "finish",
        MainFinish,
        "do preprocessing on YT file table to enable download from it");
    app.AddMode(
        "reset",
        MainReset,
        "make table writable again (after `finish`)");
    app.AddMode(
        "list",
        MainList,
        "list files in YT table, will write JSONs containing info about documents");
    app.AddMode(
        "download-all",
        MainDownloadAll,
        "download all files from YT table");
    return app.Run(argc, argv);
}
