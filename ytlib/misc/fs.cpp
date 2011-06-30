#include "fs.h"

#include "../logging/log.h"

#include <util/folder/dirut.h>
#include <util/folder/filelist.h>
#include <util/system/fs.h>
#include <quality/util/file_utils.h>

namespace NYT {

//////////////////////////////////////////////////////////////////////////////

static NLog::TLogger Logger("FS");

//////////////////////////////////////////////////////////////////////////////

Stroka GetFileExtension(Stroka fileName)
{
    i32 dotPosition = fileName.find_last_of('.');
    if (dotPosition < 0) {
        return "";
    }
    return fileName.substr(dotPosition + 1, fileName.size() - dotPosition - 1);
}

Stroka GetFileNameWithoutExtension(Stroka fileName)
{
    fileName = GetFilename(fileName);
    i32 dotPosition = fileName.find_last_of('.');
    if (dotPosition < 0) {
        return fileName;
    }
    return fileName.substr(0, dotPosition);
}

void CleanTempFiles(Stroka location)
{
    LOG_INFO("Cleaning temp files in %s", ~location);
    TFileList fileList;
    fileList.Fill(location);
    const char* fileName;
    while ((fileName = fileList.Next()) != NULL) {
        Stroka fullName = location + "/" + Stroka(fileName);
        if (fullName.has_suffix(TempFileSuffix)) {
            LOG_INFO("Removing file %s", ~fullName);
            int result = NFs::Remove(~fullName);
            if (result != 0) {
                LOG_ERROR("Error %d removing %s", result,  ~fullName);
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
