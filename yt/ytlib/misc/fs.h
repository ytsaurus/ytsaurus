#pragma once

/*!
 * \file fs.h
 * \brief Function to work with file system
 */

#include "common.h"
    
namespace NYT {
namespace NFS {

////////////////////////////////////////////////////////////////////////////////

//! File suffix for temporary files.
const char* const TempFileSuffix = "~";

//! Removes file.
/*!
 * \param name File name
 * \return True when succeeds
 */
bool Remove(Stroka name);

//! Renames file.
/*!
 * \param oldName Old name
 * \param newName New name
 * \return True when succeeds
 */
bool Rename(Stroka oldName, Stroka newName);

//! Returns name of file
/*!
 * \param filePath Path of file
 */
Stroka GetFileName(Stroka filePath);

//! Returns extension of file
/*!
 * \param filePath Path of file
 */
Stroka GetFileExtension(Stroka filePath);

//! Returns name of file without extension
/*!
 * \param filePath Path of file
 */
Stroka GetFileNameWithoutExtension(Stroka filePath);

//! Deletes all files with extension #TempFileSuffix in directory.
/*!
 * \param location Directory name
 */
void CleanTempFiles(Stroka location);

//! Returns available space at #path
i64 GetAvailableSpace(const Stroka& path) throw(yexception);

//! Creates the #path and parent directories if they don't exists
//! Actually a call of the same named function from util/folder/dirut
void MakePathIfNotExist(Stroka path, int mode = 0777);

//! Returns size of a file
/*!
 * \param filePath
 */
i64 GetFileSize(const Stroka& filePath);

////////////////////////////////////////////////////////////////////////////////

} // namespace NFS
} // namespace NYT
