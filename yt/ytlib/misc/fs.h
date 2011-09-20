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
bool Remove(const Stroka& name);

//! Renames file.
/*!
 * \param oldName Old name
 * \param newName New name
 * \return True when succeeds
 */
bool Rename(const Stroka& oldName, const Stroka& newName);

//! Returns name of file.
/*!
 * \param path Path of file
 */
Stroka GetFileName(const Stroka& path);

//! Returns extension of file.
/*!
 * \param path Path of file
 */
Stroka GetFileExtension(const Stroka& path);

//! Returns name of file without extension.
/*!
 * \param path Path of file
 */
Stroka GetFileNameWithoutExtension(const Stroka& path);

//! Deletes all files with extension #TempFileSuffix in directory.
/*!
 * \param path Directory name
 */
void CleanTempFiles(const Stroka& location);

//! Returns available space at #path.
//! Throws an exception if something went wrong.
i64 GetAvailableSpace(const Stroka& path);

//! Creates the #path and parent directories if they don't exists.
//! Throws an exception if something went wrong.
//! Actually a call of the same named function from util/folder/dirut.
void ForcePath(const Stroka& path, int mode = 0777);

//! Returns size of a file.
//! Throws an exception if something went wrong.
/*!
 * \param path Path of file
 */
i64 GetFileSize(const Stroka& path);

////////////////////////////////////////////////////////////////////////////////

} // namespace NFS
} // namespace NYT
