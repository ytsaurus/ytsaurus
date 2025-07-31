`tar_archive` is a simple C++ wrapper around C library [libarchive](https://a.yandex-team.ru/arc/trunk/arcadia/contrib/libs/libarchive). It contains two headers which you can include.

## `archive_iterator.h`
Provides iterator to read archive data.

Usage:
```cpp
TArchiveIterator arch(path);
for (const auto& file : arch) {
    if (file.IsRegular()) {
        Cout << file.GetPath()
             << ": "
             << file.GetStream().ReadAll()
             << "\n";
    }
}
```
If archive packed with standart tar utility, order of files will be like this
```
a
b
b/c
b/d
b/d/e
f
```
So all files and directories (even empty directories) listed in sorted order

However, something like this is valid too
```
b/c
b/d
b/d/e
f
b/d
a
```
## `archive_writer.h`
Allows you to create archives. You can specify type, compression and encoding. By default it would use file extension to guess type, if filename has no extension or unknown extension, it would use tar.

Usage:
```cpp
TArchiveWriter writer(path);

// You can write file from TBlob
writer.WriteFile(file_path, blob);

// Or via stream (exactly file_size bytes are expected, otherwise exception will be thrown and this object will be spoiled)
writer.WriteFileFrom(file_path, file_size, input_stream);
```
## Note
Some metadata (like file birthtime and access rights) currently not supported by this library.
