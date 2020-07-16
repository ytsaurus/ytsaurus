# YT protobufs

This directory contains go libraries for YT protobuf. Because YT builds from
two different paths inside repository, it is impossible to use original ya.make files.

Some messages might be missing, either because they are not needed or impossible to build
due to cyclic imports.

One solution to all these problems would be to dump all .proto files into a single go package,
but we choose clean approach

Directory and package structure must mirror that of yt/19_4/yt.
