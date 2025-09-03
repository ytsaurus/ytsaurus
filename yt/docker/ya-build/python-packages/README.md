# Building the ytsaurus client python packages

This configuration allows building the ytsaurus client python packages from source.

Example:
```
./ya package --docker-registry=localhost --custom-version=latest yt/docker/ya-build/python-packages/package.json
docker create --name ytsaurus-python-packages localhost/ytsaurus-python-packages:latest true
docker export ytsaurus-python-packages | tar -xv --wildcards '*.whl'
docker rm ytsaurus-python-packages
```

### Debian multi-arch systems

Currently, [multi-arch](https://wiki.debian.org/Multiarch/Implementation) systems and `USE_LOCAL_PYTHON=yes` in `ya make` do not work together well.
Header `/usr/include/python{X}/pyconfig.h` cannot include `{ARCH}/python{X}/pyconfig.h` without `-I/usr/include`.

To fix add symlink `/usr/include/python{X}/{ARCH}/python{X}` -> `/usr/include/{ARCH}/python{X}`.

### Cross-compilation

Add apt repo `/etc/apt/sources.list.d/ubuntu-arm64.sources`
```
Types: deb
URIs: http://ports.ubuntu.com/ubuntu-ports/
Suites: ${DISTRIB_CODENAME} ${DISTRIB_CODENAME}-updates ${DISTRIB_CODENAME}-backports ${DISTRIB_CODENAME}-security
Components: main universe restricted multiverse
Signed-By: /usr/share/keyrings/ubuntu-archive-keyring.gpg
Architectures: arm64
```

Add architecture:
`dpkg --add-architecture arm64`

Install python:
`apt install libpython3-dev:arm64`

To build docker image for foreign architecture install binfmt-misc handler:
`apt install qemu-user-static`

Add symlinks for python headers:
```
python_include_dir=$(python3-config --includes | sed -n 's/^-I\(\S\+\) .*/\1/p')
for arch in amd64 arm64 ; do
    python_basename=$(basename $python_include_dir)
    triplet=$(dpkg-architecture -a $arch -q DEB_HOST_MULTIARCH)
    mkdir -p ${python_include_dir}/${triplet}
    ln -s ../../${triplet}/${python_basename} ${python_include_dir}/${triplet}/${python_basename}
done
```
