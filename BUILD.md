## Building YTsaurus from sources

#### Build Requirements
 We have tested YTsaurus builds using Ubuntu 18.04 and 20.04. Other Linux distributions are likely to work, but additional effort may be needed. Only x86_64 Linux is currently supported.

 Below is a list of packages that need to be installed before building YTsaurus. 'How to Build' section contains step by step instructions to obtain these packages.

 - cmake 3.22+
 - clang-16
 - lld-16
 - lldb-16
 - conan 2.4.1
 - git 2.20+
 - python 3.8+
 - pip3
 - ninja 1.10+
 - m4
 - libidn11-dev
 - protoc
 - unzip

#### How to Build

 1. Add repositories for dependencies.

    Note: the following repositories are required for the most of Linux distributions. You may skip this step if your GNU/Linux distribution has all required packages in their default repository.
    For more information please read your distribution documentation and https://apt.llvm.org as well as https://apt.kitware.com/
    ```
    curl -s https://apt.llvm.org/llvm-snapshot.gpg.key | sudo apt-key add
    curl -s https://apt.kitware.com/keys/kitware-archive-latest.asc | gpg --dearmor - | sudo tee /usr/share/keyrings/kitware-archive-keyring.gpg >/dev/null
    echo "deb http://apt.llvm.org/$(lsb_release -cs)/ llvm-toolchain-$(lsb_release -cs)-16 main" | sudo tee /etc/apt/sources.list.d/llvm.list >/dev/null
    echo "deb [signed-by=/usr/share/keyrings/kitware-archive-keyring.gpg] https://apt.kitware.com/ubuntu/ $(lsb_release -cs) main" | sudo tee /etc/apt/sources.list.d/kitware.list >/dev/null
    sudo add-apt-repository -y ppa:ubuntu-toolchain-r/test

    sudo apt-get update
    ```

 1. Install python of version >= 3.8 

    Note: this example is taken from https://www.build-python-from-source.com/ and checked on Ubuntu 18.04

    ```
    cd /tmp/
    wget https://www.python.org/ftp/python/3.11.4/Python-3.11.4.tgz
    tar xzf Python-3.11.4.tgz
    cd Python-3.11.4
    
    sudo ./configure --prefix=/usr --enable-optimizations --with-lto --with-computed-gotos --with-system-ffi
    sudo make -j "$(nproc)"
    sudo make altinstall
    sudo rm /tmp/Python-3.11.4.tgz
    ```

 1. Install dependencies.

    ```
    sudo apt-get install -y python3-pip ninja-build libidn11-dev m4 clang-16 lld-16 cmake unzip
    sudo python3 -m pip install PyYAML==6.0.1 conan==2.4.1 dacite
    ```
 1. Install protoc.

    ```
    curl -sL -o protoc.zip https://github.com/protocolbuffers/protobuf/releases/download/v3.20.1/protoc-3.20.1-linux-x86_64.zip
    sudo unzip protoc.zip -d /usr/local
    rm protoc.zip
    ```

 1. Create the work directory. Please make sure you have at least 80Gb of free space. We also recommend placing this directory on SSD to reduce build times.
    ```
    mkdir ~/ytsaurus && cd ~/ytsaurus
    mkdir build
    ```

 1. Clone the YTsaurus repository.
    ```
    git clone https://github.com/ytsaurus/ytsaurus.git
    ```

 1. Build YTsaurus.

    Run cmake to generate build configuration:

    ```
    cd build
    cmake -G Ninja -DCMAKE_BUILD_TYPE=Release -DREQUIRED_LLVM_TOOLING_VERSION=16 -DCMAKE_TOOLCHAIN_FILE=../ytsaurus/clang.toolchain -DCMAKE_PROJECT_TOP_LEVEL_INCLUDES=../ytsaurus/cmake/conan_provider.cmake ../ytsaurus
    ```

    To build just run:
    ```
    ninja
    ```

    If you need to build concrete target you can run:
    ```
    ninja <target>
    ```

    A YTsaurus server binary can be found at:
    ```
    yt/yt/server/all/ytserver-all
    ```
