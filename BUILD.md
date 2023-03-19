## Building YTsaurus from sources

#### Build Requirements
 We have tested YTsaurus builds using Ubuntu 18.04 and 20.04. Other Linux distributions are likely to work, but additional effort may be needed. Only x86_64 Linux is currently supported.

 Below is a list of packages that need to be installed before building YTsaurus. 'How to Build' section contains step by step instructions to obtain these packages.

 - cmake 3.22+
 - clang-12
 - lld-12
 - lldb-12
 - conan 1.57.0
 - git 2.20+
 - python3.8
 - pip3
 - ninja 1.10+
 - m4
 - libidn11-dev
 - protoc

#### How to Build

 1. Add repositories for dependencies.

    Note: the following repositories are required for the most of Linux distributions. You may skip this step if your GNU/Linux distribution has all required packages in their default repository.
    For more information please read your distribution documentation and https://apt.llvm.org as well as https://apt.kitware.com/
    ```
    curl -s https://apt.llvm.org/llvm-snapshot.gpg.key | sudo apt-key add
    curl -s https://apt.kitware.com/keys/kitware-archive-latest.asc | gpg --dearmor - | sudo tee /usr/share/keyrings/kitware-archive-keyring.gpg
    echo 'deb http://apt.llvm.org/$(lsb_release -cs)/ llvm-toolchain-$(lsb_release -cs)-12 main' | sudo tee /etc/apt/sources.list.d/llvm.list
    echo 'deb [signed-by=/usr/share/keyrings/kitware-archive-keyring.gpg] https://apt.kitware.com/ubuntu/ $(lsb_release -cs) main' | sudo tee /etc/apt/sources.list.d/kitware.list

    sudo apt-get update
    ```
 
 1. Install python (for Ubuntu 18.04).
   
    ```
    sudo add-apt-repository ppa:deadsnakes/ppa
    sudo apt-get update
    sudo apt-get install -y python3.11 python3.11-dev python3.11-distutils python3.11-venv
    sudo update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.11 1
    ```

 1. Install dependencies.

    ```
    sudo apt-get install -y python3-pip ninja-build libidn11-dev m4 clang-12 lld-12 cmake
    sudo python3 -m pip install PyYAML==6.0 conan==1.57.0 dacite
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
    cmake -G Ninja -DCMAKE_BUILD_TYPE=Release -DCMAKE_TOOLCHAIN_FILE=../ytsaurus/clang.toolchain ../ytsaurus
    ```

    To build just run:
    ```
    ninja
    ```

    A YTsaurus server binary can be found at:
    ```
    yt/yt/server/all/ytserver-all
    ```
