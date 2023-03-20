Run local YTsaurus in docker
====================================

First steps
-----------
1. Install Docker (instructions for
[Ubuntu](https://docs.docker.com/install/linux/docker-ce/ubuntu/),
[Mac](https://docs.docker.com/docker-for-mac/install/),
[Windows](https://docs.docker.com/docker-for-windows/install/))

2. Run the script `run_local_cluster.sh` without parameters
3. Wait for the images to download (they are **large**) and follow the instructions of the script.

Minimum system requirements
--------------------------------
It is assumed that you have one of the versions of Ubuntu or MacOSX installed, there should be no problems with them. Running the automated installation [script](https://github.com/ytsaurus/ytsaurus/blob/main/yt/docker/local/run_local_cluster.sh) might require some effort.

The easiest way to start
-------------------------
The recommended way to start is to use the installation [script](https://github.com/ytsaurus/ytsaurus/blob/main/yt/docker/local/run_local_cluster.sh).
When it is run without arguments, the latest versions of containers are downloaded and launched. To interact with the web interface and the backend proxy node container ports are forwarded to ports 8001 and 8000 on the host machine, respectively.
After successful launch, the script outputs a message to the console with the address of the web interface and an example of launching the yt console utility. The containers can be shut down with the command `./run_local_cluster.sh --stop` when you are finished.
The method above works when running YTsaurus on your local machine. When running containers on a remote machine, you will need to specify several extra arguments, see below.

Advanced launch method
--------------------------
The script supports the following arguments, in descending order of usefulness:
* --local-cypress-dir - directory on the host machine where the directories and table files that will be mounted into the cypress root of the local cluster at startup.
This directory is mounted in read-write mode, so any changes made to the local cluster will be saved to this directory and visible even after the containers have finished running.
* --proxy-port -- the port on the host machine on which the proxy node of the cluster is available. The default value is 8000 and can be changed if the port is already in use.
* --interface-port -- the port on the host machine through which the web server with local cluster interface will be available. The default value is 8001 and can be changed if the port is already in use.
* --docker-hostname -- the name of the host running the docker daemon that deploys the containers. The web interface is exposed on this host and sends request to the proxy node on this same host. The default is localhost, but it can be set to the name of the remote machine if the containers are running on a remote machine.
* --ui-version -- web interface docker image version, stable tag is used by default. Do not change this value unless you have a good reason to do so.
* --yt-version -- cluster docker image version, latest tag is used by default. Do not change this value unless you have a good reason to do so.

Rebuild docker image
--------------------------
If you want to rebuild the docker image, you need to:
1. Build `ytserver-all` binary, see [instruction](https://github.com/ytsaurus/ytsaurus/blob/main/BUILD.md);
2. Run `./build.sh` --ytserver-all <YTSERVER_ALL_PATH>
3. Run `./run_local_cluster.sh` --yt-skip-pull true
