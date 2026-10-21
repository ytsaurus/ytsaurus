Running operations on the cluster via the Python SDK can be tricky because you deliver the code to the cluster along with the script’s runtime dependencies (from your local machine). If the differences in OS type, OS version, or Python version are significant, the code won’t run.

One way to solve this problem is to ensure that the OS and Python versions match between the launch point (your local machine) and the execution point (the cluster). You do this by running your local code inside a Docker container with the correct environment. This approach lets you run scripts from different OS setups, doesn’t require CRI support on the cluster, and doesn’t require you to push Docker images to any registry.

To do this:

#### 0. Set up Docker

- For ARM platforms (Mac M1/M2), you need to run Docker in x86 mode.

```
# Install Rosetta to support x86
softwareupdate --install-rosetta --agree-to-license

# If you’re using Colima, you need to restart it:
colima stop
colima start --arch x86_64 --vm-type=vz --vz-rosetta
```

- If your cluster uses IPv6 addresses, you need to configure their routing from the container.  
See https://docs.docker.com/engine/daemon/ipv6/  
Note: Colima doesn’t support IPv6.

#### 1. Create a Docker image that matches the cluster settings

```(bash)
# Build a Docker image for the target cluster (the OS and Python version in the image will match the cluster)
yt devtools image prepare --proxy <my_cluster.fqdn> --with-modules ytsaurus-client ytsaurus-yson
```

If the Python or OS version on the cluster is too old and blocks installing the dependencies you need, you can specify the version you want (this requires CRI support on the cluster).

```(bash)
# Check the OS/Python version on the cluster
yt devtools image get-cluster-env --proxy <my_cluster.fqdn>
# Build a Docker image with the desired OS (for example, Ubuntu Trusty) and Python
yt devtools image prepare --proxy <my_cluster.fqdn> --base-image docker.io/library/ubuntu:trusty --python 3.13
```

Make sure you use the exact same `proxy` for all commands; the resulting image name is derived from it (`cluster_env_<current_user>_<cluster_name>:latest`). You can always override this behavior with the `--image-name <my_specific_image_name>` parameter.

You can view the list of all images with this command:

```(bash)
yt devtools image list
```

#### 2. Install additional packages in the image

You can install missing packages in the image at any time.

```(bash)
# Install pip packages
yt devtools image install --proxy <my_cluster.fqdn> --pip <package_1> <package_2>
# Or run a custom command
yt devtools image install --proxy <my_cluster.fqdn> --bash '<my_command_with_spaces>'
```

#### 3. Run the script

If needed, set the environment variables:

```
export YT_TOKEN=...
export YT_TOKEN_PATH=...
export YT_PROXY=my_cluster.fqdn
```

Then run the script:

```
yt devtools image run --proxy <my_cluster.fqdn> <path_to_my_script.py>

# Or manually:
# docker run --platform linux/amd64 --rm -e USER=$USER -e YT_TOKEN_PATH="${YT_TOKEN_PATH:-$HOME/.yt/token}" -e YT_TOKEN="$YT_TOKEN" -e YT_PROXY="$YT_PROXY" -v $HOME:$HOME -it my_cluster_image:latest bash -ic "python $PWD/my_script.py"
```