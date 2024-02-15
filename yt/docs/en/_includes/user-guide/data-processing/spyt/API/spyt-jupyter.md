# SPYT in Jupyter

## Setup { #prepare }

Before you can use Spark in Jupyter, you need to create a [cluster](../../../../../user-guide/data-processing/spyt/cluster/cluster-start.md). Currently working with Spark using Jupyter notebooks is possible only using inner standalone cluster.

If there is one already, you need to find out the values of `proxy` and `discovery_path` to be able to use it.

## Configuring Jupyter { #custom }

1. Get network access from your Jupyter machine to the SPYT cluster, ports `27000-27200`.
2. Get network access from the SPYT cluster to the Jupyter machine, ports `27000-27200`.
3. Install a deb package containing java:
   ```bash
   sudo apt-get update
   sudo apt-get install openjdk-11-jdk

   ```
4. Install the pip package:

   ```bash
   pip install ytsaurus-spyt

   ```
5. Place your {{product-name}} token in `~/.yt/token`:
   ```bash
   mkdir ~/.yt
   cat <<EOT > ~/.yt/token
   $YOUR_YT_TOKEN
   EOT
   ```
6. Place a file called `~/spyt.yaml` with the Spark cluster location in your home directory:

   ```bash
   cat <<EOT > ~/spyt.yaml
   yt_proxy: "cluster_name"
   discovery_path: "$YOUR_DISCOVERY_DIR"
   EOT
   ```

## Updating the client in Jupyter { #new-client }

Update `ytsaurus-spyt` in Jupyter:

```bash
pip install ytsaurus-spyt
```
If the second `ytsaurus-spyt` version component is greater than that in your cluster version, new functionality may not work. Update your cluster per the instructions.




