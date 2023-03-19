# Slow job troubleshooting

This section provides a description of using the `Job Shell` tool to troubleshoot slow jobs.

On occasion, one or more operation jobs may run for a long time without visible progress. To figure out the reason, you can use the `Job Shell` tool. You can use this tool to open a console directly from the job environment on behalf of the same user the job is running under. Thereafter, you can explore the running job with calls to **gdb** or **strace**. The recommended work algorithm is shown in the figure:

## Starting Job Shell

To launch Job Shell, you need to run `yt --proxy <cluster name> run-job-shell <job id>`, substituting a cluster name or a job ID for `<...>`. You can find out the job ID on the **Jobs** tab of the operation page.

Detailed description of Job Shell launch:

```bash
yt --proxy cluster-name run-job-shell fcebe6b9-e06ab2e1-3fe0384-d0b66453
Use ^F to terminate shell.

Job environment:
TMPDIR=/yt/disk2/cluster-data/slots/11/sandbox
PYTHONPATH=/yt/disk2/cluster-data/slots/11/sandbox
PYTHONUSERBASE=/yt/disk2/cluster-data/slots/11/sandbox/.python-site-packages
PYTHON_EGG_CACHE=/yt/disk2/cluster-data/slots/11/sandbox/.python-eggs
HOME=/yt/disk2/cluster-data/slots/11/sandbox
YT_OPERATION_ID=78b048b5-8055fabe-3fe03e8-d5b74b2a
YT_JOB_INDEX=120
YT_JOB_ID=fcebe6b9-e06ab2e1-3fe0384-d0b66453
YT_START_ROW_INDEX=202133578

UID          PID    PPID  C STIME TTY          TIME CMD
19911     257386  239675  0 02:24 pts/1    00:00:00 /bin/bash
19911     257397  257386  0 02:24 pts/1    00:00:00  \_ ps -fu 19911 --forest
19911     240043  239675  0 02:21 ?        00:00:00 /bin/bash -c :; ./my_mapper
19911     240073  240043 91 02:21 ?        00:02:37  \_ ./my_mapper

yt_slot_11@n1757-sas:~$
```

Command output displays all kinds of job information:

- Path to the job's working directory: /yt/disk2/cluster-data/slots/11/sandbox.
- Name of user job is running under: yt_slot_11.
- List of processes running under current user.

The first and the second processes in the example belong to Job Shell, the third process is a bash command-line shell session that the job in question is launched in, and the forth is the user process itself.

## User process analysis

The first thing to do is to execute the `top -p <PID>` command passing in the user process ID as an argument. Below is an example and a call output:

```bash
yt_slot_11@n1757-sas:~$ top -p 240073
top - 02:30:52 up 6 days,  7:11,  0 users,  load average: 41.26, 37.59, 39.23
Tasks:   1 total,   1 running,   0 sleeping,   0 stopped,   0 zombie
Cpu(s): 48.9%us,  5.6%sy,  0.0%ni, 31.1%id, 14.3%wa,  0.0%hi,  0.2%si,  0.0%st
Mem:  132001796k total, 130937776k used,  1064020k free,   296488k buffers
Swap:        0k total,        0k used,        0k free, 107424768k cached

    PID USER      PR  NI  VIRT  RES  SHR S %CPU %MEM    TIME+  COMMAND
 240073 yt_slot_  20   0  4164  632  552 R   98  0.0   8:08.05 my_mapper
```

From CPU usage in this example (48.9%), it is obvious that the user process is performing computations, and the reason for the job's slow performance may be in the user code. You should connect to the job process using **gdb** and try and figure out what it is doing at the moment.

If CPU usage is close to zero, this most likely means that the process is frozen in a system call. You should call **strace** on the user process.
Example call:

```bash
yt_slot_11@n1757-sas:~$ strace -p 240073
Process 240073 attached - interrupt to quit
write(4
```

If the process is making system calls all the time (rather than freezing in a single one), be prepared for the stream of information that the above command will output to the console. In this situation, this syntax may prove useful: ``strace -p <PID> 2>&1 | head``. This helps you see a single call.

In addition, if your code is multi-treaded, it might be useful to add the ``-f`` option that helps capture system calls from all threads.

If a process hangs up on a read or **write** system call, you should look at the value of the first argument, which is the file descriptor of the thread. The value of zero for the **read** system call and a small integer for the **write** system call (1, 4, 7) means that for some reason, the {{product-name}} Job Proxy service process is not releasing or accepting the next batch of data to or from the user process. In this situation, you need to advise the system administrator of the problem enclosing detailed information on the troubleshooting you have done. For more information on troubleshooting, see [How to report](../../../user-guide/problems/howtoreport.md).

In the event of system call hang-ups on **futex**, **connect**, **open**, **stat**, and others, the cause is not related to the operation of `Job Proxy` or the {{product-name}} system overall. You should look for the problem inside user code. You need to use **gdb** to figure out what is causing the block. If the user code is multi-threaded, for instance, and blockage occurs for some reason, **strace** output could look like this:

```bash
yt_slot_11@n1757-sas:~$ strace -p 240073 2>&1 | head
Process 240073 attached - interrupt to quit
futex(0x7ffff79b3e00, FUTEX_WAIT, 2, NULL
```

It is possible that the user process is not consuming any CPU, and **strace** is not showing any system calls. This means that the code is using **memory mapping** and reads from (or writes to) a file on disk. This results in a **major page fault**, and the process waits while the operating system uploads data from disk into memory.

{% note warning "Attention!" %}

The use of **memory mapping** in jobs is **strongly discouraged** while jobs doing this create heavy disk load with random reads and writes. Such jobs are recognizable from the high values of the [statistics](../../../user-guide/problems/jobstatistics.md) for `/user_job/block_io/[io_read,io_write]`. Please remember that a conventional hard drive is capable of no more than 100 random access operations per second while there are several dozen jobs running concurrently on a cluster node.

{% endnote %}

A specific example of a **memory mapped** file read by a job is an operation executable and its dependencies dynamically loaded by a linker as the operation is launched. The loader code is normally written to prevent the resulting **page faults** from causing problems. You should remember that reducing the executable and dependency size always has a salutary effect on performance. In particular, from the performance standpoint, it is better to use executables without debug symbols.

## Job Shell execute privilege

To run Job Shell, you need to have the operation **read** and **manage** privileges.

To get these, you need to include in the operation spec an [ACL](../../../user-guide/storage/access-control.md) describing users and groups that are authorized to manipulate the operation and jobs when launching an operation.
By default, the ACL list is empty, and operation privileges are only issued to the user that launched the operation as well as the {{product-name}} system administrators.

Example ACL configuration in Python:
```python
spec={"acl": [{"permissions": ["read", "manage"], "subjects": ["user1", "user2"], "action": "allow"}]}
```
The user from the YT_TOKEN/YT_TOKEN_PATH (authenticated_user) environment variables is added automatically.

To modify the access rights of an already running operation, you need to run the `update_operation_parameters` command with a new ACL.
