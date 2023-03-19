class Container(object):
    def __init__(self, api, name):
        self.api = api
        self.name = name

    def __str__(self):
        return self.name

    def __repr__(self):
        return 'Container `{}`'.format(self.name)

    def __div__(self, child):
        if self.name == "/":
            return Container(self.api, child)
        return Container(self.api, self.name + "/" + child)

    def __getitem__(self, prop):
        return self.api.GetProperty(self.name, prop)

    def __setitem__(self, prop, value):
        return self.api.SetProperty(self.name, prop, value)

    def Start(self):
        self.api.Start(self.name)

    def Stop(self, timeout=None):
        self.api.Stop(self.name, timeout)

    def Kill(self, sig):
        self.api.Kill(self.name, sig)

    def Pause(self):
        self.api.Pause(self.name)

    def Resume(self):
        self.api.Resume(self.name)

    def Get(self, variables, nonblock=False, sync=False):
        return self.api.Get([self.name], variables, nonblock, sync)[self.name]

    def Set(self, **kwargs):
        self.api.Set(self.name, **kwargs)

    def GetProperties(self):
        return self.Get(self.api.Plist())

    def GetProperty(self, prop, index=None, sync=False):
        return self.api.GetProperty(self.name, prop, index=index, sync=sync)

    def SetProperty(self, prop, value, index=None):
        self.api.SetProperty(self.name, prop, value, index=index)

    def GetInt(self, prop, index=None):
        return self.api.GetInt(self.name, prop, index)

    def SetInt(self, prop, value, index=None):
        self.api.SetInt(self.name, prop, value, index)

    def GetMap(self, prop, conv=int):
        return self.api.GetMap(self.name, prop, conv)

    def SetMap(self, prop, value):
        self.api.SetMap(self.name, prop, value)

    def GetLabel(self, label):
        return self.api.GetLabel(self.name, label)

    def SetLabel(self, label, value, prev_value=None):
        self.api.SetLabel(self.name, label, value, prev_value)

    def IncLabel(self, label, add=1):
        return self.api.IncLabel(self.name, label, add)

    def GetData(self, data, sync=False):
        return self.api.GetData(self.name, data, sync)

    def SetSymlink(self, symlink, target):
        return self.api.SetSymlink(self.name, symlink, target)

    def WaitContainer(self, timeout=None):
        return self.api.WaitContainers([self.name], timeout=timeout)

    def Wait(self, *args, **kwargs):
        """legacy compat - timeout in ms"""
        return self.api.Wait([self.name], *args, **kwargs)

    def Destroy(self):
        self.api.Destroy(self.name)

    def ListVolumeLinks(self):
        return self.api.ListVolumeLinks(container=self)

    def Dump(self):
        return self.api.Dump(container=self)

    def LoadSpec(self, new_spec):
        return self.api.LoadSpec(container=self, new_spec=new_spec)

    def AttachProcess(self, pid, comm=""):
        self.api.AttachProcess(self.name, pid, comm)

    def AttachThread(self, pid, comm=""):
        self.api.AttachThread(self.name, pid, comm)
