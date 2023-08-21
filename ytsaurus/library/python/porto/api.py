import os
import socket
import threading
import warnings
import re

import library.cpp.porto.proto.rpc_pb2 as rpc
from . import exceptions
from .container import Container
from .volume import Layer, Storage, MetaStorage, VolumeLink, Volume


class Property(object):
    def __init__(self, name, desc, read_only, dynamic):
        self.name = name
        self.desc = desc
        self.read_only = read_only
        self.dynamic = dynamic

    def __str__(self):
        return self.name

    def __repr__(self):
        return 'Property `{}` `{}`'.format(self.name, self.desc)


class PortoApi(object):
    def __init__(self,
                 socket_path='/run/portod.socket',
                 timeout=300,
                 disk_timeout=900,
                 socket_constructor=socket.socket,
                 lock_constructor=threading.Lock,
                 auto_reconnect=True):
        self.lock = lock_constructor()
        self.socket_constructor = socket_constructor
        self.socket_path = socket_path
        self.sock = None
        self.sock_pid = None
        self.timeout = timeout
        self.disk_timeout = disk_timeout
        self.auto_reconnect = auto_reconnect
        self.async_wait_names = []
        self.async_wait_callback = None
        self.async_wait_timeout = None

    def _set_timeout(self, extra_timeout=0):
        if extra_timeout is None:
            self.sock.settimeout(None)
        else:
            self.sock.settimeout(self.timeout + extra_timeout)

    def _connect(self, timeout=None):
        try:
            SOCK_CLOEXEC = 0o2000000
            self.sock = self.socket_constructor(socket.AF_UNIX, socket.SOCK_STREAM | SOCK_CLOEXEC)
            self._set_timeout(0 if timeout is None else timeout - self.timeout)
            self.sock.connect(self.socket_path)
            if timeout is not None:
                self._set_timeout()
        except socket.timeout as e:
            self.sock = None
            raise exceptions.SocketTimeout("Porto connection timeout: {}".format(e))
        except socket.error as e:
            self.sock = None
            raise exceptions.SocketError("Porto connection error: {}".format(e))

        self.sock_pid = os.getpid()
        self._resend_async_wait()

    def _encode_message(self, msg, val, key=None):
        msg.SetInParent()
        if isinstance(val, dict):
            if key is not None:
                msg = getattr(msg, key)
            if hasattr(msg, 'keys'):
                msg.update(val)
            else:
                msg.SetInParent()
                for k, v in val.items():
                    self._encode_message(msg, v, k)
        elif isinstance(val, list):
            if key is not None:
                msg = getattr(msg, key)
            if isinstance(val[0], dict):
                for item in val:
                    self._encode_message(msg.add(), item)
            else:
                msg.extend(val)
        else:
            setattr(msg, key, val)

    def _decode_message(self, msg):
        ret = dict()
        for dsc, val in msg.ListFields():
            key = dsc.name
            if hasattr(val, 'keys'):
                ret[key] = dict(val)
            elif dsc.type == dsc.TYPE_MESSAGE:
                if dsc.label == dsc.LABEL_REPEATED:
                    ret[key] = [self._decode_message(v) for v in val]
                else:
                    ret[key] = self._decode_message(val)
            elif dsc.label == dsc.LABEL_REPEATED:
                ret[key] = list(val)
            else:
                ret[key] = val
        return ret

    def _encode_request(self, request):
        req = request.SerializeToString()
        length = len(req)
        hdr = bytearray()
        while length > 0x7f:
            hdr.append(0x80 | (length & 0x7f))
            length >>= 7
        hdr.append(length)
        return hdr + req

    def _recv_data(self, count):
        msg = bytearray()
        while len(msg) < count:
            try:
                chunk = self.sock.recv(count - len(msg))
            except socket.error as e:
                if e.errno == socket.errno.EINTR:
                    continue
                else:
                    raise
            if not chunk:
                raise socket.error(socket.errno.ECONNRESET, os.strerror(socket.errno.ECONNRESET))
            msg.extend(chunk)
        return msg

    def _recv_response(self):
        rsp = rpc.TPortoResponse()
        while True:
            length = shift = 0
            while True:
                b = self._recv_data(1)
                length |= (b[0] & 0x7f) << shift
                shift += 7
                if b[0] <= 0x7f:
                    break

            rsp.ParseFromString(bytes(self._recv_data(length)))

            if rsp.HasField('AsyncWait'):
                if self.async_wait_callback is not None:
                    if rsp.AsyncWait.HasField("label"):
                        self.async_wait_callback(name=rsp.AsyncWait.name, state=rsp.AsyncWait.state, when=rsp.AsyncWait.when, label=rsp.AsyncWait.label, value=rsp.AsyncWait.value)
                    else:
                        self.async_wait_callback(name=rsp.AsyncWait.name, state=rsp.AsyncWait.state, when=rsp.AsyncWait.when)
            else:
                return rsp

    def _sendall(self, data):
        while True:
            try:
                self.sock.sendall(data)
                return
            except socket.error as e:
                if e.errno == socket.errno.EINTR:
                    continue
                else:
                    raise

    def _call(self, request, extra_timeout=0):
        req = self._encode_request(request)

        with self.lock:
            if self.sock is None:
                if self.auto_reconnect:
                    self._connect()
                else:
                    raise exceptions.SocketError("Porto socket is not connected")
            elif self.sock_pid != os.getpid():
                if self.auto_reconnect:
                    self._connect()
                else:
                    self.sock = None
                    raise exceptions.SocketError("Porto socket connected by other pid {}".format(self.sock_pid))
            elif self.auto_reconnect:
                try:
                    self._sendall(req)
                    req = None
                except socket.timeout as e:
                    self.sock = None
                    raise exceptions.SocketTimeout("Porto connection timeout: {}".format(e))
                except socket.error:
                    self._connect()

            try:
                if req is not None:
                    self._sendall(req)

                if extra_timeout is None or extra_timeout > 0:
                    self._set_timeout(extra_timeout)

                response = self._recv_response()

                if extra_timeout is None or extra_timeout > 0:
                    self._set_timeout()

            except socket.timeout as e:
                self.sock = None
                raise exceptions.SocketTimeout("Porto connection timeout: {}".format(e))
            except socket.error as e:
                self.sock = None
                raise exceptions.SocketError("Socket error: {}".format(e))

        if response.error != rpc.Success:
            raise exceptions.PortoException.Create(response.error, response.errorMsg)

        return response

    def _resend_async_wait(self):
        if not self.async_wait_names:
            return

        request = rpc.TPortoRequest()
        request.AsyncWait.name.extend(self.async_wait_names)
        if self.async_wait_timeout is not None:
            request.AsyncWait.timeout_ms = int(self.async_wait_timeout * 1000)

        self._sendall(self._encode_request(request))
        response = self._recv_response()
        if response.error != rpc.Success:
            raise exceptions.PortoException.Create(response.error, response.errorMsg)

    def Connect(self, timeout=None):
        with self.lock:
            self._connect(timeout)

    def Disconnect(self):
        with self.lock:
            if self.sock is not None:
                self.sock.close()
                self.sock = None

    def Connected(self):
        with self.lock:
            return self.sock is not None

    def TryConnect(self, timeout=None):
        """compat"""
        warnings.warn("use Connect", DeprecationWarning)
        self.Connect(timeout)

    def connect(self, timeout=None):
        """compat"""
        warnings.warn("use Connect", DeprecationWarning)
        self.Connect(timeout)

    def disconnect(self):
        """compat"""
        warnings.warn("use Disconnect", DeprecationWarning)
        self.Disconnect()

    def GetTimeout(self):
        return self.timeout

    def SetTimeout(self, timeout):
        with self.lock:
            self.timeout = timeout
            if self.sock is not None:
                self._set_timeout()

    def GetDiskTimeout(self):
        return self.disk_timeout

    def SetDiskTimeout(self, disk_timeout):
        self.disk_timeout = disk_timeout

    def SetAutoReconnect(self, auto_reconnect):
        self.auto_reconnect = auto_reconnect

    def Call(self, command_name, response_name=None, extra_timeout=0, **kwargs):
        req = rpc.TPortoRequest()
        cmd = getattr(req, command_name)
        cmd.SetInParent()
        self._encode_message(cmd, kwargs)
        rsp = self._call(req, extra_timeout)
        if hasattr(rsp, response_name or command_name):
            return self._decode_message(getattr(rsp, response_name or command_name))
        return None

    def List(self, mask=None):
        request = rpc.TPortoRequest()
        request.List.SetInParent()
        if mask is not None:
            request.List.mask = mask
        return self._call(request).List.name

    def ListContainers(self, mask=None):
        return [Container(self, name) for name in self.List(mask)]

    def FindLabel(self, label, mask=None, state=None, value=None):
        request = rpc.TPortoRequest()
        request.FindLabel.label = label
        if mask is not None:
            request.FindLabel.mask = mask
        if state is not None:
            request.FindLabel.state = state
        if value is not None:
            request.FindLabel.value = value
        list = self._call(request).FindLabel.list
        return [{'name': ll.name, 'state': ll.state, 'label': ll.label, 'value': ll.value} for ll in list]

    def Find(self, name):
        self.GetProperty(name, "state")
        return Container(self, name)

    def Create(self, name, weak=False):
        request = rpc.TPortoRequest()
        if weak:
            request.CreateWeak.name = name
        else:
            request.Create.name = name
        self._call(request)
        return Container(self, name)

    def GetContainersSpecs(self, names):
        request = rpc.TPortoRequest()
        for name in names:
            filter = request.ListContainersBy.filters.add()
            filter.name = name
        resp = self._call(request)
        return resp.ListContainersBy.containers

    def SetSpec(self, spec):
        request = rpc.TPortoRequest()
        request.UpdateFromSpec.container.CopyFrom(spec)
        self._call(request)

    def CreateSpec(self, container, volume=None, start=False):
        request = rpc.TPortoRequest()
        if container:
            request.CreateFromSpec.container.CopyFrom(container)
        if volume:
            request.CreateFromSpec.volume.CopyFrom(volume)
        request.CreateFromSpec.start = start
        self._call(request)
        return Container(self, container.name)

    def CreateWeakContainer(self, name):
        return self.Create(name, weak=True)

    def Run(self, name, weak=True, start=True, wait=0, root_volume=None, private_value=None, **kwargs):
        ct = self.Create(name, weak=True)
        try:
            ct.Set(**kwargs)
            if private_value is not None:
                ct.SetProperty('private', private_value)
            if root_volume is not None:
                root = self.CreateVolume(containers=name, **root_volume)
                ct.SetProperty('root', root.path)
            if start:
                ct.Start()
            if not weak:
                ct.SetProperty('weak', False)
            if wait != 0:
                ct.WaitContainer(wait)
        except exceptions.PortoException as e:
            try:
                ct.Destroy()
            except exceptions.ContainerDoesNotExist:
                pass
            raise e
        return ct

    def Dump(self, container):
        if isinstance(container, Container):
            container = container.name
        spec = self.GetContainersSpecs([container])
        if len(spec) == 0:
            raise exceptions.ContainerDoesNotExist
        return spec[0]

    def LoadSpec(self, container, new_spec):
        if isinstance(container, Container):
            container = container.name
        spec = rpc.TContainerSpec()
        spec.CopyFrom(new_spec)
        spec.name = container
        self.SetSpec(spec)

    def Destroy(self, container):
        if isinstance(container, Container):
            container = container.name
        request = rpc.TPortoRequest()
        request.Destroy.name = container
        self._call(request)

    def Start(self, name, timeout=None):
        request = rpc.TPortoRequest()
        request.Start.name = name
        self._call(request, timeout)

    def Stop(self, name, timeout=None):
        request = rpc.TPortoRequest()
        request.Stop.name = name
        if timeout is not None and timeout >= 0:
            request.Stop.timeout_ms = timeout * 1000
        else:
            timeout = 30
        self._call(request, timeout)

    def Kill(self, name, sig):
        request = rpc.TPortoRequest()
        request.Kill.name = name
        request.Kill.sig = sig
        self._call(request)

    def Pause(self, name):
        request = rpc.TPortoRequest()
        request.Pause.name = name
        self._call(request)

    def Resume(self, name):
        request = rpc.TPortoRequest()
        request.Resume.name = name
        self._call(request)

    def Get(self, containers, variables, nonblock=False, sync=False):
        request = rpc.TPortoRequest()
        request.Get.name.extend(containers)
        request.Get.variable.extend(variables)
        request.Get.sync = sync
        if nonblock:
            request.Get.nonblock = nonblock
        resp = self._call(request)
        res = {}
        for container in resp.Get.list:
            var = {}
            for kv in container.keyval:
                if kv.HasField('error'):
                    var[kv.variable] = exceptions.PortoException.Create(kv.error, kv.errorMsg)
                    continue
                if kv.value == 'false':
                    var[kv.variable] = False
                elif kv.value == 'true':
                    var[kv.variable] = True
                else:
                    var[kv.variable] = kv.value

            res[container.name] = var
        return res

    def GetProperty(self, name, prop, index=None, sync=False, real=False):
        request = rpc.TPortoRequest()
        request.GetProperty.name = name
        if type(prop) is tuple:
            request.GetProperty.property = prop[0] + "[" + prop[1] + "]"
        elif index is not None:
            request.GetProperty.property = prop + "[" + index + "]"
        else:
            request.GetProperty.property = prop
        request.GetProperty.sync = sync
        request.GetProperty.real = real
        res = self._call(request).GetProperty.value
        if res == 'false':
            return False
        elif res == 'true':
            return True
        return res

    def SetProperty(self, name, prop, value, index=None):
        if value is False:
            value = 'false'
        elif value is True:
            value = 'true'
        elif value is None:
            value = ''
        elif hasattr(value, 'keys'):
            value = ';'.join([k+':'+str(value[k]) for k in value.keys()])
        else:
            value = str(value)

        request = rpc.TPortoRequest()
        request.SetProperty.name = name

        if type(prop) is tuple:
            request.SetProperty.property = prop[0] + "[" + prop[1] + "]"
        elif index is not None:
            request.SetProperty.property = prop + "[" + index + "]"
        else:
            request.SetProperty.property = prop

        request.SetProperty.value = value
        self._call(request)

    def Set(self, container, **kwargs):
        for prop, value in kwargs.items():
            self.SetProperty(container, prop, value)

    def GetData(self, name, data, sync=False):
        request = rpc.TPortoRequest()
        request.GetDataProperty.name = name
        request.GetDataProperty.data = data
        request.GetDataProperty.sync = sync
        res = self._call(request).GetDataProperty.value
        if res == 'false':
            return False
        elif res == 'true':
            return True
        return res

    def GetInt(self, name, prop, index=None):
        if isinstance(prop, tuple):
            prop, index = prop

        val = self.GetProperty(name, prop, index=index)
        try:
            return int(val)
        except ValueError:
            raise exceptions.InvalidValue("Non integer value: {}".format(val))

    def SetInt(self, name, prop, value, index=None):
        self.SetProperty(name, prop, value, index=index)

    def GetMap(self, name, prop, conv=int):
        val = self.GetProperty(name, prop)
        ret = dict()
        for v in val.split(';'):
            p = v.split(':')
            if len(p) == 2:
                ret[p[0].strip()] = conv(p[1])
        return ret

    def SetMap(self, name, prop, value):
        self.SetProperty(name, prop, value)

    def ContainerProperties(self):
        request = rpc.TPortoRequest()
        request.ListProperties.SetInParent()
        res = {}
        for prop in self._call(request).ListProperties.list:
            res[prop.name] = Property(prop.name, prop.desc, prop.read_only, prop.dynamic)
        return res

    def VolumeProperties(self):
        request = rpc.TPortoRequest()
        request.ListVolumeProperties.SetInParent()
        res = {}
        for prop in self._call(request).ListVolumeProperties.list:
            res[prop.name] = Property(prop.name, prop.desc, False, False)
        return res

    def Plist(self):
        request = rpc.TPortoRequest()
        request.ListProperties.SetInParent()
        return [item.name for item in self._call(request).ListProperties.list]

    def Dlist(self):
        """deprecated - now they properties"""
        request = rpc.TPortoRequest()
        request.ListDataProperties.SetInParent()
        return [item.name for item in self._call(request).ListDataProperties.list]

    def Vlist(self):
        request = rpc.TPortoRequest()
        request.ListVolumeProperties.SetInParent()
        result = self._call(request).ListVolumeProperties.list
        return [prop.name for prop in result]

    def WaitContainers(self, containers, timeout=None, labels=None):
        request = rpc.TPortoRequest()
        for ct in containers:
            request.Wait.name.append(str(ct))
        if timeout is not None and timeout >= 0:
            request.Wait.timeout_ms = int(timeout * 1000)
        else:
            timeout = None
        if labels is not None:
            request.Wait.label.extend(labels)
        resp = self._call(request, timeout)
        if resp.Wait.name == "":
            raise exceptions.WaitContainerTimeout("Timeout {} exceeded".format(timeout))
        return resp.Wait.name

    def Wait(self, containers, timeout=None, timeout_s=None, labels=None):
        """legacy compat - timeout in ms"""
        if timeout_s is not None:
            timeout = timeout_s
        elif timeout is not None and timeout >= 0:
            timeout = timeout / 1000.
        try:
            return self.WaitContainers(containers, timeout, labels=labels)
        except exceptions.WaitContainerTimeout:
            return ""

    def AsyncWait(self, containers, callback, timeout=None, labels=None):
        names = [str(ct) for ct in containers]

        with self.lock:
            self.async_wait_names = names
            self.async_wait_callback = callback
            self.async_wait_timeout = timeout

        request = rpc.TPortoRequest()
        request.AsyncWait.name.extend(names)
        if timeout is not None:
            request.AsyncWait.timeout_ms = int(timeout * 1000)
        if labels is not None:
            request.AsyncWait.label.extend(labels)
        self._call(request)

    def WaitLabels(self, containers, labels, timeout=None):
        request = rpc.TPortoRequest()
        for ct in containers:
            request.Wait.name.append(str(ct))
        if timeout is not None and timeout >= 0:
            request.Wait.timeout_ms = int(timeout * 1000)
        else:
            timeout = None
        request.Wait.label.extend(labels)
        resp = self._call(request, timeout)
        if resp.Wait.name == "":
            raise exceptions.WaitContainerTimeout("Timeout {} exceeded".format(timeout))
        return self._decode_message(resp.Wait)

    def GetLabel(self, container, label):
        return self.GetProperty(container, 'labels', label)

    def SetLabel(self, container, label, value, prev_value=None, state=None):
        req = rpc.TPortoRequest()
        req.SetLabel.name = str(container)
        req.SetLabel.label = label
        req.SetLabel.value = value
        if prev_value is not None:
            req.SetLabel.prev_value = prev_value
        if state is not None:
            req.SetLabel.state = state
        self._call(req)

    def IncLabel(self, container, label, add=1):
        req = rpc.TPortoRequest()
        req.IncLabel.name = str(container)
        req.IncLabel.label = label
        req.IncLabel.add = add
        return self._call(req).IncLabel.result

    def CreateVolume(self, path=None, layers=None, storage=None, private_value=None, timeout=None, **properties):
        if layers:
            layers = [layer.name if isinstance(layer, Layer) else layer for layer in layers]
            properties['layers'] = ';'.join(layers)

        if storage is not None:
            properties['storage'] = str(storage)

        if private_value is not None:
            properties['private'] = private_value

        request = rpc.TPortoRequest()
        request.CreateVolume.SetInParent()
        if path:
            request.CreateVolume.path = path
        request.CreateVolume.properties.update(properties)
        pb = self._call(request, timeout or self.disk_timeout).CreateVolume
        return Volume(self, pb.path, pb)

    def FindVolume(self, path):
        pb = self._ListVolumes(path=path)[0]
        return Volume(self, path, pb)

    def NewVolume(self, spec, timeout=None):
        req = rpc.TPortoRequest()
        req.NewVolume.SetInParent()
        self._encode_message(req.NewVolume.volume, spec)
        rsp = self._call(req, timeout or self.disk_timeout)
        return self._decode_message(rsp.NewVolume.volume)

    def GetVolume(self, path, container=None, timeout=None):
        req = rpc.TPortoRequest()
        req.GetVolume.SetInParent()
        if container is not None:
            req.GetVolume.container = str(container)
        req.GetVolume.path.append(path)
        rsp = self._call(req, timeout or self.disk_timeout)
        return self._decode_message(rsp.GetVolume.volume[0])

    def GetVolumes(self, paths=None, container=None, labels=None, timeout=None):
        req = rpc.TPortoRequest()
        req.GetVolume.SetInParent()
        if container is not None:
            req.GetVolume.container = str(container)
        if paths is not None:
            req.GetVolume.path.extend(paths)
        if labels is not None:
            req.GetVolume.label.extend(labels)
        rsp = self._call(req, timeout or self.disk_timeout)
        return [self._decode_message(v) for v in rsp.GetVolume.volume]

    def LinkVolume(self, path, container, target=None, read_only=False, required=False):
        request = rpc.TPortoRequest()
        if target is not None or required:
            command = request.LinkVolumeTarget
        else:
            command = request.LinkVolume
        command.path = path
        command.container = container
        if target is not None:
            command.target = target
        if read_only:
            command.read_only = True
        if required:
            command.required = True
        self._call(request)

    def UnlinkVolume(self, path, container=None, target=None, strict=None, timeout=None):
        request = rpc.TPortoRequest()
        if target is not None:
            command = request.UnlinkVolumeTarget
        else:
            command = request.UnlinkVolume
        command.path = path
        if container:
            command.container = container
        if target is not None:
            command.target = target
        if strict is not None:
            command.strict = strict
        self._call(request, timeout or self.disk_timeout)

    def DestroyVolume(self, volume, strict=None, timeout=None):
        self.UnlinkVolume(volume.path if isinstance(volume, Volume) else volume, '***', strict=strict, timeout=timeout)

    def _ListVolumes(self, path=None, container=None):
        if isinstance(container, Container):
            container = container.name
        request = rpc.TPortoRequest()
        request.ListVolumes.SetInParent()
        if path:
            request.ListVolumes.path = path
        if container:
            request.ListVolumes.container = container
        return self._call(request).ListVolumes.volumes

    def ListVolumes(self, path=None, container=None):
        return [Volume(self, v.path, v) for v in self._ListVolumes(path=path, container=container)]

    def ListVolumeLinks(self, volume=None, container=None):
        links = []
        for v in self._ListVolumes(path=volume.path if isinstance(volume, Volume) else volume, container=container):
            for link in v.links:
                links.append(VolumeLink(Volume(self, v.path, v), Container(self, link.container), link.target, link.read_only, link.required))
        return links

    def GetVolumeProperties(self, path):
        return {p.name: p.value for p in self._ListVolumes(path=path)[0].properties}

    def TuneVolume(self, path, **properties):
        request = rpc.TPortoRequest()
        request.TuneVolume.SetInParent()
        request.TuneVolume.path = path
        request.TuneVolume.properties.update(properties)
        self._call(request)

    def ImportLayer(self, layer, tarball, place=None, private_value=None, timeout=None):
        request = rpc.TPortoRequest()
        request.ImportLayer.layer = layer
        request.ImportLayer.tarball = tarball
        request.ImportLayer.merge = False
        if place is not None:
            request.ImportLayer.place = place
        if private_value is not None:
            request.ImportLayer.private_value = private_value

        self._call(request, timeout or self.disk_timeout)
        return Layer(self, layer, place)

    def MergeLayer(self, layer, tarball, place=None, private_value=None, timeout=None):
        request = rpc.TPortoRequest()
        request.ImportLayer.layer = layer
        request.ImportLayer.tarball = tarball
        request.ImportLayer.merge = True
        if place is not None:
            request.ImportLayer.place = place
        if private_value is not None:
            request.ImportLayer.private_value = private_value
        self._call(request, timeout or self.disk_timeout)
        return Layer(self, layer, place)

    def RemoveLayer(self, layer, place=None, timeout=None):
        request = rpc.TPortoRequest()
        request.RemoveLayer.layer = layer
        if place is not None:
            request.RemoveLayer.place = place
        self._call(request, timeout or self.disk_timeout)

    def GetLayerPrivate(self, layer, place=None):
        request = rpc.TPortoRequest()
        request.GetLayerPrivate.layer = layer
        if place is not None:
            request.GetLayerPrivate.place = place
        return self._call(request).GetLayerPrivate.private_value

    def SetLayerPrivate(self, layer, private_value, place=None):
        request = rpc.TPortoRequest()
        request.SetLayerPrivate.layer = layer
        request.SetLayerPrivate.private_value = private_value

        if place is not None:
            request.SetLayerPrivate.place = place
        self._call(request)

    def ExportLayer(self, volume, tarball, place=None, compress=None, timeout=None):
        request = rpc.TPortoRequest()
        request.ExportLayer.volume = volume
        request.ExportLayer.tarball = tarball
        if place is not None:
            request.ExportLayer.place = place
        if compress is not None:
            request.ExportLayer.compress = compress
        self._call(request, timeout or self.disk_timeout)

    def ReExportLayer(self, layer, tarball, place=None, compress=None, timeout=None):
        request = rpc.TPortoRequest()
        request.ExportLayer.volume = ""
        request.ExportLayer.layer = layer
        request.ExportLayer.tarball = tarball
        if place is not None:
            request.ExportLayer.place = place
        if compress is not None:
            request.ExportLayer.compress = compress
        self._call(request, timeout or self.disk_timeout)

    def _ListLayers(self, place=None, mask=None):
        request = rpc.TPortoRequest()
        request.ListLayers.SetInParent()
        if place is not None:
            request.ListLayers.place = place
        if mask is not None:
            request.ListLayers.mask = mask
        return self._call(request).ListLayers

    def ListLayers(self, place=None, mask=None):
        response = self._ListLayers(place, mask)
        if response.layers:
            return [Layer(self, layer.name, place, layer) for layer in response.layers]
        return [Layer(self, layer, place) for layer in response.layer]

    def FindLayer(self, layer, place=None):
        response = self._ListLayers(place, layer)
        if layer not in response.layer:
            raise exceptions.LayerNotFound("layer `%s` not found" % layer)
        if response.layers and response.layers[0].name == layer:
            return Layer(self, layer, place, response.layers[0])
        return Layer(self, layer, place)

    def _ListStorages(self, place=None, mask=None):
        request = rpc.TPortoRequest()
        request.ListStorages.SetInParent()
        if place is not None:
            request.ListStorages.place = place
        if mask is not None:
            request.ListStorages.mask = mask
        return self._call(request).ListStorages

    def ListStorages(self, place=None, mask=None):
        return [Storage(self, s.name, place, s) for s in self._ListStorages(place, mask).storages]

    def FindStorage(self, name, place=None):
        response = self._ListStorages(place, name)
        if not response.storages:
            raise exceptions.VolumeNotFound("storage `%s` not found" % name)
        return Storage(self, name, place, response.storages[0])

    def ListMetaStorages(self, place=None, mask=None):
        return [MetaStorage(self, s.name, place, s) for s in self._ListStorages(place, mask).meta_storages]

    def FindMetaStorage(self, name, place=None):
        response = self._ListStorages(place, name + "/")
        if not response.meta_storages:
            raise exceptions.VolumeNotFound("meta storage `%s` not found" % name)
        return MetaStorage(self, name, place, response.meta_storages[0])

    def RemoveStorage(self, name, place=None, timeout=None):
        request = rpc.TPortoRequest()
        request.RemoveStorage.name = name
        if place is not None:
            request.RemoveStorage.place = place
        self._call(request, timeout or self.disk_timeout)

    def ImportStorage(self, name, tarball, place=None, private_value=None, timeout=None):
        request = rpc.TPortoRequest()
        request.ImportStorage.name = name
        request.ImportStorage.tarball = tarball
        if place is not None:
            request.ImportStorage.place = place
        if private_value is not None:
            request.ImportStorage.private_value = private_value
        self._call(request, timeout or self.disk_timeout)
        return Storage(self, name, place)

    def ExportStorage(self, name, tarball, place=None, timeout=None):
        request = rpc.TPortoRequest()
        request.ExportStorage.name = name
        request.ExportStorage.tarball = tarball
        if place is not None:
            request.ExportStorage.place = place
        self._call(request, timeout or self.disk_timeout)

    def CreateMetaStorage(self, name, place=None, private_value=None, space_limit=None, inode_limit=None):
        request = rpc.TPortoRequest()
        request.CreateMetaStorage.name = name
        if place is not None:
            request.CreateMetaStorage.place = place
        if private_value is not None:
            request.CreateMetaStorage.private_value = private_value
        if space_limit is not None:
            request.CreateMetaStorage.space_limit = space_limit
        if inode_limit is not None:
            request.CreateMetaStorage.inode_limit = inode_limit
        self._call(request)
        return MetaStorage(self, name, place)

    def GetProcMetric(self, names, metric):
        def GetMetricValue(pid, metric_name):
            sched_path = "/proc/{0}/sched".format(pid)
            try:
                sched = open(sched_path)
                for line in sched:
                    if re.search('\s{0,}' + metric_name, line): # noqa W605
                        line = line.split()
                        return int(line[-1])
            except: # noqa E722
                pass
            return 0

        def TidSnapshot(pids):
            clean_pids = []
            for pid in pids:
                try:
                    pid = int(pid)
                    clean_pids.append(pid)
                except: # noqa E722
                    continue
            return clean_pids

        def GetFreezerCgroup(pid):
            cgroup_path = "/proc/{0}/cgroup".format(pid)
            try:
                cgroup = open(cgroup_path)
                for line in cgroup:
                    fPos = line.find(":freezer:")
                    if fPos >= 0:
                        return line[fPos + len(":freezer:"):].strip()
            except: # noqa E722
                pass
            return ''

        def MatchCgroups(pid_cgroup, ct_cgroup):
            if len(pid_cgroup) <= len(ct_cgroup):
                return pid_cgroup == ct_cgroup
            return ct_cgroup == pid_cgroup[:len(ct_cgroup)] and list(pid_cgroup)[len(ct_cgroup)] == '/'

        def GetCtxsw():
            values = {}
            for name in names:
                values[name] = 0

            containers_props = self.Get(names, ["cgroups[freezer]"])
            pids = TidSnapshot(os.listdir("/proc"))
            for pid in pids:
                pid_cgroup = GetFreezerCgroup(pid)
                if not pid_cgroup:
                    continue
                metric_value = GetMetricValue(pid, 'nr_switches')
                for name in names:
                    container_cgroup = containers_props[name]["cgroups[freezer]"].replace("/sys/fs/cgroup/freezer", '')
                    if MatchCgroups(pid_cgroup, container_cgroup):
                        values[name] += metric_value
            return values

        if metric == 'ctxsw':
            return GetCtxsw()
        raise exceptions.InvalidValue("Unknown metric: {}".format(metric))

    def ResizeMetaStorage(self, name, place=None, private_value=None, space_limit=None, inode_limit=None):
        request = rpc.TPortoRequest()
        request.ResizeMetaStorage.name = name
        if place is not None:
            request.ResizeMetaStorage.place = place
        if private_value is not None:
            request.ResizeMetaStorage.private_value = private_value
        if space_limit is not None:
            request.ResizeMetaStorage.space_limit = space_limit
        if inode_limit is not None:
            request.ResizeMetaStorage.inode_limit = inode_limit
        self._call(request)

    def RemoveMetaStorage(self, name, place=None):
        request = rpc.TPortoRequest()
        request.RemoveMetaStorage.name = name
        if place is not None:
            request.RemoveMetaStorage.place = place
        self._call(request)

    def ConvertPath(self, path, source, destination):
        request = rpc.TPortoRequest()
        request.ConvertPath.path = path
        request.ConvertPath.source = source
        request.ConvertPath.destination = destination
        return self._call(request).ConvertPath.path

    def SetSymlink(self, name, symlink, target):
        request = rpc.TPortoRequest()
        request.SetSymlink.container = name
        request.SetSymlink.symlink = symlink
        request.SetSymlink.target = target
        self._call(request)

    def AttachProcess(self, name, pid, comm=""):
        request = rpc.TPortoRequest()
        request.AttachProcess.name = name
        request.AttachProcess.pid = pid
        request.AttachProcess.comm = comm
        self._call(request)

    def AttachThread(self, name, pid, comm=""):
        request = rpc.TPortoRequest()
        request.AttachThread.name = name
        request.AttachThread.pid = pid
        request.AttachThread.comm = comm
        self._call(request)

    def LocateProcess(self, pid, comm=""):
        request = rpc.TPortoRequest()
        request.LocateProcess.pid = pid
        request.LocateProcess.comm = comm
        name = self._call(request).LocateProcess.name
        return Container(self, name)

    def Version(self):
        request = rpc.TPortoRequest()
        request.Version.SetInParent()
        response = self._call(request)
        return (response.Version.tag, response.Version.revision)

    # Docker images API
    def DockerImageStatus(self, name=None, place=None):
        request = rpc.TPortoRequest()
        request.dockerImageStatus.CopyFrom(rpc.TDockerImageStatusRequest())
        request.dockerImageStatus.name = name
        if place is not None:
            request.dockerImageStatus.place = place

        return self._call(request).dockerImageStatus.image

    def ListDockerImages(self, place=None, mask=None):
        request = rpc.TPortoRequest()
        request.listDockerImages.CopyFrom(rpc.TDockerImageListRequest())
        if place is not None:
            request.listDockerImages.place = place
        if mask is not None:
            request.listDockerImages.mask = mask

        return self._call(request).listDockerImages.images

    def PullDockerImage(self, name, place=None, auth_token=None, auth_path=None, auth_service=None):
        request = rpc.TPortoRequest()
        request.pullDockerImage.CopyFrom(rpc.TDockerImagePullRequest())
        request.pullDockerImage.name = name
        if place is not None:
            request.pullDockerImage.place = place
        if auth_token is not None:
            request.pullDockerImage.auth_token = auth_token
        if auth_path is not None:
            request.pullDockerImage.auth_path = auth_path
        if auth_service is not None:
            request.pullDockerImage.auth_service = auth_service

        return self._call(request).pullDockerImage.image

    def RemoveDockerImage(self, name, place=None):
        request = rpc.TPortoRequest()
        request.removeDockerImage.CopyFrom(rpc.TDockerImageRemoveRequest())
        request.removeDockerImage.name = name
        if place is not None:
            request.removeDockerImage.place = place

        self._call(request)


Connection = PortoApi
