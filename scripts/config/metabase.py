#!/usr/bin/python
from types import FunctionType
from copy import deepcopy

# a hack to get classmethod type
def get_classmethod_type():
    class X(object):
        def m(cls):
            pass
    return type(classmethod(X.m))
    
ClassMethodType = get_classmethod_type()

class Subclass(object):
    def __init__(self, iterable):
        self._data  = [x for x in iterable]
        
    def __iter__(self):
        for x in self._data:
            yield x
            
class InitMethod(object):
    def __init__(self, method):
        self.method = classmethod(method)
        
    def __call__(self):
        return self.method()
    
    
def initmethod(x):
    return InitMethod(x)
    
    
class PropMethod(object):
    def __init__(self, method):
        self.method = classmethod(method)
        
    def __call__(self):
        return self.method()
    
def propmethod(x):
    return PropMethod(x)
    

class Template(object):
    def __init__(self, data):
        self._data = data
        
    def __str__(self):
        raise 'Do not make me string!'
    
    @staticmethod
    def _process(data, cls):
        if isinstance(data, dict):
            for k, v in data.items():
                k = Template._process(k, cls)
                data[k] = Template._process(v, cls)
            
        elif isinstance(data, list):
            for i, v in enumerate(data):
                data[i] = Template._process(v, cls)
            
        elif isinstance(data, basestring):
            data = data % cls        
        return data
    
    def process_local(self, cls):
        data = deepcopy(self._data)
        return Template._process(data, cls.__dict__)
        
    def process(self, cls):
        class AttributeMapper(object):
            def __init__(self, cls):
                self.cls = cls
                
            def __getitem__(self, key):
                    #if key == '__name__':
                    #    import pdb
                    #    pdb.set_trace()
                    x =  getattr(self.cls, key, None)
                    return x

        data = deepcopy(self._data)
        return Template._process(data, AttributeMapper(cls))
        

class ConfigMeta(type):
    #references store to disable garbage collector
    generated_classes = []
    
    @staticmethod
    def iterfuncs(cls):
        for k, v in cls.__dict__.iteritems():
            if isinstance(v, ClassMethodType) and k[0] == '_' and k[1] != '_':
                yield k
                
        if cls.__bases__:
            for base in cls.__bases__:
                for name in ConfigMeta.iterfuncs(base):
                    yield name
        
    @staticmethod
    def process_list(lst, func):
        while True:
            success = False
            for i, value in enumerate(lst):
                try:
                    func(value)
                    success = True
                    del lst[i]
                    break
                except Exception as e:
                    #print e
                    pass
                    
            if not lst or not success:
                break
        
    @staticmethod
    def process_propmethods(cls, funcs):
        def processor(name):
            method = getattr(cls, name)
            res = method()
            # set attribute excluding heading underscore
            setattr(cls, name, res)
            
        ConfigMeta.process_list(funcs, processor)
    
    @staticmethod
    def process_initmethods(cls, funcs):
        def processor(name):
            method = getattr(cls, name)
            res = method()

        ConfigMeta.process_list(funcs, processor)
        
    @staticmethod
    def process_templates(cls, templates):
        def process_local(value):
            name, template = value
            res = template.process_local(cls)
            setattr(cls, name, res)
        
        def process(value):
            name, template = value
            res = template.process(cls)
            setattr(cls, name, res)
            
        
        ConfigMeta.process_list(templates, process_local)
        #print 'Before process unlocal' , cls, templates
        ConfigMeta.process_list(templates, process)
        #print 'After process unlocal' , cls, templates
        
    @staticmethod
    def initdict(props, bases, pcls, key):
        d = {}
        for base in bases:
            basedict = getattr(base, key, {})
            d.update(basedict)
            
        for k in d.keys():
            if k in props:
                del d[k]
            
        for k, v in props.iteritems():
            if isinstance(v, pcls):
                d[k] = v
                
        return d

    def __new__(mcls, name, bases, props):             
        initmethods = ConfigMeta.initdict(props, bases, InitMethod, '__initmethods')
        propmethods = ConfigMeta.initdict(props, bases, PropMethod, '__propmethods')
        templates = ConfigMeta.initdict(props, bases, Template, '__templates')
        subclasses = ConfigMeta.initdict(props, bases, Subclass, '__subclasses')
        
        # bind base propmethods to current class
        props.update(propmethods)
        
        # make all (new) methods - classmethods
        for k, v in props.items():
            if isinstance(v, FunctionType):
                props[k] = classmethod(v)
            elif isinstance(v, InitMethod) or isinstance(v, PropMethod):
                props[k] = v.method
                
        # make class
        cls = type.__new__(mcls, name, bases, props)
        
        #print name, propmethods
        
        setattr(cls, '__templates', templates)
        setattr(cls, '__initmethods', initmethods)
        setattr(cls, '__propmethods', propmethods)
        setattr(cls, '__subclasses', subclasses)
        
        ConfigMeta.process_initmethods(cls, initmethods.keys())
        ConfigMeta.process_propmethods(cls, propmethods.keys())
        ConfigMeta.process_templates(cls, templates.items())

        #generate subclasses from enumerables
        for k, v in subclasses.iteritems():
            try:
                for i, value in enumerate(v):
                    subcls = ConfigMeta.__new__(mcls, str(i), (cls, ), {k : value})
                    # dirty hack! important! preserves reference on newly generated class, protects from garbage collector
                    mcls.generated_classes.append(subcls)
                break
            except:
                print "Failed to subclass %s by field %s" % (name, k)
        
        return cls


class ConfigBase:
    __metaclass__ = ConfigMeta
    
    
if __name__ == '__main__':
    # ToDo: some tests and examples here
    print 'test here'
    class Test(ConfigBase):
        plan = Subclass(map(str, range(10)))
        
        port = 12
    
        x = Template('shfksjhf %(port)d %(something)s')
        y = Template({'%(port)s' : 'fsdf %(future)s', 5 : ['fdsafd', '%(rem_dir)s', '%(something)s']})
        
        def something(cls):
            return 'const'
    
        def rem_dir(cls):
            return 'x   ' + cls.something
    
        def future(cls):
            return 'future ' + cls.plan
    
    #print ConfigBase.__dict__
    #b = ConfigBase.__subclasses__()[2]
    #print b.__dict__
    
