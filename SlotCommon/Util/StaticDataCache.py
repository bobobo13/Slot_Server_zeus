import functools
import inspect

try:
    from Common.RoutineProc import RoutineProc
except ImportError:
    RoutineProc = None


class _StaticDataCache(object):
    def __init__(self, Func, FlushTime, bAllowCacheNone):
        def wrapper(func):
            @functools.wraps(func)
            def wrapped(*args, **kwargs):
                return self.wrap(func, *args, **kwargs)

            return wrapped

        self._Func = wrapper(Func)
        self._IsAllowCacheNone = bAllowCacheNone
        self._CachedData = {}
        self._DataExpireTime = {}
        if RoutineProc is not None:
            RoutineProc("FlushCacheData", FlushTime, self.FlushCachedData, )

    def FlushCachedData(self):
        self._CachedData = {}

    def wrap(self, _func, *args, **kwargs):
        fargs, fvarargs, fkeywords, fdefaults = inspect.getargspec(_func)
        t = []
        for i in range(len(fargs)):
            if fargs[i] == 'self':
                continue
            k = fargs[i]
            if i < len(args):
                v = args[i]
            elif fargs[i] in kwargs:
                v = kwargs[fargs[i]]
            elif fdefaults is not None and (i - len(fargs) < len(fdefaults)):
                v = fdefaults[i - len(fargs) + (len(fdefaults))]
            else:
                raise TypeError("{}() takes {} {} arguments ({} given)".format(_func.__name__, "exactly" if (
                        fdefaults is None and fvarargs is None and fkeywords is None) else "at least",
                                                                               len(fargs) if fdefaults is None else len(
                                                                                   fargs) - len(fdefaults),
                                                                               len(args) + len(kwargs)))
            t.append((k, v))
        t2 = tuple(t)
        if t2 in self._CachedData:
            return self._CachedData[t2]
        if fargs[0] == 'self':
            r = _func(*args, **kwargs)
        else:
            r = _func(*args, **kwargs)
        if r is None and not self._IsAllowCacheNone:
            return None
        self._CachedData[t2] = r
        return r


def StaticDataCache(nFlushTime=3 * 60, bAllowCacheNone=False):
    def decorator(func):
        return _StaticDataCache(func, nFlushTime, bAllowCacheNone)._Func

    return decorator


if __name__ == "__main__":
    import time


    class Test:
        @StaticDataCache(flush_time=60)
        def GetMyData(self, d1, d2):
            # print(self)
            assert isinstance(self, Test)
            r = 0
            for i in range(d2):
                r += d1
            return r

        @StaticDataCache()
        def GetMyData2(self, d1, d2):
            # print(self)
            r = 0
            for i in range(d2):
                r += d1 * 2
            return r


    class Test2:
        @StaticDataCache()
        def GetMyData(self, d1, d2):
            # print(self)
            r = 0
            for i in range(d2):
                r -= d1
            return r


    test_loop_amount = 100000000
    t = Test()
    t0 = time.time()
    r = t.GetMyData(2, test_loop_amount)
    print
    r
    print(time.time() - t0)

    t0 = time.time()
    r = t.GetMyData(2, test_loop_amount)
    print(r)
    print(time.time() - t0)

    t = Test()
    t0 = time.time()
    r = t.GetMyData(2, test_loop_amount)
    print(r)
    print(time.time() - t0)

    t = Test()
    t0 = time.time()
    r = t.GetMyData2(2, test_loop_amount)
    print(r)
    print(time.time() - t0)

    t = Test()
    t0 = time.time()
    r = t.GetMyData2(2, test_loop_amount)
    print(r)
    print(time.time() - t0)

    t = Test2()
    t0 = time.time()
    r = t.GetMyData(2, test_loop_amount)
    print
    r
    print(time.time() - t0)
