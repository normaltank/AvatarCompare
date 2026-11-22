"""Native Windows process/system counters; no optional monitoring dependency."""
from __future__ import annotations

import ctypes
from ctypes import wintypes
import os
import threading
import time


class ResourceMonitor:
    def __init__(self, interval=0.2, extra_pids=()):
        self.interval = interval
        self.stop = threading.Event()
        self.samples = []
        self.thread = None
        self.cpus = os.cpu_count() or 1
        self.extra_handles = []
        self.extra_pids = set()
        self.attach_lock = threading.Lock()
        if os.name == 'nt':
            self.kernel = ctypes.WinDLL('kernel32', use_last_error=True)
            self.psapi = ctypes.WinDLL('psapi', use_last_error=True)
            self.kernel.GetCurrentProcess.restype = wintypes.HANDLE
            self.process = self.kernel.GetCurrentProcess()

            class Memory(ctypes.Structure):
                _fields_ = [('cb', wintypes.DWORD), ('PageFaultCount', wintypes.DWORD)]+[(name, ctypes.c_size_t) for name in
                    ['PeakWorkingSetSize', 'WorkingSetSize', 'QuotaPeakPagedPoolUsage', 'QuotaPagedPoolUsage',
                     'QuotaPeakNonPagedPoolUsage', 'QuotaNonPagedPoolUsage', 'PagefileUsage', 'PeakPagefileUsage', 'PrivateUsage']]

            class IO(ctypes.Structure):
                _fields_ = [(name, ctypes.c_ulonglong) for name in ['read_ops', 'write_ops', 'other_ops', 'read_bytes', 'write_bytes', 'other_bytes']]

            self.Memory, self.IO = Memory, IO
            self.kernel.GetProcessTimes.argtypes = [wintypes.HANDLE]+[ctypes.POINTER(wintypes.FILETIME)]*4
            self.kernel.GetSystemTimes.argtypes = [ctypes.POINTER(wintypes.FILETIME)]*3
            self.kernel.GetProcessIoCounters.argtypes = [wintypes.HANDLE, ctypes.POINTER(IO)]
            self.psapi.GetProcessMemoryInfo.argtypes = [wintypes.HANDLE, ctypes.POINTER(Memory), wintypes.DWORD]
            self.kernel.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
            self.kernel.OpenProcess.restype = wintypes.HANDLE
            self.kernel.CloseHandle.argtypes = [wintypes.HANDLE]
            for pid in extra_pids:
                self.attach_pid(pid)

    def attach_pid(self, pid):
        if os.name != 'nt':
            return
        with self.attach_lock:
            if pid in self.extra_pids:
                return
            handle = self.kernel.OpenProcess(0x410, False, pid)
            if not handle:
                raise ctypes.WinError(ctypes.get_last_error())
            self.extra_handles.append(handle)
            self.extra_pids.add(pid)

    def read(self):
        now = time.perf_counter()
        if os.name != 'nt':
            return {'time': now, 'process_cpu': time.process_time(), 'memory': 0, 'private': 0, 'read_bytes': 0, 'write_bytes': 0, 'system_cpu': 0, 'system_idle': 0}
        def seconds(value):
            return ((value.dwHighDateTime << 32) | value.dwLowDateTime)/1e7
        idle, system_kernel, system_user = [wintypes.FILETIME() for _ in range(3)]
        if not self.kernel.GetSystemTimes(*[ctypes.byref(x) for x in [idle, system_kernel, system_user]]):
            raise ctypes.WinError(ctypes.get_last_error())
        cpu = working = private = read = written = 0
        for process in [self.process]+self.extra_handles:
            created, ended, kernel, user = [wintypes.FILETIME() for _ in range(4)]
            memory, io = self.Memory(), self.IO()
            memory.cb = ctypes.sizeof(memory)
            if not self.kernel.GetProcessTimes(process, *[ctypes.byref(x) for x in [created, ended, kernel, user]]):
                raise ctypes.WinError(ctypes.get_last_error())
            if not self.psapi.GetProcessMemoryInfo(process, ctypes.byref(memory), memory.cb):
                if process == self.process:
                    raise ctypes.WinError(ctypes.get_last_error())
            if not self.kernel.GetProcessIoCounters(process, ctypes.byref(io)):
                if process == self.process:
                    raise ctypes.WinError(ctypes.get_last_error())
            cpu += seconds(kernel)+seconds(user)
            working += memory.WorkingSetSize
            private += memory.PrivateUsage
            read += io.read_bytes
            written += io.write_bytes
        return {'time': now, 'process_cpu': cpu,
                'memory': working, 'private': private,
                'read_bytes': read, 'write_bytes': written,
                'system_cpu': seconds(system_kernel)+seconds(system_user), 'system_idle': seconds(idle)}

    def __enter__(self):
        self.samples.append(self.read())
        def sample():
            while not self.stop.wait(self.interval):
                self.samples.append(self.read())
        self.thread = threading.Thread(target=sample, daemon=True)
        self.thread.start()
        return self

    def __exit__(self, *args):
        self.stop.set()
        self.thread.join()
        self.samples.append(self.read())
        for handle in self.extra_handles:
            self.kernel.CloseHandle(handle)
        self.extra_handles = []

    def summary(self):
        first, last = self.samples[0], self.samples[-1]
        elapsed = max(last['time']-first['time'], 1e-9)
        core_seconds = last['process_cpu']-first['process_cpu']
        system_total = last['system_cpu']-first['system_cpu']
        cpu_peaks = [(b['process_cpu']-a['process_cpu'])/(b['time']-a['time'])/self.cpus*100 for a, b in zip(self.samples, self.samples[1:]) if b['time']-a['time'] > 0.05]
        # Summing many live processes is not an atomic snapshot. Short windows
        # may exceed100% due to scheduling/accounting granularity; retain that
        # diagnostic but use a longer bounded estimate for display.
        rolling = []
        for index, ending in enumerate(self.samples):
            earlier = next((sample for sample in reversed(self.samples[:index]) if ending['time']-sample['time'] >= 0.75), None)
            if earlier:
                rolling.append((ending['process_cpu']-earlier['process_cpu'])/(ending['time']-earlier['time'])/self.cpus*100)
        return {'elapsed_seconds': elapsed, 'logical_cpus': self.cpus,
                'process_cpu_percent': core_seconds/elapsed/self.cpus*100,
                'process_cpu_peak_percent': min(100, max(rolling, default=core_seconds/elapsed/self.cpus*100)),
                'process_cpu_raw_short_interval_peak_percent': max(cpu_peaks, default=0),
                'cpu_peak_window_seconds': 0.75, 'process_core_equivalents': core_seconds/elapsed,
                'system_cpu_percent': (1-(last['system_idle']-first['system_idle'])/system_total)*100 if system_total else None,
                'peak_working_set_bytes': max(s['memory'] for s in self.samples),
                'peak_private_bytes': max(s['private'] for s in self.samples),
                'process_read_bytes': last['read_bytes']-first['read_bytes'],
                'process_write_bytes': last['write_bytes']-first['write_bytes'],
                'samples': self.samples}
