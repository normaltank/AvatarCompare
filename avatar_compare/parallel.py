"""Spawn-safe CPU work used by the hardware-sized range mode."""
import os
import signal

import cv2

from .features import extract


def initialize_cpu():
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    cv2.setNumThreads(1)


def extract_in_process(data):
    return os.getpid(), extract(data)
