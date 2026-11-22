"""Keep Python test traffic on loopback, including CLI subprocesses."""

import socket
import sys


def require_loopback(event, args):
    if event == "socket.getaddrinfo":
        host = args[0]
        if host is None:
            return
    elif event == "socket.connect" and args[0].family in (socket.AF_INET, socket.AF_INET6):
        host = args[1][0]
    else:
        return

    if host not in ("localhost", "127.0.0.1", "::1"):
        raise PermissionError("Tests may only access loopback addresses")


sys.addaudithook(require_loopback)
