#!/usr/bin/env python3
"""Evaluate a Python expression against JSON from stdin, bound as `d`.
Used by the shell scripts; taking the expression as an argument avoids the
quoting problems of a python -c one-liner."""
import json
import sys

d = json.load(sys.stdin)
v = eval(sys.argv[1], {"d": d, "next": next, "sum": sum, "len": len})
if isinstance(v, bool):
    print(int(v))
elif v is None:
    print("")
else:
    print(v)
