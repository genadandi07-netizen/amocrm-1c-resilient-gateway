"""Root conftest: guarantees the project root (`code/`) is importable as sys.path[0]
so that `import gateway` works regardless of the installed pytest version.

This file intentionally contains NO test cases and NO adapter shims.
"""
