"""Compatibility entry point for Luigi CLI."""
from luigi_app.cli import main

if __name__ == "__main__":
    import sys
    from luigi_app.cli import entry
    sys.exit(entry())
