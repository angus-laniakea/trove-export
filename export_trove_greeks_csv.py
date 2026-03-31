"""Backward-compatible shim; prefer: uv run export-trove-greeks"""

from trove_export.export_greeks_csv import main

if __name__ == "__main__":
    main()
