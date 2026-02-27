# Sample Data

This folder contains two partitions of the mock order data generated in `01_generate_orders.py`.

## Files Included

- `orders_001.json`
- `orders_002.json`

## Purpose

These files are used as the input source for the Auto Loader stream defined in the bronze notebook. They mock raw data arriving in cloud object storage.

## Notes

- These files are not sourced from any real system.
- File names ending in `_001`, `_002`, etc. simulate incremental arrivals.
