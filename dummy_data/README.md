# Dummy Data Files

This directory contains dummy CSV files for testing the poller worker service.

## Files

- `sample_data_1.csv`: Sample employee data
- `sample_data_2.csv`: Sample company/employee records
- `sample_data_3.csv`: Sample product inventory data

## Usage

The poller worker will automatically discover and process these files when:
- A polling job is received with `source_type: "dummy"`
- The `path` in source_config points to this directory (or is not specified, using default)

## Adding More Files

Simply add more CSV files to this directory. The poller worker will discover them based on the file pattern specified in the polling job (default: `*.csv`).

