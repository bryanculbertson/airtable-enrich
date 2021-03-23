# airtable-enrich

Helpers for adding data to airtable bases

## Usage

### Setup Python Env Once

1. `curl https://pyenv.run | bash`
1. `pyenv install`
1. `pipx install poetry`
1. `poetry env use 3.9.2`
1. `poetry install`
1. `export AIRTABLE_APIKEY=<your-airtable-apikey>`

### Run Commands

1. Add census columns to table:

```python
poetry run airtable-enrinch add-census \
  --base=<base-key> \
  --table=<table-name> \
  --lat=<column-lat> \
  --lng=<column=lng>
```
