# airtable-enrich

Helpers for adding data to airtable bases

## Usage

### Setup Environment Once

1. Install required system deps (Debian):

    ```sh
    sudo apt-get install libbz2-dev liblzma-dev
    ```

1. Install `pyenv`:

    ```sh
    curl https://pyenv.run | bash
    ```

1. Add to `.bashrc`:

    ```sh
    export PATH="/home/codespace/.pyenv/bin:$PATH"
    eval "$(pyenv init -)"
    eval "$(pyenv virtualenv-init -)"
    ```

1. Install project python version:

    ```sh
    pyenv install
    ```

1. Install `poetry`:

    ```sh
    curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py | python -
    ```

1. Add to `.bashrc`:

    ```sh
    export PATH="$HOME/.poetry/bin:$PATH"
    ```

1. Install app dependancies:

    ```sh
    poetry install
    ```

1. (optional) For development:

    ```sh
    poetry install --extras lint
    ```

1. Add your airtable api key to `.env`

### Run Commands

1. Add census columns to table:

    ```python
    poetry run airtable-enrich/cli.py fill-census \
      --base=<base-key> \
      --table=<table-name> \
      --lat=<column-lat> \
      --lng=<column-lng> \
      --tract=<column-tract>
    ```
