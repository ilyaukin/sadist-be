# Backend

## Configure dev env

### Python

- Install Python 3.6+.

- Create virtual env: `python -m venv venv`

- Activate virtual env: `. venv/bin/activate`

- Install requirements: `pip install -r requirements.txt`

- Get static files from sadist-fe repo.
    - Make a directory:
    ```
    cd app
    mkdir static
    ```
    - In sadist-fe directory, which is supposed to be
    at the same level as sadist-be, run `npm run build`

- Install app package locally
```
cd app/
pip install -e .
```

- Run app: `./run.sh`


### MongoDB

Must be installed as a replica set.

MacOS: [Instruction](https://medium.com/@OndrejKvasnovsky/mongodb-replica-set-on-local-macos-f5fc383b3fd6)
<br/>
(TODO: make a script)
