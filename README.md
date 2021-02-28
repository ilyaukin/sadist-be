# Backend

## Configure dev env

### Python

- Install Python 3.7+.

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


### Tests

- There are two ways to run tests - using a real mongo instance
or a `mongomock` library... <br/>In the former case you should run a mongo
DB (see above).<br/>
To run test using mongomock `export USE_MONGOMOCK=1`. Some tests
may fail in this case so far because not all features of mongo are
implemented in mongomock.
- Install test requirements: `pip install -r requirements-test.txt --extra-index-url http://my-handicapped-pet.io:3141/ilyaukin/dev --trusted-host my-handicapped-pet.io`
(note that we use custom devpi server with dev library version because of the bug in mongomock)
- Run `pytest`