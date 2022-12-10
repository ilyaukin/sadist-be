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


## Environments

Environments in docker are parameterized by multiple compose files ([see the docs](https://docs.docker.com/compose/extends/)).
- [`docker-compose.yml`](docker-compose.yml) is the base configuration.
- [`docker-compose.dev.yml`](docker-compose.dev.yml) - configuration for dev environment.
It runs the app on 0.0.0.0:80.
You can launch it by execution
```shell
docker compose -f docker-compose.yml -f docker-compose.dev.yml up --build
```
- [`docker-compose.prod.yml`](docker-compose.prod.yml) - configuration for prod. Beside of the app, it contains a network
for the blog served by the same nginx, which is maintained outside of this project.
You can launch it by
```shell
docker compose -f docker-compose.yml -f docker-compose.prod.yml up --build
```
- [`docker-compose.staging.yml`](docker-compose.staging.yml) - configuration for staging.
You can launch it by
```shell
export COMPOSE_PROJECT_NAME=staging && docker compose -f docker-compose.yml -f docker-compose.staging.yml up --build
```
The env variable is needed in order not to mess up with prod containers. 
The app is run on 0.0.0.0:8080


## Tests

- There are two ways to run tests - using a real mongo instance
or a `mongomock` library... <br/>In the former case you should run a mongo
DB (see above).<br/>
To run test using mongomock `export USE_MONGOMOCK=1`. Some tests
may fail in this case so far because not all features of mongo are
implemented in mongomock.
- Install test requirements: `pip install -r requirements-test.txt --extra-index-url http://my-handicapped-pet.io:3141/ilyaukin/dev --trusted-host my-handicapped-pet.io`
(note that we use custom devpi server with dev library version because of the bug in mongomock)
- Run `pytest`


## Useful scripts

### Remove non-active data sources
```javascript
ids = db.ds_list.find({ "status": { "$ne": "active" } }).toArray().map((rec) => rec._id)
ids.forEach((_id) => {
   if (db.getCollectionNames().indexOf('ds_' + _id.str) !== -1) { db['ds_' + _id.str].drop(); }
   if (db.getCollectionNames().indexOf('ds_' + _id.str + '_classification') !== -1) { db['ds_' + _id.str + '_classification'].drop(); }
   db.ds_list.deleteMany({ _id })
})
```