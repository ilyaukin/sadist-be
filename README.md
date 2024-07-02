# Backend for [my-handicapped-pet.io](https://my-handicapped-pet.io)

## Configure dev env

### Python

- Install Python >=3.7 and <=3.9.

- Create virtual env: `python -m venv venv`

- Activate virtual env: `. venv/bin/activate`

- Install requirements: `pip install -r requirements.txt --extra-index-url http://my-handicapped-pet.io:3141/ilyaukin/dev --trusted-host my-handicapped-pet.io`

- Get static files from sadist-fe repo.
    - Make a directory:
    ```
    cd app
    mkdir static
    ```
    - In sadist-fe directory, which is supposed to be
    at the same level as sadist-be, run `npm run build-n-copy`

- Install app package locally
```
cd app/
pip install -e .
```

- Run app: `python run.py`


### MongoDB

Must be installed as a replica set.

MacOS: [Instruction](https://medium.com/@OndrejKvasnovsky/mongodb-replica-set-on-local-macos-f5fc383b3fd6)
<br/>
(TODO: make a script)


### nginx

If you are running solo backend application, you can skip nginx
installation and use the app serving at default Uvicorn's port 8000.

However, to run web crawler, for instance, `sadist-proxy` service
is needed. So nginx should be configured like in prod.


## Environments, deployment, CI/CD

Deployment to staging/prod is done by docker compose.
Configuration is in the [sadist-ci](https://github.com/ilyaukin/sadist-ci) repo.

## Tests

- There are two ways to run tests - using a real mongo instance
or a `mongomock` library... <br/>In the former case you should run a mongo
DB (see above).<br/>
To run test using mongomock `export USE_MONGOMOCK=1`.
- Install test requirements: `pip install -r requirements-test.txt --extra-index-url http://my-handicapped-pet.io:3141/ilyaukin/dev --trusted-host my-handicapped-pet.io`
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

### Drop orphan collections
```javascript
_ids = db.ds_list.find().map(rec => rec._id + '')
for (let n of db.getCollectionNames()) { if(/^ds_[a-f0-9]{24}(!?_classification)?$/.test(n)) { const m = _ids.find(_id => n.indexOf(_id) !== -1); if (!m) { db[n].drop() }; }  }
```
