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


## Deployment

Deployment to staging/prod is done by docker compose.
Configuration is in the [sadist-ci](https://github.com/my-handicapped-pet/sadist-ci) repo.


### Environments

Prod and staging are hosted at the same AWS EC2 instance. To separate,
compose project is used. Staging is mapped to :8080, :8043 ports.


### Deployment via docker compose locally

1. Build all images that need to be updated. E.g., for webapp-flask:
```shell
docker build -f Dockerfile-flask -t myhandicappedpet/webapp-flask .
docker tag myhandicappedpet/webapp-flask myhandicappedpet/webapp-flask:latest
```
2. Create volumes `prod_certbot_data`, `prod_ssl_certs` and copy certs from 
the prod. This has to be done once a while per a developer machine, while
the certs are valid.
  - Create volumes:
  ```shell
  docker volume create prod_certbot_www
  docker volume create prod_ssl_certs
  ```
  - SSH to the prod. Copy prod certs to /tmp and give permissions:
  ```shell
  mkdir /tmp/certs
  sudo cp -r /var/lib/docker/volumes/prod_ssl_certs/_data/archive/my-handicapped-pet.io /tmp/certs/
  sudo chown -R ec2-user:ec2-user /tmp/certs/
  ```
  - Locally, copy certs from the remote and create symlinks:
  ```shell
  scp -r  ec2-user@my-handicapped-pet.io:/tmp/certs/my-handicapped-pet.io/ /tmp/
  sudo mkdir  /var/lib/docker/volumes/prod_ssl_certs/_data/archive
  sudo mkdir  /var/lib/docker/volumes/prod_ssl_certs/_data/live
  sudo mkdir  /var/lib/docker/volumes/prod_ssl_certs/_data/live/my-handicapped-pet.io
  sudo -i
  cd /var/lib/docker/volumes/prod_ssl_certs/_data/live/my-handicapped-pet.io/
  # change number if needed
  ln -s ../../archive/my-handicapped-pet.io/cert8.pem cert.pem
  ln -s ../../archive/my-handicapped-pet.io/chain8.pem chain.pem
  ln -s ../../archive/my-handicapped-pet.io/fullchain8.pem fullchain.pem
  ln -s ../../archive/my-handicapped-pet.io/privkey8.pem privkey.pem
  ```
  - In the prod, remove /tmp certs:
  ```shell
  rm -rf /tmp/certs/
  ```
3. Edit `/etc/hosts` to point my-handicapped-pet.io to localhost.
4. The next steps must be done in the sadist-ci repo which contains compose
configuration (I just keep all instructions here in one place).
5. Export .env with the secrets used for blog app now. (Further we may want
to improve the deployment by using common mechanism for providing all services
with env, as well as service monitoring, scaling, restarting and re-configuring
on fly, but for now it's a little bit specific for each service)
```shell
echo -e '<the secret secret>' > .env
```
6. Export proper environment variables. Run with .dev compose file.
```shell
export ENV=dev
export DATABASE_URL=mongodb://root:password@mongo:27017/sadist
docker compose -f docker-compose.yml -f docker-compose.dev.yml up -d
```
7. Import DB, this also has to be done once a while.
```shell
docker run -it --network sadist-ci_default -v <path to the dump>:/dump mongo:latest mongorestore -d sadist --authenticationDatabase admin mongodb://root:password@mongo:27017/ /dump
```
8. To shut down, run
```shell
docker compose -f docker-compose.yml -f docker-compose.dev.yml down
```


### CI/CD

Deployment to staging is triggered by push to `develop`. Each repo triggers
the same workflow in sadist-ci, but with different list of images to rebuild.
Results, however, will be under Actions in the corresponding repo.

Deployment to prod is triggered manually always from sadist-ci. After deployment,
code is automatically merged to `master`.


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


## AI usage guidelines

Below are guidelines for the usage of AI coding agents in the project. They are
written with Junie and JetBrains IDEs in mind, but keep 
them tool-agnostic where possible.

### Suggested process

The `.junie` directory acts as the "long-term memory" for AI agents. Since agents have a limited context window, this directory allows them to stay consistent across multiple sessions.

- **`.junie/memory/tasks.md`**: Use this to track the high-level roadmap and the status of specific sub-tasks. It helps the agent know "what's next" without being told every time.
- **`.junie/memory/errors.md`**: Record project-specific "gotchas," recurring bugs, or specific coding standards that the agent should keep in mind.
- **`.junie/plans/`**: For complex tasks, ask the agent to write a plan here first. This allows you to review the strategy before execution and provides a reference if the session needs to be restarted.

**Recommended Workflow:**
1. At the start of a task, ask the agent to read `.junie/memory/`.
2. For large tasks, ask the agent to create/update a plan in `.junie/plans/`.
3. After completing work, ensure the agent updates `tasks.md` with the latest progress.

### Q&A

#### What 'New Chat' does? What is shared and what is not between chats?

'New Chat' starts a fresh conversation with a clean context window.
- **Shared:** The codebase index (file structure, symbol search) and any files on disk (including `.junie`).
- **Not shared:** The specific history of previous messages and any "mental state" the agent had during that session. Use `.junie` files to bridge important information between chats.

#### Can the context be shared between projects and different JetBrains products?

No, agent sessions are typically scoped to the current IDE project. If you have PyCharm for backend and WebStorm for frontend, they won't see each other's chat history.
- **Workaround:** If you need to make cross-repo changes, try opening both projects in a single IDE window (e.g., by opening the parent directory) or manually copy-paste relevant context (like API definitions) into the chat.

#### How to keep chat history between IDE runs?

The IDE generally persists chat history in its internal database. However, chat history can become bloated and confusing for the agent over time. It is better to rely on `.junie/memory/` for critical project state rather than old chat logs.

#### How to keep allowed commands between IDE runs?

Permissions (e.g., allowing the agent to run `bash` or `pytest`) are usually stored in the IDE's AI settings. You can often configure these to be "Always allowed" for a specific project to reduce interruptions.

#### I have a scrolling issue

Sometimes the chat window doesn't automatically scroll down when the agent is generating a long response or waiting for your approval.
- **Solution:** Manually scroll to the bottom if you see the "Thinking..." indicator but no new text, or look for a "Scroll to bottom" arrow in the chat UI.
