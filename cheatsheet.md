### install monogodb on a computer, which doesn't support AVX
```shell
git clone git@github.com:GermanAizek/mongodb-without-avx.git
cd mongodb-without-avx/
git submodule init
git submodule update
cd mongo/
git fetch origin r7.0.6
git checkout r7.0.6
sudo apt install build-essential
git apply ../o3_patch.diff
sudo apt install libcurl4-openssl-dev
sudo apt install liblzma-dev
python3 -m venv venv
. venv/bin/activate
python -m pip install -r etc/pip/compile-requirements.txt
sudo apt install python-dev-is-python3 libssl-dev
sudo mkdir /opt/mongo
sudo chown ilya /opt/mongo/
python buildscripts/scons.py DESTDIR=/opt/mongo install-mongod --disable-warnings-as-errors --linker=gold
# wait ~8h...
```

### release python package
```shell
# don't forget to manually update version in setup.cfg
export RELEASE=x.x
git add setup.cfg
git commit --allow-empty -m "RELEASE $RELEASE"
git tag -a $RELEASE -m "version $RELEASE"
git push origin --tags
```

### copy a certain collection from local to prod
```shell
mongodump -d sadist -c <collection name> -o sadist-dump
mongorestore -u kzerby -p <password> -d sadist "mongodb+srv://cluster1.yoka1fj.mongodb.net/" sadist-dump/sadist/
```

### run script on a remote container
```shell
export DOCKER_HOST=ssh://ec2-user@ec2-54-201-237-197.us-west-2.compute.amazonaws.com
export 'DATABASE_URL=<copy connection from atlas, put password and database>'
docker run -it -e DATABASE_URL myhandicappedpet/webapp-flask python -m scripts.classification --help
```