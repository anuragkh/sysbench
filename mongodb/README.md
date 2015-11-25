# Benchmarking MongoDB

## EC2 instance setup

* _Instance Type:_ c3.8xlarge instance
* _AMI Type:_ Amazon Linux AMI 2015.09.1 (HVM), SSD Volume Type - ami-60b6c60a
* Install MongoDB using the following commands:

```bash
echo "[mongodb-org-3.0]
name=MongoDB Repository
baseurl=https://repo.mongodb.org/yum/amazon/2013.03/mongodb-org/3.0/x86_64/
gpgcheck=0
enabled=1" | sudo tee /etc/yum.repos.d/mongodb-org-3.0.repo

sudo yum install -y mongodb-org
```

* You might need to mount one (or both) instance store volumes:

```bash
sudo mkdir /media/ephemeral1/
sudo mount /dev/xvdc /media/ephemeral1/
sudo chown ec2-user:ec2-user -R /media
sudo chmod a+w -R /media
```

* Update `/etc/mongod.conf` to use the instance store as data/log path.

```
...

systemLog:
  destination: file
  logAppend: true
  path: /media/ephemeral0/mongod.log

...

storage:
  dbPath: /media/ephemeral1
  journal:
    enabled: true

...

```

* Start the Elasticsearch service:

```
sudo service mongod start
```

## Loading the data

* Load the data using the bulk loader script at [`load/mongoload.sh`](load/mongoload.sh):

```bash
bash load/mongoload.sh load/sample/table.dat
```

## Running the benchmarks

### Latency

* Run the latency benchmark using the script at [`perf/mongolatency.py`](perf/mongolatency.py):

```bash
python load/mongolatency.py --benchtype search --queries perf/sample/queries
python load/mongolatency.py --benchtype get
```

### Throughput

* Run the throughput benchmark using the script at [`perf/mongothroughput.py`](perf/mongothroughput.py):

```bash
python load/mongothroughput.py --benchtype search --queries perf/sample/queries --numthreads 1
python load/mongothroughput.py --benchtype get --numthreads 1
```