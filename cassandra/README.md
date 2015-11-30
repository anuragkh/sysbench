# Benchmarking Cassandra

## EC2 instance setup

* _Instance Type:_ c3.8xlarge instance
* _AMI Type:_ Amazon Linux AMI 2015.09.1 (HVM), SSD Volume Type - ami-60b6c60a
* Install Cassandra using the following commands:

```bash
echo "[datastax] 
name = DataStax Repo for Apache Cassandra
baseurl = http://rpm.datastax.com/community
enabled = 1
gpgcheck = 0" | sudo tee /etc/yum.repos.d/datastax.repo

sudo yum install -y dsc22 cassandra22-tools
```

* You might need to mount one (or both) instance store volumes:

```bash
sudo mkdir /media/ephemeral1/
sudo mount /dev/xvdc /media/ephemeral1/
sudo chown ec2-user:ec2-user -R /media
sudo chmod a+w -R /media
```

* Update `/etc/cassandra/conf/cassandra.yaml` to use the instance store as data/log path.

```
...

data_file_directories:
    - /media/ephemeral0/data
    - /media/ephemeral1/data

...

```

* Start the Cassandra service:

```
sudo service cassandra start
```

## Loading the data

* Generate the SSTables using the bulk loader script at [`load/bin/bulkloader`](load/bin/bulkloader) script:
```bash
bash load/bin/bulkloader load/sample/table.dat
```

* Load the data using the sstableloader tool:

```bash
sstableloader -d localhost data/bench/data
```

## Running the benchmarks

### Latency

* Run the latency benchmark using the script at [`perf/cassandralatency.py`](perf/cassandralatency.py):

```bash
python load/cassandralatency.py --benchtype search --queries perf/sample/queries
python load/cassandralatency.py --benchtype get
```

### Throughput

* Run the throughput benchmark using the script at [`perf/cassandrathroughput.py`](perf/cassandrathroughput.py):

```bash
python load/cassandrathroughput.py --benchtype search --queries perf/sample/queries --numthreads 1
python load/cassandrathroughput.py --benchtype get --numthreads 1
```
