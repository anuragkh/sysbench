# Benchmarking Elasticsearch

## EC2 instance setup

* c3.8xlarge instance
* Amazon Linux AMI 2015.09.1 (HVM), SSD Volume Type - ami-60b6c60a
* Install Elasticsearch using the following commands:

```bash
\# Add GPG key
sudo rpm --import https://packages.elastic.co/GPG-KEY-elasticsearch

\# Add repository
echo "[elasticsearch-2.x]
name=Elasticsearch repository for 2.x packages
baseurl=http://packages.elastic.co/elasticsearch/2.x/centos
gpgcheck=1
gpgkey=http://packages.elastic.co/GPG-KEY-elasticsearch
enabled=1" | sudo tee /etc/yum.repos.d/elasticsearch.repo

\# Install elasticsearch
sudo yum install -y elasticsearch
```
