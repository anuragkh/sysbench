# Assumes server is running Ubuntu; modify if running on a different distribution
sudo apt-get update # sudo yum update

sudo mkdir -p /mnt/data
sudo chown elasticsearch:elasticsearch /mnt/data
sudo mkdir -p /mnt/log
sudo chown elasticsearch:elasticsearch /mnt/log

# Change location of elasticsearch.yml accordingly
echo "
node:
  name: elastic-${HOSTNAME}

path:
  data: /mnt/data
  logs: /mnt/log
	
discovery:
  type: ec2
  ec2:
    groups: elastic-benchmark
    any_group: true
    availability_zones: us-east-1b
	
cloud:
  aws:
    access_key: $AWS_ACCESS_KEY
    secret_key: $AWS_SECRET_KEY
		
index:
  number_of_shards: 40
  numer_of_replicas: 0
" | sudo tee -a /opt/bitnami/elasticsearch/config/elasticsearch.yml

sudo /opt/bitnami/elasticsearch/bin/plugin install cloud-aws
sudo /opt/bitnami/ctlscript.sh elasticsearch restart