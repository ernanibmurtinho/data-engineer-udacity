PYTHON=$(shell which python)

#################################################################################
# Clean                                                                         #
#################################################################################
clean:
	rm -rf .pytest_cache
	rm -rf release

#################################################################################
# Build pex                                                                     #
#################################################################################
build: clean
	mkdir -p release
	pex . -v --disable-cache -o release/datalakeingestion.pex --python=$(PYTHON)

#################################################################################
# Run pex                                                                       #
#################################################################################
run:
	$(PYTHON) release/datalake_ingestion.pex -m datalakeingestion.main


#################################################################################
# Execute the build Docker                                                      #
#################################################################################
docker:
	rm -rf release
	docker build . --target=result --output release

#################################################################################
# Execute the build Docker on AWS                                               #
#################################################################################
docker_aws:
	rm -rf release
	docker build -t pex_data_pipeline . --target=result --output release
	docker tag pex_data_pipeline:latest 434726952675.dkr.ecr.us-west-2.amazonaws.com/pex_data_pipeline:latest
	docker push 434726952675.dkr.ecr.us-west-2.amazonaws.com/pex_data_pipeline:latest

#################################################################################
# Copy to AWS bucket                                               #
#################################################################################
s3_aws:
	aws s3 cp release/datalakeingestion.pex s3://aws-deploy-ingestion/pex_job/datalakeingestion.pex --profile udacity

#################################################################################
# Create emr and run pex script                                               #
#################################################################################
emr_pex:
	aws emr create-cluster --termination-protected --applications Name=Hadoop Name=Hive Name=Hue Name=Spark --release-label emr-6.2.0 --ec2-attributes '{"InstanceProfile":"EMR_EC2_DefaultRole","SubnetId":"subnet-80f644f7","EmrManagedSlaveSecurityGroup":"sg-0ff0f5e6eb2c549b0","EmrManagedMasterSecurityGroup":"sg-09e1139fd1a431861"}' --log-uri 's3n://aws-logs-434726952675-us-west-2/elasticmapreduce/' --steps '[{"Args":["s3://aws-deploy-ingestion/pex_run/pex_executor_cp.sh","s3://aws-deploy-ingestion/pex_job/datalakeingestion.pex","HADOOP_HOME=/usr/lib/hadoop","SPARK_HOME=/usr/lib/spark","./datalakeingestion.pex","-m","datalakeingestion.main","-j","JobDataLakeIngestion"],"Type":"CUSTOM_JAR","ActionOnFailure":"TERMINATE_CLUSTER","Jar":"s3://us-east-1.elasticmapreduce/libs/script-runner/script-runner.jar","Properties":"","Name":"Pipeline ingestion runner"}]' --instance-groups '[{"InstanceCount":2,"BidPrice":"0.078","EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":32,"VolumeType":"gp2"},"VolumesPerInstance":2}]},"InstanceGroupType":"CORE","InstanceType":"m5.xlarge","Name":"Core - 2"},{"InstanceCount":1,"BidPrice":"0.078","EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":32,"VolumeType":"gp2"},"VolumesPerInstance":2}]},"InstanceGroupType":"MASTER","InstanceType":"m5.xlarge","Name":"Master - 1"}]' --configurations '[{"Classification":"hadoop-env","Properties":{},"Configurations":[{"Classification":"export","Properties":{"PYSPARK_PYTHON":"python3","PYSPARK_DRIVER_PYTHON":"python3"}}]}]' --auto-terminate --auto-scaling-role EMR_AutoScaling_DefaultRole --ebs-root-volume-size 10 --service-role EMR_DefaultRole --enable-debugging --name 'Pipeline_ingestion_job' --scale-down-behavior TERMINATE_AT_TASK_COMPLETION --region us-west-2 --profile udacity

#################################################################################3
# Deploy to prodution		                                                    #
#################################################################################
deploy: docker

#################################################################################
# Deploy to prodution aws                                                       #
#################################################################################
deploy_aws: docker_aws s3_aws emr_pex
