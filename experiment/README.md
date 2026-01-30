# How to run the experiment

## Setup key pair and AWS key

You only need to run this steps once.

We first need to setup a private/public key pair to SSH into the instances. Use the same key pair for all instances for convenience.
1. Starting from the root directory.
2. Run `./keygen.sh` to create private key (`key`) and public key (`key.pub`) pair.

Then, we need to add our AWS access key ID and secret access key to use Terraform.
3. Set your AWS access key ID `export AWS_ACCESS_KEY_ID=[YOUR_ACCESS_KEY_ID]`.
4. Set your AWS secret access key `export AWS_SECRET_ACCESS_KEY=[YOUR_SECRET_ACCESS_KEY]`.

Example:
```
./keygen.sh
export AWS_ACCESS_KEY_ID=MYAWSACCESSKEYID
export AWS_SECRET_ACCESS_KEY=MYAWSSECRETACCESSKEY
```

## Building executable file

You only need to run this steps once unless there's a change in the code.

After creating a key pair, we need to build the executable files for easier distribution.
1. Go to the build directory by running `cd terraform/build`.
2. Set your AWS access key ID and AWS secret access key if you haven't already, check the previous step on how to do this.
3. Initialize Terraform script `terraform init`.
4. Run Terraform script `terraform apply`. Take note of the output, it will show the public IP of the instance.
5. At this point, an AWS EC2 instance is created to build the code.
6. Go back to the root folder `cd ..`.
7. Run the build script `./build.sh [PUBLIC_IP] [CODE_ZIP_FILE_NAME] [YCSB_ZIP_FILE_NAME]`. This will build the code and download the executables in the root directory.
8. Files under the name of `app`, `failure-client`, and `go-ycsb` will appear after the build is done, these are the executable files.
9. Go back to the build directory `cd build` and destroy your AWS instance `terraform destroy`.

Note:
Make sure that the code zip output a folder with the same name without the `.zip` extension and contain the code with `main.go`.
If you don't use `terraform destroy`, you will have to delete 3 things: the EC2 instance, the security group, and the key pair. It's better to just use Terraform.

Example:
```
cd terraform/build
terraform init
terraform apply
cd ../..
./build.sh 192.168.0.0 rcp.zip go-ycsb-rcp.zip
cd terraform/build
terraform destroy
```

## Setup before the experiment

You need to run this steps to change the number of servers or the distribution of the servers.

After you have the executable file, it's time to setup the instances for the experiment.
1. Go to the terraform directory by running `cd terraform`.
2. Go to the desired config directory by running `cd [CONFIG_NUM]`.
3. Set your AWS access key ID and AWS secret access key if you haven't already, check the previous step on how to do this.
4. Initialize Terraform script `terraform init` and run it `terraform apply`.
5. Get the instance IPs as JSON file for further processing `terraform output -json > ./../../instance_ips.json`.
6. Go back to the root folder `cd ../..`.
7. Send the executable and network configuration to each instance by running `./setup.sh [USE_PRIVATE_IP]`. This script will also install tmux which will be used during the experiment.
8. Don't forget to do `terraform destroy` in the deploy directory to destroy the AWS resources.
9. Go back to the root folder `cd ..` and run `./cleanup.sh` to clean up the generated files.

Example:
```
cd terraform/config1
terraform init
terraform apply
terraform output -json > ./../../instance_ips.json
cd ../..
./setup.sh true
...run experiment
cd terraform/config1
terraform destroy
cd ..
./cleanup.sh
```

### Config numbers

| Number | Geodistributed | Count |
|--------|----------------|-------|
| 1      | No             | 7     |
| 2      | Yes, region    | 7     |
| 3      | Yes, AZ        | 7     |
| 4      | No             | 5     |
| 5      | No             | 11    |
| 6      | No             | 17    |
| 7      | No             | 23    |

## Running the experiment

You need to run this steps for each experiment.

1. Go to the run experiment directory by running `cd run`.
2. Run the experiment using `./run.sh --protocol [PROTOCOL] --K [K] --batch-low [BATCH_LOW] --batch-high [BATCH_HIGH] --client [CONCURRENT_CLIENT] --time [EXPERIMENT_TIME] --failure [None/LF/RF/LOF/ROF]`.
3. The result of the run will be in `out.txt`.

Example:
```
cd run
./run.sh --protocol rcp --K 2 --batch-low 256 --batch-high 512 --client 256 --time 30 --failure None
```

### Notes

For non-geodistributed, the best batch timeout is 2 at ~37k tps with private IP and ~35k with public IP.

For geodistributed with public IP, the best batch timeout is 4 at ~19k tps with the right leader, ~14-15k tps with not-so-right leader, ~8k with far leader

For geodistributed with private IP, the best batch timeout is 4 at ~13k tps with not-so-right leader

For geodistributed with 7 nodes, the best is batch size and client 512 at ~4k tps with client in us-east-1, leader in eu-west-1, need to double election and consensus timeout
Somehow if leader is near, i.e. us-west-1, leader election is triggered
Looks like with batch size and client 256 at ~2k tps, there might not be any problem

Total availability zone is 25 in North America
We can put all in 12 AZ and client in one other AZ, all in the US, 3 AZ per region

## Others

This setup is set to run on AWS Linux OS with the default user `ec2-user`. If you use different OS, you will need to change that. There also might be a slight difference in the build script, mainly the part where it install packages.
