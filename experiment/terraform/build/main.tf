provider "aws" {
  region = "us-east-1"
}

module "security_region_1" {
  source = "./../modules/security"
  providers = { aws = aws }
}

module "instance_build" {
  source    = "./../modules/instance"
  providers = { aws = aws }
  key_name = module.security_region_1.key_name
  security_group_id = module.security_region_1.security_group_id
}

output "public_ip" {
  description = "Public IP of EC2 instance"
  value = module.instance_build.public_ip
}
