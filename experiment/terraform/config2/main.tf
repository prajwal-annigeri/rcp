module "security_region_1" {
  source = "./../modules/security"
  providers = { aws = aws.region_1 }
}

module "security_region_2" {
  source = "./../modules/security"
  providers = { aws = aws.region_2 }
}

module "security_region_3" {
  source = "./../modules/security"
  providers = { aws = aws.region_3 }
}

module "security_region_4" {
  source = "./../modules/security"
  providers = { aws = aws.region_4 }
}

module "security_region_5" {
  source = "./../modules/security"
  providers = { aws = aws.region_5 }
}

module "instance_1" {
  source    = "./../modules/instance"
  providers = { aws = aws.region_1 }
  key_name = module.security_region_1.key_name
  security_group_id = module.security_region_1.security_group_id

  depends_on = [module.security_region_1]
}

module "instance_2" {
  source    = "./../modules/instance"
  providers = { aws = aws.region_1 }
  key_name = module.security_region_1.key_name
  security_group_id = module.security_region_1.security_group_id

  depends_on = [module.security_region_1]
}

module "instance_3" {
  source    = "./../modules/instance"
  providers = { aws = aws.region_2 }
  key_name = module.security_region_2.key_name
  security_group_id = module.security_region_2.security_group_id

  depends_on = [module.security_region_2]
}

module "instance_4" {
  source    = "./../modules/instance"
  providers = { aws = aws.region_2 }
  key_name = module.security_region_2.key_name
  security_group_id = module.security_region_2.security_group_id

  depends_on = [module.security_region_2]
}

module "instance_5" {
  source    = "./../modules/instance"
  providers = { aws = aws.region_3 }
  key_name = module.security_region_3.key_name
  security_group_id = module.security_region_3.security_group_id

  depends_on = [module.security_region_3]
}

module "instance_6" {
  source    = "./../modules/instance"
  providers = { aws = aws.region_3 }
  key_name = module.security_region_3.key_name
  security_group_id = module.security_region_3.security_group_id

  depends_on = [module.security_region_3]
}

module "instance_7" {
  source    = "./../modules/instance"
  providers = { aws = aws.region_4 }
  key_name = module.security_region_4.key_name
  security_group_id = module.security_region_4.security_group_id

  depends_on = [module.security_region_4]
}

module "instance_client" {
  source    = "./../modules/instance"
  providers = { aws = aws.region_5 }
  key_name = module.security_region_5.key_name
  security_group_id = module.security_region_5.security_group_id

  depends_on = [module.security_region_5]
}

output "client_ip" {
  description = "Public IP of client instance"
  value = module.instance_client.public_ip
}

output "public_ips" {
  description = "Public IPs of all server instances"
  value = [
    module.instance_1.public_ip,
    module.instance_2.public_ip,
    module.instance_3.public_ip,
    module.instance_4.public_ip,
    module.instance_5.public_ip,
    module.instance_6.public_ip,
    module.instance_7.public_ip
  ]
}

output "private_ips" {
  description = "Private IPs of all server instances"
  value = [
    module.instance_1.private_ip,
    module.instance_2.private_ip,
    module.instance_3.private_ip,
    module.instance_4.private_ip,
    module.instance_5.private_ip,
    module.instance_6.private_ip,
    module.instance_7.private_ip
  ]
}
