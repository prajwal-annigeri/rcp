module "security_region_1" {
  source = "./../modules/security"
  providers = { aws = aws.region_1 }
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
  providers = { aws = aws.region_1 }
  key_name = module.security_region_1.key_name
  security_group_id = module.security_region_1.security_group_id

  depends_on = [module.security_region_1]
}

module "instance_4" {
  source    = "./../modules/instance"
  providers = { aws = aws.region_1 }
  key_name = module.security_region_1.key_name
  security_group_id = module.security_region_1.security_group_id

  depends_on = [module.security_region_1]
}

module "instance_5" {
  source    = "./../modules/instance"
  providers = { aws = aws.region_1 }
  key_name = module.security_region_1.key_name
  security_group_id = module.security_region_1.security_group_id

  depends_on = [module.security_region_1]
}

module "instance_6" {
  source    = "./../modules/instance"
  providers = { aws = aws.region_1 }
  key_name = module.security_region_1.key_name
  security_group_id = module.security_region_1.security_group_id

  depends_on = [module.security_region_1]
}

module "instance_7" {
  source    = "./../modules/instance"
  providers = { aws = aws.region_1 }
  key_name = module.security_region_1.key_name
  security_group_id = module.security_region_1.security_group_id

  depends_on = [module.security_region_1]
}

module "instance_8" {
  source    = "./../modules/instance"
  providers = { aws = aws.region_1 }
  key_name = module.security_region_1.key_name
  security_group_id = module.security_region_1.security_group_id

  depends_on = [module.security_region_1]
}

module "instance_9" {
  source    = "./../modules/instance"
  providers = { aws = aws.region_1 }
  key_name = module.security_region_1.key_name
  security_group_id = module.security_region_1.security_group_id

  depends_on = [module.security_region_1]
}

module "instance_10" {
  source    = "./../modules/instance"
  providers = { aws = aws.region_1 }
  key_name = module.security_region_1.key_name
  security_group_id = module.security_region_1.security_group_id

  depends_on = [module.security_region_1]
}

module "instance_11" {
  source    = "./../modules/instance"
  providers = { aws = aws.region_1 }
  key_name = module.security_region_1.key_name
  security_group_id = module.security_region_1.security_group_id

  depends_on = [module.security_region_1]
}

module "instance_12" {
  source    = "./../modules/instance"
  providers = { aws = aws.region_1 }
  key_name = module.security_region_1.key_name
  security_group_id = module.security_region_1.security_group_id

  depends_on = [module.security_region_1]
}

module "instance_13" {
  source    = "./../modules/instance"
  providers = { aws = aws.region_1 }
  key_name = module.security_region_1.key_name
  security_group_id = module.security_region_1.security_group_id

  depends_on = [module.security_region_1]
}

module "instance_14" {
  source    = "./../modules/instance"
  providers = { aws = aws.region_1 }
  key_name = module.security_region_1.key_name
  security_group_id = module.security_region_1.security_group_id

  depends_on = [module.security_region_1]
}

module "instance_15" {
  source    = "./../modules/instance"
  providers = { aws = aws.region_1 }
  key_name = module.security_region_1.key_name
  security_group_id = module.security_region_1.security_group_id

  depends_on = [module.security_region_1]
}

module "instance_16" {
  source    = "./../modules/instance"
  providers = { aws = aws.region_1 }
  key_name = module.security_region_1.key_name
  security_group_id = module.security_region_1.security_group_id

  depends_on = [module.security_region_1]
}

module "instance_17" {
  source    = "./../modules/instance"
  providers = { aws = aws.region_1 }
  key_name = module.security_region_1.key_name
  security_group_id = module.security_region_1.security_group_id

  depends_on = [module.security_region_1]
}

module "instance_18" {
  source    = "./../modules/instance"
  providers = { aws = aws.region_1 }
  key_name = module.security_region_1.key_name
  security_group_id = module.security_region_1.security_group_id

  depends_on = [module.security_region_1]
}

module "instance_19" {
  source    = "./../modules/instance"
  providers = { aws = aws.region_1 }
  key_name = module.security_region_1.key_name
  security_group_id = module.security_region_1.security_group_id

  depends_on = [module.security_region_1]
}

module "instance_20" {
  source    = "./../modules/instance"
  providers = { aws = aws.region_1 }
  key_name = module.security_region_1.key_name
  security_group_id = module.security_region_1.security_group_id

  depends_on = [module.security_region_1]
}

module "instance_21" {
  source    = "./../modules/instance"
  providers = { aws = aws.region_1 }
  key_name = module.security_region_1.key_name
  security_group_id = module.security_region_1.security_group_id

  depends_on = [module.security_region_1]
}

module "instance_22" {
  source    = "./../modules/instance"
  providers = { aws = aws.region_1 }
  key_name = module.security_region_1.key_name
  security_group_id = module.security_region_1.security_group_id

  depends_on = [module.security_region_1]
}

module "instance_23" {
  source    = "./../modules/instance"
  providers = { aws = aws.region_1 }
  key_name = module.security_region_1.key_name
  security_group_id = module.security_region_1.security_group_id

  depends_on = [module.security_region_1]
}

module "instance_client" {
  source    = "./../modules/instance"
  providers = { aws = aws.region_1 }
  key_name = module.security_region_1.key_name
  security_group_id = module.security_region_1.security_group_id

  depends_on = [module.security_region_1]
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
    module.instance_7.public_ip,
    module.instance_8.public_ip,
    module.instance_9.public_ip,
    module.instance_10.public_ip,
    module.instance_11.public_ip,
    module.instance_12.public_ip,
    module.instance_13.public_ip,
    module.instance_14.public_ip,
    module.instance_15.public_ip,
    module.instance_16.public_ip,
    module.instance_17.public_ip,
    module.instance_18.public_ip,
    module.instance_19.public_ip,
    module.instance_20.public_ip,
    module.instance_21.public_ip,
    module.instance_22.public_ip,
    module.instance_23.public_ip
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
    module.instance_7.private_ip,
    module.instance_8.private_ip,
    module.instance_9.private_ip,
    module.instance_10.private_ip,
    module.instance_11.private_ip,
    module.instance_12.private_ip,
    module.instance_13.private_ip,
    module.instance_14.private_ip,
    module.instance_15.private_ip,
    module.instance_16.private_ip,
    module.instance_17.private_ip,
    module.instance_18.private_ip,
    module.instance_19.private_ip,
    module.instance_20.private_ip,
    module.instance_21.private_ip,
    module.instance_22.private_ip,
    module.instance_23.private_ip
  ]
}
