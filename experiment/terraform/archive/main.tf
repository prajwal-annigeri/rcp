resource "aws_networkmanager_global_network" "global" {
  description = "Global network for Cloud WAN"
}

data "aws_networkmanager_core_network_policy_document" "core_policy" {
  core_network_configuration {
    asn_ranges = ["64512-65534"]

    edge_locations {
      location = "us-east-1"
      asn      = 64512
    }

    edge_locations {
      location = "us-east-2"
      asn      = 64513
    }

    edge_locations {
      location = "us-west-1"
      asn      = 64514
    }

    edge_locations {
      location = "us-west-2"
      asn      = 64515
    }
  }

  segments {
    name = "global"
    require_attachment_acceptance = false
  }

  attachment_policies {
    rule_number = 100
    condition_logic = "or"

    conditions {
      type     = "tag-value"
      operator = "equals"
      key      = "Segment"
      value    = "global"
    }

    action {
      association_method = "constant"
      segment            = "global"
    }
  }
}

resource "aws_networkmanager_core_network" "core" {
  global_network_id    = aws_networkmanager_global_network.global.id
  base_policy_document = data.aws_networkmanager_core_network_policy_document.core_policy.json
  create_base_policy   = true

  depends_on = [aws_networkmanager_global_network.global]
}

module "security_region_1" {
  source          = "./../modules/security_with_vpc"
  cidr_block      = "10.1.0.0/16"
  core_network_id = aws_networkmanager_core_network.core.id
  core_network_arn = aws_networkmanager_core_network.core.arn
  providers       = { aws = aws.region_1 }

  depends_on = [aws_networkmanager_core_network.core]
}

module "security_region_2" {
  source          = "./../modules/security_with_vpc"
  cidr_block      = "10.2.0.0/16"
  core_network_id = aws_networkmanager_core_network.core.id
  core_network_arn = aws_networkmanager_core_network.core.arn
  providers       = { aws = aws.region_2 }

  depends_on = [aws_networkmanager_core_network.core]
}

module "security_region_3" {
  source          = "./../modules/security_with_vpc"
  cidr_block      = "10.3.0.0/16"
  core_network_id = aws_networkmanager_core_network.core.id
  core_network_arn = aws_networkmanager_core_network.core.arn
  providers       = { aws = aws.region_3 }

  depends_on = [aws_networkmanager_core_network.core]
}

module "security_region_4" {
  source          = "./../modules/security_with_vpc"
  cidr_block      = "10.4.0.0/16"
  core_network_id = aws_networkmanager_core_network.core.id
  core_network_arn = aws_networkmanager_core_network.core.arn
  providers       = { aws = aws.region_4 }

  depends_on = [aws_networkmanager_core_network.core]
}

module "instance_1" {
  source            = "./../modules/instance_with_vpc"
  providers         = { aws = aws.region_1 }
  key_name          = module.security_region_1.key_name
  security_group_id = module.security_region_1.security_group_id
  subnet_id         = module.security_region_1.subnet_id

  depends_on = [module.security_region_1]
}

module "instance_2" {
  source            = "./../modules/instance_with_vpc"
  providers         = { aws = aws.region_2 }
  key_name          = module.security_region_2.key_name
  security_group_id = module.security_region_2.security_group_id
  subnet_id         = module.security_region_2.subnet_id

  depends_on = [module.security_region_2]
}

module "instance_3" {
  source            = "./../modules/instance_with_vpc"
  providers         = { aws = aws.region_3 }
  key_name          = module.security_region_3.key_name
  security_group_id = module.security_region_3.security_group_id
  subnet_id         = module.security_region_3.subnet_id

  depends_on = [module.security_region_3]
}

module "instance_4" {
  source            = "./../modules/instance_with_vpc"
  providers         = { aws = aws.region_4 }
  key_name          = module.security_region_4.key_name
  security_group_id = module.security_region_4.security_group_id
  subnet_id         = module.security_region_4.subnet_id

  depends_on = [module.security_region_4]
}

module "instance_client" {
  source            = "./../modules/instance_with_vpc"
  providers         = { aws = aws.region_1 }
  key_name          = module.security_region_1.key_name
  security_group_id = module.security_region_1.security_group_id
  subnet_id         = module.security_region_1.subnet_id

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
    module.instance_4.public_ip
  ]
}

output "private_ips" {
  description = "Private IPs of all server instances"
  value = [
    module.instance_1.private_ip,
    module.instance_2.private_ip,
    module.instance_3.private_ip,
    module.instance_4.private_ip
  ]
}
