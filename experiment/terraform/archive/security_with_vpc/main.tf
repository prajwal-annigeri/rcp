variable "core_network_id" {
  type = string
}

variable "core_network_arn" {
  type = string
}

variable "cidr_block" {
  type = string
}

resource "aws_vpc" "this" {
  cidr_block = var.cidr_block

  tags = {
    Name = "vpc-research"
  }
}

resource "aws_subnet" "this" {
  vpc_id                  = aws_vpc.this.id
  cidr_block              = cidrsubnet(var.cidr_block, 8, 0)
  availability_zone       = data.aws_availability_zones.available.names[0]
  map_public_ip_on_launch = true

  tags = {
    Name = "subnet-research"
  }
}

data "aws_availability_zones" "available" {}

resource "aws_internet_gateway" "this" {
  vpc_id = aws_vpc.this.id
  
  tags = {
    Name = "igw-research"
  }
}

resource "aws_route_table" "this" {
  vpc_id = aws_vpc.this.id
}

resource "aws_route" "core_network" {
  route_table_id         = aws_route_table.this.id
  destination_cidr_block = "10.0.0.0/8"
  core_network_arn       = var.core_network_arn
}

resource "aws_route" "default" {
  route_table_id         = aws_route_table.this.id
  destination_cidr_block = "0.0.0.0/0"
  gateway_id             = aws_internet_gateway.this.id
}

resource "aws_route_table_association" "subnet_assoc" {
  subnet_id      = aws_subnet.this.id
  route_table_id = aws_route_table.this.id
}

resource "aws_security_group" "security_group" {
  name        = "allow-ssh-8080"
  description = "Allow 8080 and SSH"
  vpc_id      = aws_vpc.this.id

  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    from_port   = 8080
    to_port     = 8080
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    from_port   = -1
    to_port     = -1
    protocol    = "icmp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_networkmanager_vpc_attachment" "this" {
  core_network_id = var.core_network_id
  subnet_arns     = [aws_subnet.this.arn]
  vpc_arn         = aws_vpc.this.arn

  tags = {
    Name    = "cloudwan-attachment"
    Segment = "global"
  }

  depends_on = [aws_internet_gateway.this]
}

resource "aws_key_pair" "key" {
  key_name   = "key"
  public_key = file("${path.module}/../../../key.pub")
}

output "key_name" {
  description = "Key pair name of the region"
  value = aws_key_pair.key.key_name
}

output "security_group_id" {
  description = "Security group ID of the region"
  value = aws_security_group.security_group.id
}

output "subnet_id" {
  value = aws_subnet.this.id
}

output "vpc_id" {
  value = aws_vpc.this.id
}

output "attachment_id" {
  value = aws_networkmanager_vpc_attachment.this.id
}
