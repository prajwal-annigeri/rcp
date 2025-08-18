variable "key_name" {
  type = string
}

variable "security_group_id" {
  type = string
}

variable "availability_zone" {
  type    = string
  default = ""  # Leave empty to skip
}

data "aws_ami" "amazon_linux" {
  most_recent = true
  owners      = ["amazon"]

  filter {
    name   = "name"
    values = ["amzn2-ami-hvm-*-x86_64-gp2"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

data "aws_subnet" "default_for_az" {
  count = var.availability_zone != "" ? 1 : 0

  filter {
    name   = "default-for-az"
    values = ["true"]
  }

  filter {
    name   = "availabilityZone"
    values = [var.availability_zone]
  }
}

resource "aws_instance" "instance" {
  ami                    = data.aws_ami.amazon_linux.id
  instance_type          = "m5.xlarge"
  key_name               = var.key_name
  vpc_security_group_ids = [var.security_group_id]
  subnet_id              = var.availability_zone != "" ? data.aws_subnet.default_for_az[0].id : null

  tags = {
    Name = "ec2-research"
  }
}

output "public_ip" {
  description = "Public IP of EC2 instance"
  value = aws_instance.instance.public_ip
}

output "private_ip" {
  description = "Private IP of EC2 instance"
  value = aws_instance.instance.private_ip
}
