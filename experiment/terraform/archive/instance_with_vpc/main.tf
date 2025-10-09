variable "key_name" {
  type = string
}

variable "security_group_id" {
  type = string
}

variable "subnet_id" {
  type = string
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

resource "aws_instance" "instance" {
  ami                    = data.aws_ami.amazon_linux.id
  instance_type          = "m5.xlarge"
  key_name               = var.key_name
  vpc_security_group_ids = [var.security_group_id]
  subnet_id              = var.subnet_id

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
