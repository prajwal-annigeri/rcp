resource "aws_security_group" "security_group" {
  name        = "allow-ssh-http"
  description = "Allow HTTP (8080, 7080) and SSH"

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
    from_port   = 7080
    to_port     = 7080
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
