provider "aws" {
  region = "us-east-1"
}

provider "aws" {
  alias  = "region_1"
  region = "us-east-1"
}

provider "aws" {
  alias  = "region_2"
  region = "us-east-2"
}

provider "aws" {
  alias  = "region_3"
  region = "us-west-1"
}

provider "aws" {
  alias  = "region_4"
  region = "us-west-2"
}
