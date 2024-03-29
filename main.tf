terraform {
  required_providers {
    docker = {
      source = "kreuzwerker/docker"
      version = "3.0.2"
    }
  }
}
provider "docker" {
  host = "tcp://localhost:2375"  # Assuming Docker is running on localhost
}
resource "docker_image" "mage_spark_image" {
  name = "mage_spark"
  build {
    context    = "."
    dockerfile = "Dockerfile"
  }
}
resource "docker_container" "mage_spark_container" {
  name  = "mage_spark"
  image = docker_image.mage_spark_image.name
  ports {
    internal = 6789
    external = 6789
  }
  ports {
    internal = 4040
    external = 4040
  }
  volumes {
    host_path      = "D:\\project"
    container_path = "/home/src"
  }
  env = ["SPARK_MASTER_HOST=local"]

  command = ["/app/run_app.sh", "mage", "start", "demo_project"]
}
