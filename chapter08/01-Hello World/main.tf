terraform {

  required_providers {

    databricks = {

      	source = "databricks/databricks"

    }

  }

}

data "databricks_current_user" "me" {}

variable "notebook_name" {

  description = "The name of the notebook."

  Type = string

} 

resource "databricks_notebook" "hello_world" {

  path = "${data.databricks_current_user.me.home}/${var.notebook_name}"

  language = "Python"

  source = "./${var.notebook_name}"

}

output "notebook_url" {

 value = databricks_notebook.hello_world.url

}
