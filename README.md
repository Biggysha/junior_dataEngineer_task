# junior_dataEngineer_task
the first project of junior_dataEngineer to polish skills 

#to set the environment with the docker
#i just test with the localhost with docker compose
docker compose up -d 

#to set up the gcp with terraform
terraform init
terraform plan
terraform apply

#sample dataset 
#i used the openai text dataset that is destined for to mathematics Q&A
from datasets import load_dataset

ds = load_dataset("openai/gsm8k", "main")

#pipeline architecture

