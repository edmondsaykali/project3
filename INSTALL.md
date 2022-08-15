Installation Instructions
-- 
IMPORTANT: Any installation option requires at least 8Gb of RAM for proper operation.

1. Docker Desktop

	Using Windows: Install docker desktop https://docs.docker.com/desktop/install/windows-install/
	Using MacOS: Follow instructions in the below link to setup your MacOS https://arjon.es/2019/setting-up-macbook-pro-for-development/

2. Ensure docker desktop is up and running by following below validations:
	
	Setup Validation - [SETUP_VALIDATION.md](https://github.com/projectforyou/project1/blob/main/SETUP_VALIDATION.md) 

3. Spinup Spark cluster: 
	a. Open shell and run the below lines
	b. Change directory to the /project1
		cd {project1-directory}
	c. Docker compose
		docker compose -f ./docker-compose.yml --project-name my_assesment up
