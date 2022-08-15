Installation Instructions
-- 
IMPORTANT: Any installation option requires at least 8Gb of RAM for proper operation.


Using Windows:
--

1. Install docker desktop https://docs.docker.com/desktop/install/windows-install/
2. Make sure docker desktop is up and running.
3. Open the power shell and trigger below lines of the code

	cd {project1-directory}
	
	docker compose -f ./docker-compose.yml --project-name my_assesment up


Using MacOS:
--

1. Follow instructions in the below link to setup your MacOS https://arjon.es/2019/setting-up-macbook-pro-for-development/
2. Make sure docker desktop is up and running.
3. Open the shell and run the below line 
	
	cd {project1-directory}
	
	docker compose -f ./docker-compose.yml --project-name my_assesment up

