Installation Instructions
-- 
IMPORTANT: Any installation option requires at least 8Gb of RAM for proper operation.


Using Windows:
--

Install docker desktop https://docs.docker.com/desktop/install/windows-install/

Open the power shell and trigger below lines of the code

	cd <parent-directory-where-you-would-clone-the-Git-branch>
	
	git clone https://github.com/projectforyou/project1.git

	docker compose -f ./project1/docker-compose.yml --project-name my_assesment up




Using MacOS:
--

Follow instructions in the below link to setup your MacOS https://arjon.es/2019/setting-up-macbook-pro-for-development/

Open the shell and run the below line 
	
	cd <parent-directory-where-you-would-clone-the-Git-branch>
	
	git clone git@github.com:projectforyou/project1.git

	docker compose -f ./project1/docker-compose.yml --project-name my_assesment up


	


