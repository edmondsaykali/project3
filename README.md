# Coding Challenge

This Code assessment represents a coding challenge for Data Engineering roles.

## Purpose

- Evaluate your coding abilities and your software engineering skills
- Judge your technical experience
- Understand how you design the solution 
- Have a technical coversation once the code submission is done

## Challenge 

In this challenge, we will use a dataset having the current job posting hosted by the City of New york which is available on the City of New York's official jobs site (https://www1.nyc.gov/jobs/index.page).

- Internal postings availble to city employees and external postings availbale to the public are included. 
- Data is accesible in the CSV file [nyc-jobs.csv](https://github.com/projectforyou/project1/dataset/nyc-jobs.csv)

This challenge is composed of following steps: 

### Data Exploration

- Provide a detailed analysis of source data: Column values (eg: Numerical vs character), categorical columns, etc. 
- List of KPIs to be resolved:
  - Whats the number of jobs posting per category (Top 10)? 
  - Whats the salary distribution per job category? 
  - Is there any correlation between the higher degree and the salary?
  - Whats the job posting having the highest salary per agency? 
  - Whats the job positings average salary per agency for the last 2 years? 
  - What are the highest paid skills in the US market? 

### Data Processing
 
- Create functions to process your dataset (Cleaning, column pre-processing, data wrangling, transformation etc) 
- Apply atleast 3 feature engineering techniques 
- Features removal based on the exploration/ profiling.
- Store your processed data into a target file

## Expectations

- Above mentioned challenge
- Test cases 
- Code Comments 
- If any deployment to be done, proposals of the deployment steps
- If you had to trigger your code, please suggest your approach. 

## Coding Instructions

- PySpark should be used
- Feel free to use any libraries (you can use pip install, if needed)
- Use a visualization library to present your analysis results.
- Any learnings/ challenges/ considerations/ assumptions, please document in [MyDocument.md](https://github.com/projectforyou/project1/blob/main/MyDocument.md)

## Technical Support:

1. Clone this repository to your local (using HTTPS protocol).
     - **Access to the parent repo will be removed within 4 days from the day you are made as collaborator**.
2. Push the cloned repo into your our Github account.
     - **Please don't commit anything to the parent repo, everything should be done in your local repos**.
3. Follow the installation instructions in [INSTALL.md](https://github.com/projectforyou/project1/blob/main/INSTALL.md)
4. Open the Jupyter notebook home page, using the link similar to the one highlighted in the picture 

  ![jupyter-notebook](https://github.com/projectforyou/project1/blob/main/pictures/docker-compose.png)

5. Open the notebook (assesment_notebook.ipynb). 
6. Read the documentation and run the cells accordingly. Once you create a spark session you should be able to see application **pyspark-assesment** running in the Spark Master UI as below. 

  ![spark-application](https://github.com/projectforyou/project1/blob/main/pictures/spark-application.png)

By now you would have already have below things in place:

- Created Spark cluster with a master and 2 worker nodes.
- Notebook to start working having the basic libraries imported (Please feel free to import any new library, if required). 
- Datasets read that is required for your activity.
- Sample function within the notebook arriving at a basic KPI.
- Sample test case for testing the function. 

## Submission:
 
Once the activity is completed, commit/ push your code into your own GitHub repo. Please share the GitHub repository URL with hiring team, so they can review your work.

# Happy Coding!!
