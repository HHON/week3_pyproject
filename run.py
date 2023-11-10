import requests 
import json
import toml
from collections import ChainMap
import pandas as pd
# from dotenv import load_dotenv
import os
import boto3
import subprocess



def read_api(url):
    """
    Reads the API and returns the response
    """
    response = requests.get(url)
    return response.json()


if __name__=='__main__':
    app_config = toml.load('config.toml')
    url = app_config['api']['url']

    # read the API
    print('Reading the API...')
    data=read_api(url)
    print('API Reading Done!')


    # the company name
    print('Building the dataframe...')

    company_list = []
    for i in range(len(data['results'])):
          company_list.append(data['results'][i]['company']['name'])
    print(company_list)
    company_name = {'company':company_list}
    print(company_name)

    # locations
    location_list = [data['results'][i]['locations'][0]['name'] for i in range(len(data['results']))]
    location_name = {'location':location_list}
    print(location_list)

    # job name
    job_list = [data['results'][i]['name'] for i in range(len(data['results']))]
    job_name = {'job':job_list}
    print(job_list)

    # job type
    job_type_list = [data['results'][i]['type'] for i in range(len(data['results']))]
    job_type = {'job type':job_type_list}
    print(job_type_list)

    # publication date
    publication_date_list = [data['results'][i]['publication_date'] for i in range(len(data['results']))]
    publication_date = {'publication date': publication_date_list}
    print(publication_date_list)


    # merge the dictionaries with ChainMap and dict "from collections import ChainMap"
    data = dict(ChainMap(company_name, location_name, job_name, job_type, publication_date))
    df = pd.DataFrame.from_dict(data)
    # print(data)
    print(df)

    # update publication date to date
    df['publication date'] = df['publication date'].str[:10]

    # split location into city and country and drop location
    df['city'] = df['location'].str.split(',').str[0]
    df['country'] = df['location'].str.split(',').str[1]
    df.drop('location', axis=1, inplace=True)

    print(df)
    # save the dataframe to a csv file locally first
    df.to_csv('jobs.csv', index=False)
    print('datafrme saved to local file called jobs.csv')

    # read secret_access_key of AWS form the .env file
    print('uploading to AWS S3...')

  # use linux command to upload file to S3
    subprocess.run(['aws', 's3', 'cp', 'jobs.csv', 's3://week3-mini-pyproject/input/job.csv'])
  
    # Success.
    print('File uploading Done!')

    # load_dotenv()
    # access_key=os.getenv('access_key')
    # secret_access_key=os.getenv('secret_access_key')

    # # upload the csv file to AWS S3
    # bucket = app_config['aws']['bucket']
    # folder = app_config['aws']['folder']
    # s3 = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_access_key)
    # s3.upload_file('jobs.csv', bucket, folder+'jobs.csv')

    print('File uploading Done!')




