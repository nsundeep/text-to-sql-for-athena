import boto3
from botocore.config import Config
from langchain_community.embeddings import BedrockEmbeddings
from langchain_aws import BedrockLLM
import traceback
import pymysql
import simplejson as simplejson
import logging 
import json
import os,sys
import re
import time
import pandas as pd
import io
from boto_client import Clientmodules
from llm_basemodel import LanguageModel
from athena_execution import AthenaQueryExecute
from openSearchVCEmbedding import EmbeddingBedrockOpenSearch

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize the AWS clients (stateless)
session = boto3.session.Session()
bedrock_client = session.client('bedrock')
print(bedrock_client.list_foundation_models()['modelSummaries'][0])

# Athena Execution and OpenSearch setup (you can optimize these for Lambda's stateless execution)
index_name = 'bedrock-knowledge-base-default-index'  
domain = 'https://pnwwg51jitljmsoy48gh.us-east-1.aoss.amazonaws.com'##-- update here with your OpenSearch domain
region = 'us-east-1' ##-- update here with your AWS region
vector_name = 'bedrock-knowledge-base-default-vector'
fieldname = 'id'
connection_name = 'Aurora connection'

def lambda_handler(event, context):
    user_query = event.get('query', 'default query')
    logger.info(f"User query: {user_query}")
    
    # Fetch data from OpenSearch or Bedrock
    try:
        ebropen2 = EmbeddingBedrockOpenSearch(domain, vector_name, fieldname)
        rqst = RequestQueryBedrock(ebropen2)
        
        vector_search_match = rqst.getOpenSearchEmbedding(index_name, user_query)
        final_question = format_question(user_query, vector_search_match)
        
        # Generate the SQL query
        generated_sql = rqst.generate_sql(final_question)
        
        # Execute the generated SQL query
        query_output = execute_query('us-east-1', connection_name, generated_sql)
        
        # Generate a human-readable response
        prompt = f"Create a response for the query: {user_query} with result: {json.dumps(query_output)}"
        response = rqst.generateResponse(prompt)
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'response': response
            })
        }
    
    except Exception as e:
        logger.error(f"Error in Lambda execution: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e)
            })
        }

# Function to generate final question
def format_question(user_query, vector_search_match):
    details = """
    It is important that the SQL query complies with Athena syntax. For joins, if column names are the same, use aliases (e.g., llm.customer_id in the SELECT statement). Ensure column types are respected.
    """
    question = "\n\nHuman:"+details + vector_search_match + user_query+ "n\nAssistant:"
    return question

# Function to execute the query on Glue (or any RDS)
def execute_query(region, conn_name, query):
    try:
        response = glue_client.get_connection(Name=conn_name)
        connection_props = response['Connection']['ConnectionProperties']
        
        host = connection_props['JDBC_CONNECTION_URL'].split('/')[2].split(':')[0]
        port = connection_props['JDBC_CONNECTION_URL'].split(':')[2].split('/')[0]
        database = connection_props['JDBC_CONNECTION_URL'].split('/')[-1]
        user = connection_props['USERNAME']
        pwd = connection_props['PASSWORD']
        
        conn = pymysql.connect(host=host, user=user, password=pwd, database=database, port=int(port))
        with conn.cursor() as cur:
            cur.execute(query)
            columns = cur.description
            result = [{columns[index][0]: column for index, column in enumerate(row)} for row in cur.fetchall()]
        
        return result
    except Exception as e:
        logger.error(f"Error executing query: {str(e)}")
        raise e

class RequestQueryBedrock:
    def __init__(self, ebropen2):
    
        ##self.bedrock_client = Clientmodules.createBedrockRuntimeClient()
        self.ebropen2 = ebropen2
  

        self.bedrock_client = ebropen2.bedrock_client
        if self.bedrock_client is None:
            self.bedrock_client = Clientmodules.createBedrockRuntimeClient()
        else : 
            print("the bedrock_client is not null")
        self.language_model = LanguageModel(self.bedrock_client)
        self.llm = self.language_model.llm
        
    def getOpenSearchEmbedding(self, index_name,user_query):
        vcindxdoc=self.ebropen2.getDocumentfromIndex(index_name=index_name)
        documnet=self.ebropen2.getSimilaritySearch(user_query,vcindxdoc)
        #return self.ebropen2.format_metadata(documnet)
        return self.ebropen2.get_data(documnet)

    def generateResponse(self,prompt):
        generated_response = self.llm.predict(prompt)
        return generated_response
        
    def generate_sql(self,prompt, max_attempt=4) ->str:
            """
            Generate and Validate SQL query.

            Args:
            - prompt (str): Prompt is user input and metadata from Rag to generating SQL.
            - max_attempt (int): Maximum number of attempts correct the syntax SQL.

            Returns:
            - string: Sql query is returned .
            """
            attempt = 0
            error_messages = []
            prompts = [prompt]

            while attempt < max_attempt:
                logger.info(f'Sql Generation attempt Count: {attempt+1}')
                try:
                    logger.info(f'we are in Try block to generate the sql and count is :{attempt+1}')
                    logger.info(f'Prompt is :{prompt}')
                    generated_sql = self.llm.predict(prompt)
                    logger.info(f'generated_sql is :{generated_sql}')
                    query_str = generated_sql.split("```")[1]
                    query_str = " ".join(query_str.split("\n")).strip()                    
                    sql_query = query_str[3:] if query_str.startswith("sql") else query_str
                    print(sql_query)
                    # return sql_query
                    syntaxcheckmsg=rqstath.syntax_checker(sql_query)
                    if syntaxcheckmsg=='Passed':
                        logger.info(f'syntax checked for query passed in attempt number :{attempt+1}')
                        return sql_query
                    else:
                        prompt = f"""{prompt}
                        This is syntax error: {syntaxcheckmsg}. 
                        To correct this, please generate an alternative SQL query which will correct the syntax error.
                        The updated query should take care of all the syntax issues encountered.
                        Follow the instructions mentioned above to remediate the error. 
                        Update the below SQL query to resolve the issue:
                        {sqlgenerated}
                        Make sure the updated SQL query aligns with the requirements provided in the initial question."""
                        prompts.append(prompt)
                        attempt += 1
                except Exception as e:
                    print(e)
                    logger.error('FAILED')
                    msg = str(e)
                    error_messages.append(msg)
                    attempt += 1
            return sql_query

            