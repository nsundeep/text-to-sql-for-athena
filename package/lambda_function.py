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
import uuid
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

#DB connection
connection_name = 'Aurora connection'
glue_client = session.client('glue')
response = glue_client.get_connection(Name=connection_name)
connection_props = response['Connection']['ConnectionProperties']
host = connection_props['JDBC_CONNECTION_URL'].split('/')[2].split(':')[0]
port = connection_props['JDBC_CONNECTION_URL'].split(':')[2].split('/')[0]
database = connection_props['JDBC_CONNECTION_URL'].split('/')[-1]
user = connection_props['USERNAME']
pwd = connection_props['PASSWORD']
rs_conn = pymysql.connect(database=database, host=host, user=user, password=pwd)


print(bedrock_client.list_foundation_models()['modelSummaries'][0])

# Athena Execution and OpenSearch setup (you can optimize these for Lambda's stateless execution)
index_name = 'bedrock-knowledge-base-default-index'  
domain = 'https://xfgbyl8lojtbf3as0cm6.us-east-1.aoss.amazonaws.com'##-- update here with your OpenSearch domain
region = 'us-east-1' ##-- update here with your AWS region
vector_name = 'bedrock-knowledge-base-default-vector'
fieldname = 'id'


def lambda_handler(event, context):
    user_query = event.get('query', 'default query')
    user_budgets = event.get('budgets', 'default budgets')
    conversation_id = event.get('conversation_id', '')

    if conversation_id == '':
        conversation_id = uuid.uuid4()

    logger.info(f"User query: {user_query}")
    logger.info(f"Conversation Id: {conversation_id}")
    
    log_data = {};
    log_data['conversation_id'] = conversation_id;

    # Fetch data from OpenSearch or Bedrock
    try:

        ebropen2 = EmbeddingBedrockOpenSearch(domain, vector_name, fieldname)
        rqst = RequestQueryBedrock(ebropen2)

        log_data['user_query'] = user_query
        
        vector_search_match = rqst.getOpenSearchEmbedding(index_name, user_query)
        final_question = format_question(conversation_id, user_query, user_budgets, vector_search_match)

        log_data['final_question']= final_question

        # Generate the SQL query
        generated_sql = rqst.generate_sql(final_question)
        log_data['generated_sql']= generated_sql

        # Execute the generated SQL query
        query_output = execute_query('us-east-1', connection_name, generated_sql)
        log_data['query_output']= 'a'
        
        # Generate a human-readable response
        prompt = f"Create a response for the query: {user_query} with result: {simplejson.dumps(query_output)}"
        response = rqst.generateResponse(prompt)
        log_data['final_response']= response

        logger.info(f"Log data: {log_data}")

        save_log(log_data)
        
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
def format_question(conversation_id, user_query, user_budgets, vector_search_match):
    try:

        logger.info(f"In format_question: {conversation_id}")
        conversation_history = ""
        details = "User has access to only the following budget ids: " +user_budgets
        conversation_history = fetch_prev_conversations(conversation_id)

        if(len(conversation_history)>0):
            details += "The same user has previously asked other questions. When you generate the answer to the current question, take the context of all previous questions. The previous questions are as follows:\n"
            for item in conversation_history:
                details += " Human: " + item['user_query'] + " \n Assistant: " + item['final_response']+"\n"

        details += """
        It is important that the SQL query complies with MYSQL syntax. For joins, if column names are the same, use aliases (e.g., llm.customer_id in the SELECT statement). Ensure column types are respected.
        """
        question = "\n\nHuman: " + vector_search_match + "\n" + details + "\n" + "Current question is: " +user_query+ "\n\nAssistant:"
        return question
    except Exception as e:
        logger.error(f"Error format_question: {str(e)}")

def fetch_prev_conversations(conversation_id):
    try:

        logger.info(f"fetch data for: {conversation_id}")
        fetch_query = """ SELECT * from (select * from ai_query_logs where conversation_id = %s order by id Desc LIMIT 10) as sub order by id ASC """
        cur = rs_conn.cursor()
        cur.execute(fetch_query, conversation_id)
        columns = cur.description 
        result = [{columns[index][0]:column for index, column in enumerate(value)} for value in cur.fetchall()]
        
        return result        
    except Exception as e:
        logger.error(f"Error executing query: {str(e)}")
        raise e
        if rs_conn:
            cur.close()
            


# Function to generate log query and initaite the execute_query call
def save_log(log_data):
    try:
        
        insert_query = """
        INSERT INTO ai_query_logs (conversation_id, user_query, generated_sql, query_result, final_response, final_question)
        VALUES (%s, %s, %s, %s, %s, %s);
        """

        logger.info(f"fetch data for: {insert_query}")
        cur = rs_conn.cursor()
        cur.execute(insert_query, (log_data['conversation_id'], log_data['user_query'], log_data['generated_sql'], log_data['query_output'], log_data['final_response'], log_data['final_question']))

        rs_conn.commit()
    except Exception as e:
        logger.error(f"Failed to log query: {e}")
        raise e
    finally:
        if rs_conn:
            cur.close()
            

# Function to execute the query on Glue (or any RDS)
def execute_query(region, conn_name, query):

    try:
        cur = rs_conn.cursor()
        cur.execute(query)
        columns = cur.description 
        result = [{columns[index][0]:column for index, column in enumerate(value)} for value in cur.fetchall()]
        
        return result
    except Exception as e:
        logger.error(f"Error executing query: {str(e)}")
        raise e
        if rs_conn:
            cur.close()
            
       

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
            rqstath=AthenaQueryExecute()
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

            