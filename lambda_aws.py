import boto3
import json
import re
import time
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

def lambda_handler(payload, context):
    # Setup Anthropic API
    # setup_anthropic()
    
    # Initialize Anthropic client
    # anthropic_client = AnthropicClient(api_key=os.environ["ANTHROPIC_API_KEY"])

    # Initialize Bedrock client
    bedrock_client = boto3.client(service_name='bedrock-runtime', region_name='us-east-1')

    # Initialize Athena client
    athena_client = boto3.client('athena')
    
    event = json.loads(payload['body'])
    print(f"payload {event}")
    # Initialize parameters
    database = event['database']
    table_name = event['table_name']
    client = event['client']
    output_bucket = 's3://llm-output-bucket/'
    
    # Create SQL_Answer_Agent instance
    sql_agent = SQL_Answer_Agent(bedrock_client, athena_client, database, table_name, output_bucket)
    
    # Get and set the schema
    sql_agent.get_set_db_schema()
    
    # Define the question and get SQL query
    question = event['question'] #"give me details of employees in Irwin-Martinez company?"
    answer = sql_agent.get_answer(question, client)
    
    # Print the results
    print("############")
    print(answer)
    
    return {
        # 'statusCode': 200,
        'answer': answer
    }



class SQL_Answer_Agent:
    def __init__(self, bedrock_client, athena_client, database,table_name, output_bucket) -> None:
        # self.client = anthropic_client
        self.bedrock_client = bedrock_client
        self.athena_client = athena_client
        self.database = database
        self.output_bucket = output_bucket
        self.table_name = table_name
        self.schema = None

    def set_prompt(self, question, prompt=None):
        if prompt:
            self.prompt = prompt
        else:
            self.prompt = f'''Use the schema {self.schema} and respond ONLY with an SQL query to answer the question: {question}. 
            Response should contain ONLY the SQL Query'''

    def get_llm_response(self, content, modelId = "anthropic.claude-3-sonnet-20240229-v1:0"):
        # "anthropic.claude-3-sonnet-20240229-v1:0"
        input = {
            "modelId": modelId, 
            "contentType": 'application/json',
            "accept": '*/*',
            "body": json.dumps({
                "max_tokens": 15000, 
                "system": f'''You are an expert SQL database manager to write queries for AWS Athena. youe job is to help convert text descriptions into SQL queries for querying AWS Athena. Verify the correctness of the syntax of the query generated. 
            Keep in mind that in Athena, timestamps have milliseconds precision. Write query in between SQL tags like <SQL></SQL> and give it in 1 line without any formatting. Dont respond with any explanation of the query, output of this is going to Athena directly so just return SQL query compatible with Athena in response.''', 
                "messages": [{"role": "user", "content": content}], 
                "anthropic_version": "bedrock-2023-05-31"
            })
        }
        
        response = self.bedrock_client.invoke_model(
            body=input["body"],
            modelId=input["modelId"],
            accept=input["accept"],
            contentType=input["contentType"]
        )

        response_body = json.loads(response['body'].read())
        response = response_body.get('content')[0].get('text')
        # print(response)
        return response
        
           
    def get_set_db_schema(self):
        try:
            query = f"describe {self.table_name};"
            query_result = self.execute_sql_query(query)
            self.schema = query_result
            print(f"self.schema:{self.schema}")
            return True
        except (NoCredentialsError, PartialCredentialsError) as e:
            print(f"Credentials error: {e}")
            return False
        except Exception as e:
            print(f"Error fetching schema: {e}")
            return False

    def execute_sql_query(self, query):
        if query:
            try:
                response = self.athena_client.start_query_execution(
                    QueryString=query,
                    QueryExecutionContext={'Database': self.database},
                    ResultConfiguration={'OutputLocation': self.output_bucket}
                )

                query_execution_id = response['QueryExecutionId']
                # print(query)
                # print(response)
                while True:
                    result = self.athena_client.get_query_execution(QueryExecutionId=query_execution_id)
                    print(result)
                    status = result['QueryExecution']['Status']['State']
                    if status == 'SUCCEEDED':
                        break
                    elif status in ['FAILED', 'CANCELLED']:
                        raise Exception(f"Query failed with status: {status}")
                    time.sleep(2)

                results = self.athena_client.get_query_results(QueryExecutionId=query_execution_id)
                rows = results['ResultSet']['Rows']
                # response_summary = self.summarize_sql_response(f"Question: {question} Answer: {rows}")
                return rows
            except Exception as e:
                print(f"Error executing query: {e}")
                return False
        else:
            return False


    def extract_sql(self, text):
        pattern = r'<SQL>(.*?)</SQL>'
        matches = re.findall(pattern, text, re.DOTALL)
        cleaned_matches = [match.strip() for match in matches]
        return cleaned_matches
        
    def get_answer(self, query, client, prompt=None):
        if prompt:
            prompt = prompt
        else:
            prompt =f'''Use the schema for table {self.table_name} mentioned below to prepare query. Schema - {self.schema}. Please provide the SQL query for this question:{query} and for client {client} '''
            
            # f"Use the schema {self.schema} for table {self.table_name} and respond ONLY with an SQL query to answer the question: {query}"
           
        # print(f"prompt: {prompt}") 
        sql_query = self.get_llm_response(prompt)
        print(f"\nSQL QUERY from LLM : {sql_query}") 
        final_sql = self.extract_sql(sql_query)
        sql_response = self.execute_sql_query(final_sql[0])
        print(f"response: {sql_response}")
        response_summary = self.summarize_sql_response(query, f'<data> {sql_response}')

        return response_summary

    def summarize_sql_response(self,query, sql_response):
        # p = f'''Specifically, I'm interested in context of asked question. Data mentioned below is the response from Athena for question {query}.  {sql_response}'''
        # print(p)
        prompt = f"Analyze the data mentioned below and respond only with the analysis based on the given Question {query}  : {sql_response}"
        response = self.summary_llm_agent(prompt)
        return response

    def summary_llm_agent(self, content, modelId = "anthropic.claude-3-sonnet-20240229-v1:0"):
        # "anthropic.claude-3-sonnet-20240229-v1:0"
        input = {
            "modelId": modelId, 
            "contentType": 'application/json',
            "accept": '*/*',
            "body": json.dumps({
                "max_tokens": 15000, 
                "system": f'''I have the following SQL data start after <data> tag: Can you provide a summary from this data? ''', 
                "messages": [{"role": "user", "content": content}], 
                "anthropic_version": "bedrock-2023-05-31"
            })
        }
        
        response = self.bedrock_client.invoke_model(
            body=input["body"],
            modelId=input["modelId"],
            accept=input["accept"],
            contentType=input["contentType"]
        )

        response_body = json.loads(response['body'].read())
        response = response_body.get('content')[0].get('text')
        # print(response)
        return response