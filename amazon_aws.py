import boto3
import json
import time
import re
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

def lambda_handler(event, context):
    # Setup Anthropic API
    # setup_anthropic()
    
    # Initialize Anthropic client
    # anthropic_client = AnthropicClient(api_key=os.environ["ANTHROPIC_API_KEY"])

    # Initialize Bedrock client
    bedrock_client = boto3.client(service_name='bedrock-runtime', region_name='us-east-1')

    # Initialize Athena client
    athena_client = boto3.client('athena')
    
    # Initialize parameters
    database = event['database']
    table_name = event['table_name']
    output_bucket = 's3://llm-output-bucket/'
    
    # Create SQL_Answer_Agent instance
    sql_agent = SQL_Answer_Agent(bedrock_client, athena_client, database, table_name, output_bucket)
    
    # Get and set the schema
    sql_agent.get_set_db_schema()
    
    # Define the question and get SQL query
    question = event['question'] #"give me details of employees in Irwin-Martinez company?"
    answer = sql_agent.get_answer(question)
    
    # Print the results
    print("############")
    print(answer)
    
    return {
        'statusCode': 200,
        'body': answer
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
                "max_tokens": 4096, 
                "system": "You are an expert SQL database manager. Convert user questions into accurate SQL queries based on the given schemas.", 
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
            # print(f"self.schema:{self.schema}")
            return True
        except (NoCredentialsError, PartialCredentialsError) as e:
            print(f"Credentials error: {e}")
            return False
        except Exception as e:
            print(f"Error fetching schema: {e}")
            return False

    def execute_sql_query(self, query) -> bool:
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
                    # print(result)
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

    def get_answer(self, query, prompt=None):
        if prompt:
            prompt = prompt
        else:
            prompt = f'''Use the schema {self.schema} for table {self.table_name} 
            and respond ONLY with an SQL query to answer the question: {query}.
            Write query in between SQL tags like <SQL></SQL>.'''
           
        # print(f"prompt: {prompt}") 
        llm_response = self.get_llm_response(prompt)
        sql_query = self.extract_sql(llm_response)
        print(f"llm_response: {llm_response}") 
        print(f"sql_query: {sql_query}") 
        sql_response = self.execute_sql_query(sql_query)
        response_summary = self.summarize_sql_response(f"Question: {query} Answer: {sql_response}")

        return response_summary

    def summarize_sql_response(self, sql_response):
        prompt = f"Summarize: {sql_response}"
        response = self.get_llm_response(prompt)
        return response

    def extract_sql(self, text):
        pattern = r'<SQL>(.*?)</SQL>'
        matches = re.findall(pattern, text, re.DOTALL)
        cleaned_matches = [match.strip() for match in matches]
        if cleaned_matches:
            cleaned_match = cleaned_matches[0]
            # Replace newline characters with spaces
            return cleaned_match.replace('\n', ' ')
        else:
            return "" #Future: Loop over the queries        return cleaned_matches[0] #Future: Loop over the queries