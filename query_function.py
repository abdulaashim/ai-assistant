from transformers import GPT2LMHeadModel, GPT2Tokenizer
import psycopg2
from psycopg2.extras import RealDictCursor
import pandas as pd

# PostgreSQL connection details
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'panda_query'  # Your database name
DB_USER = 'postgres'     # Your PostgreSQL username
DB_PASSWORD = '9087'  # Your PostgreSQL password

# Function to connect to PostgreSQL
def get_db_connection():
    return psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )

# Load the GPT-2 model and tokenizer for natural language to SQL
def load_gpt2():
    model = GPT2LMHeadModel.from_pretrained("gpt2")
    tokenizer = GPT2Tokenizer.from_pretrained("gpt2")
    return model, tokenizer

# Function to generate SQL from natural language query
def generate_sql(query: str, model, tokenizer):
    inputs = tokenizer.encode(query, return_tensors="pt")
    outputs = model.generate(inputs, max_length=100, num_return_sequences=1)
    sql_query = tokenizer.decode(outputs[0], skip_special_tokens=True)
    return sql_query.strip()

# Function to execute the generated SQL on the PostgreSQL database
def execute_query(sql_query: str):
    try:
        # Connect to the database
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute(sql_query)
        
        # Fetch all results
        result = cursor.fetchall()

        # Close the cursor and connection
        cursor.close()
        conn.close()

        return result
    except Exception as e:
        print(f"Error executing query: {e}")
        return None

# Main function that integrates everything
def query_pandas(query: str):
    # Load the GPT-2 model
    model, tokenizer = load_gpt2()

    # Generate SQL from the natural language query
    sql_query = generate_sql(query, model, tokenizer)
    print(f"Generated SQL: {sql_query}")

    # Execute the SQL query on PostgreSQL and fetch results
    results = execute_query(sql_query)

    # If results are found, display them
    if results:
        df = pd.DataFrame(results)
        return df
    else:
        return "No results found."

# Example usage
if __name__ == "__main__":
    user_query = "What are the products and quantities in the sales table?"
    result_df = query_pandas(user_query)
    print(result_df)
