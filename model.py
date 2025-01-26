import streamlit as st
import os
import psycopg2
import google.generativeai as genai
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure Gemini API
genai.configure(api_key=os.getenv("GOOGLE_API_KEY"))

class PostgreSQLQueryAssistant:
    def __init__(self):
        # Initialize Gemini model
        self.model = genai.GenerativeModel('gemini-pro')
        
        # Database connection parameters
        self.db_params = {
            'dbname': os.getenv('DB_NAME'),
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD'),
            'host': os.getenv('DB_HOST'),
            'port': os.getenv('DB_PORT', '5432')
        }
        
        # Prompt template for SQL query generation
        self.prompt_template = """
        You are an expert SQL query generator specialized in PostgreSQL. 
        Your task is to convert natural language questions into precise PostgreSQL queries.
        
        Guidelines:
        - Always use parameterized queries to prevent SQL injection
        - Focus on retrieving exactly what the user asked
        - If the question is ambiguous, ask for clarification
        
        Example Mappings:
        1. "How many employees joined yesterday?" 
           → SELECT COUNT(*) FROM employees WHERE join_date = CURRENT_DATE - INTERVAL '1 day'
        
        2. "Show me the departments with most employees"
           → SELECT department, COUNT(*) as employee_count 
             FROM employees 
             GROUP BY department 
             ORDER BY employee_count DESC 
             LIMIT 5
        
        Current Question: {question}
        
        Please generate the most appropriate PostgreSQL query.
        """

    def generate_sql_query(self, question):
        """Generate SQL query from natural language question"""
        prompt = self.prompt_template.format(question=question)
        response = self.model.generate_content(prompt)
        return response.text.strip()

    def execute_query(self, query):
        """Execute the generated SQL query"""
        try:
            # Establish database connection
            conn = psycopg2.connect(**self.db_params)
            cursor = conn.cursor()
            
            # Execute query
            cursor.execute(query)
            
            # Fetch results
            columns = [desc[0] for desc in cursor.description]
            results = cursor.fetchall()
            
            # Close connection
            cursor.close()
            conn.close()
            
            return columns, results
        
        except Exception as e:
            st.error(f"Database Error: {e}")
            return None, None

def main():
    st.title("🤖 Natural Language to PostgreSQL Query Assistant")
    
    # Initialize query assistant
    query_assistant = PostgreSQLQueryAssistant()
    
    # User input
    user_question = st.text_input("Ask a question about your data:")
    
    if st.button("Get Insights"):
        if user_question:
            # Generate SQL query
            with st.spinner("Generating SQL Query..."):
                sql_query = query_assistant.generate_sql_query(user_question)
                st.code(sql_query, language='sql')
            
            # Execute query
            with st.spinner("Fetching Results..."):
                columns, results = query_assistant.execute_query(sql_query)
                
                if columns and results:
                    # Display results
                    st.subheader("Query Results")
                    
                    # Create DataFrame for better visualization
                    import pandas as pd
                    df = pd.DataFrame(results, columns=columns)
                    st.dataframe(df)
                    
                    # Additional insights
                    st.write(f"Total rows returned: {len(results)}")

if __name__ == "__main__":
    main()