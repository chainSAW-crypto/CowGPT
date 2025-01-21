import streamlit as st
import snowflake.connector

def create_session():
    try:
        conn = snowflake.connector.connect(
            user="WOLF",
            password= SNOWFLAKE_PASSWORD,
            account="kdb70594.us-east-1",
            warehouse="COMPUTE_WH",
            database="METHANEGPT",
            schema="PUBLIC"
        )
        return conn
    except Exception as e:
        st.error(f"Failed to connect to Snowflake: {e}")
        return None


# Function to run query
def run_query(session, question):
    query = f"""
    SELECT snowflake.cortex.complete(
        'mistral-large2',
        CONCAT(
            'Answer the question based on the provided context. Provide detailed answer relevant to provided context ',
            'Context:',
            (
                select listagg(chunk[0],' ') from (SELECT CHUNK from MethaneGPT.PUBLIC.METHANERAGRESULTS
                ORDER BY VECTOR_L2_DISTANCE(
                    SNOWFLAKE.CORTEX.EMBED_TEXT_768('snowflake-arctic-embed-m', 
                    '{question}'), Embedding_Vector
                ) limit 5)
            ),
            ' Question: ',
            '{question}',
            'Answer: '
        )
    ) as response
    """
    try:
        cursor = session.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        return result[0][0] if result else "No response generated."
    except Exception as e:
        st.error(f"Query execution failed: {e}")
        return None

# Set up the Streamlit UI
st.title("🌱 MethaneGPT Chat 🐄")
st.write(""""Methane is a potent greenhouse gas, contributing significantly to global warming. 🌍 It has over 80 times the warming power of carbon dioxide over 20 years. Tackling methane emissions is crucial to mitigate climate change. 🌡️
Learn more about these problems, their associated risks, and how, from a farmer's perspective, you can adopt healthy, profitable agricultural 🌾 and cattle-rearing 🐮 practices to address this issue. 💡 """)

# Create a text input for user questions
st.subheader("Ask your question")
user_question = st.text_input("Enter your question below:", placeholder="Type your question here...")

# Establish Snowflake session
session = create_session()

# Add a button to submit the question
if st.button("Submit"):
    if user_question.strip():
        if session:
            with st.spinner("Fetching response..."):
                response = run_query(session, user_question)
                if response:
                    st.success("Response received!")
                    st.subheader("Response:")
                    st.write(response)
                else:
                    st.warning("No response generated.")
        else:
            st.error("Unable to connect to Snowflake. Please check your credentials and try again.")
    else:
        st.warning("Please enter a valid question.")

# Optionally, display information or instructions
st.sidebar.title("About this App")
st.sidebar.write(
    "This application uses Snowflake's Cortex and Mistral LLM to answer your queries based on context from the MethaneGPT database."
)
