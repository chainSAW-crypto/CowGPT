import streamlit as st
import snowflake.connector
from PyPDF2 import PdfReader

def create_session():
    try:
        conn = snowflake.connector.connect(
        user=st.secrets["snowflake"]["user"],
        password=st.secrets["snowflake"]["password"],
        account=st.secrets["snowflake"]["account"],
        warehouse=st.secrets["snowflake"]["warehouse"],
        database=st.secrets["snowflake"]["database"]
    )
        return conn
    except Exception as e:
        st.error(f"Failed to connect to Snowflake: {e}")
        return None


# Function to run query
def run_query(session, question):
    query = f"""
    WITH
question_embedding AS (
  SELECT
    SNOWFLAKE.CORTEX.EMBED_TEXT_768(
      'snowflake-arctic-embed-m',
      '{question}'
    ) AS question_vector
),
ranked_chunks AS (
  SELECT
    r.RELATIVE_PATH,
    r.INDEX,
    r.CHUNK AS chunk_text,
    VECTOR_L2_DISTANCE(r.EMBEDDING_VECTOR, q.question_vector) AS similarity_score
  FROM
    MethaneGPT.PUBLIC.MethaneRAGResults r,
    question_embedding q
  ORDER BY similarity_score ASC
  LIMIT 10  -- Top 10 most relevant chunks
),
combined_context AS (
  SELECT
    LISTAGG(CHUNK_TEXT, '\n') WITHIN GROUP (ORDER BY similarity_score ASC) AS full_context
  FROM ranked_chunks
)
SELECT
  SNOWFLAKE.CORTEX.COMPLETE(
    'mistral-large2',
    CONCAT(
      ' you are a smart llm with the purpose of resolving user queries. You take the provided scientific context from document base into the account and
Answer the question based on the context. You also provide data-drive insights if available. You Provide answer relevant to provided context only. if context does not match to the question no answer should be provided. Also mention the source documents in the answer and elaborate each point to fulfil user understanding.',
      'Context: ', (SELECT full_context FROM combined_context),
      '\n\nQuestion: {question} ',
      'Answer with bullet points using exact statistics when available:'
    )
  ) AS ai_analysis,
  (SELECT full_context FROM combined_context) AS source_material
FROM combined_context;
    """
    try:
        cursor = session.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        return result[0][0] if result else "No response generated."
    except Exception as e:
        st.error(f"Query execution failed: {e}")
        return None

def run_pdf_query(Session, question):
    query = """
WITH
QUESTION_EMBEDDING AS (
  SELECT
    SNOWFLAKE.CORTEX.EMBED_TEXT_768(
      'snowflake-arctic-embed-m',
      '{user_question}'
    ) AS QUESTION_VECTOR
),
RANKED_TEXT AS (
  SELECT
    TEXT_CONTENT,
    VECTOR_L2_DISTANCE(EMBEDDING_VECTOR, QUESTION_VECTOR) AS SIMILARITY
  FROM input_pdf_embedding_store, QUESTION_EMBEDDING
  WHERE TEXT_CONTENT IS NOT NULL
  ORDER BY SIMILARITY ASC
  LIMIT 10  -- Get top 10 relevant text chunks
),
COMBINED_CONTEXT AS (
  SELECT
    LISTAGG(TEXT_CONTENT, '\n') WITHIN GROUP (
      ORDER BY SIMILARITY ASC
    ) AS FULL_CONTEXT
  FROM RANKED_TEXT
)
SELECT
  SNOWFLAKE.CORTEX.COMPLETE(
    'mistral-large2',
    CONCAT('you are a smart llm with the purpose of resolving user queries. You take the provided scientific context from document base into the account and
Answer the question based on the context. You also provide data-drive insights if available. You Provide answer relevant to provided context only. if context does not match to the question no answer should be provided.', 
      'Context: ', (SELECT FULL_CONTEXT FROM COMBINED_CONTEXT),
      '\nQuestion: {user_question}',
      '\nAnswer concisely with bullet points:'
    )
  ) AS ANSWER,
  (SELECT FULL_CONTEXT FROM COMBINED_CONTEXT) AS SOURCE_MATERIAL
FROM COMBINED_CONTEXT;     
            """
    cursor = session.cursor()
    cursor.execute()
    try:
        cursor = session.cursor()
        cursor.query(""" INSERT INTO INPUT_PDF_EMBEDDING_STORE (TEXT_CONTENT, EMBEDDING_VECTOR)
          SELECT
        '{text}' AS TEXT_CONTENT,
        SNOWFLAKE.CORTEX.EMBED_TEXT_768(
            'snowflake-arctic-embed-m', 
            '{text}'
        ) AS EMBEDDING_VECTOR; """)
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
button = st.button("Submit")
st.write("Upload pdf to fetch the specific answer")
uploaded_file = st.file_uploader("Choose a document", type=["pdf", "docx"])

# Establish Snowflake session
session = create_session()

# Add a button to submit the question
if button:
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



if uploaded_file is not None:
    file_contents = uploaded_file.read()
    pdf_reader = PdfReader(uploaded_file)

        # Extract text from each page
    text = ""
    for page in pdf_reader.pages:
        text += page.extract_text()
    st.write("File uploaded successfully!")
    
    if user_question.strip():
        if session:
            with st.spinner("Fetching response..."):
                response = run_pdf_query(session, user_question)
                if response:
                    st.success("Response received!")
                    st.subheader("Response:")
                    st.write(response)
                else:
                    st.warning("No response generated.")
        else:
            st.error("Unable to connect to Snowflake. Please check your credentials and try again.")

# Empty the table after session end
cursor.execute("""
TRUNCATE TABLE input_pdf_embedding_store;
""")

# Optionally, display information or instructions
st.sidebar.title("About this App")
st.sidebar.write(
    "This application uses Snowflake's Cortex and Mistral LLM to answer your queries based on context from the MethaneGPT database."
)
