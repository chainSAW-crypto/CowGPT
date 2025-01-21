import streamlit as st
def run_query(question):
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
    result = session.sql(query).collect()
    return result[0]["RESPONSE"] if result else "No response generated."

# Set up the Streamlit UI

st.title("🌱 MethaneGPT Chat 🐄")
st.write(""""Methane is a potent greenhouse gas, contributing significantly to global warming. 🌍 It has over 80 times the warming power of carbon dioxide over 20 years. Tackling methane emissions is crucial to mitigate climate change. 🌡️
Learn more about these problems, their associated risks, and how, from a farmer's perspective, you can adopt healthy, profitable agricultural 🌾 and cattle-rearing 🐮 practices to address this issue. 💡 """)

# Create a text input for user questions
st.subheader("Ask your question")
user_question = st.text_input("Enter your question below:", placeholder="Type your question here...")

# Add a button to submit the question
if st.button("Submit"):
    if user_question.strip():
        with st.spinner("Fetching response..."):
            try:
                response = run_query(user_question)
                st.success("Response received!")
                st.subheader("Response:")
                st.write(response)
            except Exception as e:
                st.error(f"An error occurred: {e}")
    else:
        st.warning("Please enter a valid question.")

# Optionally, display information or instructions
st.sidebar.title("About this App")
st.sidebar.write(
    "This application uses Snowflake's Cortex and Mistral LLM to answer your queries based on context from the MethaneGPT database."
)
