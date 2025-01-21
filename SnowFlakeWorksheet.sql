create database MethaneGPT;
use MethaneGPT;


CREATE OR REPLACE function read_pdf(file_name string)
  RETURNS string
  LANGUAGE PYTHON
  RUNTIME_VERSION = '3.8'
  PACKAGES = ('snowflake-snowpark-python','PyPDF2')
  HANDLER = 'main_fn'
AS
$$
from snowflake.snowpark.files import SnowflakeFile
import PyPDF2
def main_fn(file_name):
    f = SnowflakeFile.open(file_name, 'rb')
    pdf_object = PyPDF2.PdfReader(f)
    
    # Initialize a variable to hold all the text
    all_text = ""
    
    # Iterate over all the pages and concatenate the text
    for page in pdf_object.pages:
        all_text += page.extract_text().replace('\n',' ')
    
    return all_text
$$;


CREATE OR REPLACE FUNCTION pdf_split_by_pages(file_name STRING)
  RETURNS ARRAY
  LANGUAGE PYTHON
  RUNTIME_VERSION = '3.8'
  PACKAGES = ('snowflake-snowpark-python', 'PyPDF2')
  HANDLER = 'main_fn'
AS
$$
from snowflake.snowpark.files import SnowflakeFile
import PyPDF2

def main_fn(file_name):
    # Open the file from the stage
    with SnowflakeFile.open(file_name, 'rb') as f:
        pdf_object = PyPDF2.PdfReader(f)
        
        # Initialize a list to hold text chunks for each page
        page_chunks = []
        
        # Iterate over all the pages and add each page's text as a chunk
        for page in pdf_object.pages:
            page_text = page.extract_text().replace('\n', ' ')
            page_chunks.append(page_text)
    
    return page_chunks
$$;

create or replace stage MethaneGPT.PUBLIC.MethaneRAGStorage url=""
credentials=(aws_key_id=''
aws_secret_key='')
Directory=(ENABLE=TRUE);


SELECT * FROM directory(@MethaneGPT.PUBLIC.MethaneRAGStorage);

--------------------------------------------------------
-- Running Simple queries

--get the data
SELECT RELATIVE_PATH,
pdf_split_by_pages(BUILD_SCOPED_FILE_URL( @MethaneGPT.PUBLIC.MethaneRAGStorage ,RELATIVE_PATH)) as pdf_split FROM directory(@MethaneGPT.PUBLIC.MethaneRAGStorage);


-- get the number of pages in the pdf
select RELATIVE_PATH, 
count_no_of_pages_udf(BUILD_SCOPED_FILE_URL( @MethaneGPT.PUBLIC.MethaneRAGStorage ,RELATIVE_PATH)) as pages 
from directory(@MethaneGPT.PUBLIC.MethaneRAGStorage)


--take specific columns from Flatten Data. Trim the " from chunks
with splitted_data as (SELECT RELATIVE_PATH, pdf_split_by_pages(BUILD_SCOPED_FILE_URL( @MethaneGPT.PUBLIC.MethaneRAGStorage, RELATIVE_PATH )) as pdf_text_split FROM directory(@MethaneGPT.PUBLIC.MethaneRAGStorage))
select Relative_path,f.Index,trim(f.value,'"') as chunk from splitted_data , lateral flatten(pdf_text_split) f ;

--Vector Embedding     
with splitted_data as (SELECT RELATIVE_PATH,SIZE,pdf_split_by_pages(BUILD_SCOPED_FILE_URL(@MethaneGPT.PUBLIC.MethaneRAGStorage, RELATIVE_PATH )) as pdf_text_split FROM directory(@MethaneGPT.PUBLIC.MethaneRAGStorage))
select Relative_path,SIZE,f.Index,trim(f.value,'"') as chunk,SNOWFLAKE.CORTEX.EMBED_TEXT_768('snowflake-arctic-embed-m', trim(f.value,'"')) as Embedding_Vector from splitted_data , lateral flatten(pdf_text_split) f ;  


-----------------------------------------------------
--various Tables to store data
------------------
--1>>>>>>
--  Create the table to store the data
CREATE OR REPLACE TABLE PDF_FullInfo (
    RELATIVE_PATH STRING,          -- To store the file path
    pdf_split ARRAY, -- To store the array of split PDF chunks
    pdf_vectors VECTOR(FLOAT, 768)
);

-- Insert data into the table
INSERT INTO PDF_FullInfo(RELATIVE_PATH, pdf_split, pdf_vectors)
SELECT 
    RELATIVE_PATH, 
    pdf_split_by_pages(BUILD_SCOPED_FILE_URL(@MethaneGPT.PUBLIC.MethaneRAGStorage, RELATIVE_PATH)) AS pdf_split,
    SNOWFLAKE.CORTEX.EMBED_TEXT_768( 'snowflake-arctic-embed-m', 
                           BUILD_SCOPED_FILE_URL(@MethaneGPT.PUBLIC.MethaneRAGStorage, RELATIVE_PATH)) as pdf_vectors
FROM 
    directory(@MethaneGPT.PUBLIC.MethaneRAGStorage);

select * from PDF_FullInfo;


------
--2>>>>>
-- Creating a Column  

CREATE or REPLACE TABLE MethaneGPT.PUBLIC.MethaneRAGResults (
    RELATIVE_PATH STRING,
    SIZE NUMBER,
    INDEX NUMBER,
    CHUNK array,
    EMBEDDING_VECTOR VECTOR(FLOAT, 768)
);
CREATE OR REPLACE TABLE MethaneGPT.PUBLIC.MethaneRAGResults (
    RELATIVE_PATH STRING,
    SIZE NUMBER,
    INDEX NUMBER,
    CHUNK ARRAY,
    EMBEDDING_VECTOR VECTOR(FLOAT, 768)
);

INSERT INTO MethaneGPT.PUBLIC.MethaneRAGResults
SELECT
    RELATIVE_PATH,
    SIZE,
    INDEX, 
    ARRAY_CONSTRUCT(TRIM(f.VALUE, '"')) AS CHUNK, -- Store the chunk in an array
    SNOWFLAKE.CORTEX.EMBED_TEXT_768('snowflake-arctic-embed-m', TRIM(f.VALUE, '"')) AS EMBEDDING_VECTOR
FROM
    (SELECT     
        RELATIVE_PATH,
        SIZE,
        pdf_split_by_pages(BUILD_SCOPED_FILE_URL(@MethaneGPT.PUBLIC.MethaneRAGStorage, RELATIVE_PATH)) AS pdf_text_split
    FROM 
        directory(@MethaneGPT.PUBLIC.MethaneRAGStorage)
    ) AS splitted_data,
    LATERAL FLATTEN(INPUT => splitted_data.pdf_text_split) f;

select * from MethaneGPT.PUBLIC.MethaneRAGResults;


-----------------------
--3>>>>>>>>>

CREATE OR REPLACE TABLE MethaneGPT.PUBLIC.pdf_tags_table (
    RELATIVE_PATH STRING,
    SIZE NUMBER(38,0),
    PAGE_COUNT NUMBER(38,0), 
    PDF_TAG STRING,
    PDF_TAG_EMBEDDINGS VECTOR(FLOAT, 768)
);

INSERT INTO MethaneGPT.PUBLIC.pdf_tags_table (RELATIVE_PATH, SIZE, PAGE_COUNT, PDF_TAG, PDF_TAG_EMBEDDINGS)
WITH pdf_data AS (
    -- Extract the PDF content and the size of the PDF
    SELECT 
        RELATIVE_PATH,
        SIZE,
        pdf_split_by_pages(BUILD_SCOPED_FILE_URL(@MethaneGPT.PUBLIC.MethaneRAGStorage, RELATIVE_PATH)) AS pdf_pages
    FROM DIRECTORY(@MethaneGPT.PUBLIC.MethaneRAGStorage)
),
tagged_data AS (
    -- Apply logic to create the TAG column
    SELECT 
        RELATIVE_PATH,
        SIZE,
        ARRAY_TO_STRING(ARRAY_SLICE(pdf_pages, 0, 5), ' ') AS pdf_tag, -- First 5 pages if size > 7
        count_no_of_pages_udf(BUILD_SCOPED_FILE_URL(@MethaneGPT.PUBLIC.MethaneRAGStorage, RELATIVE_PATH)) as page_count -- Use UDF to get the actual page count
    FROM pdf_data
    WHERE SIZE > 7
    GROUP BY RELATIVE_PATH, SIZE, pdf_pages
    UNION ALL
    SELECT 
        RELATIVE_PATH,
        SIZE,
        ARRAY_TO_STRING(pdf_pages, ' ') AS pdf_tag, -- Entire content if size <= 7
        count_no_of_pages_udf(BUILD_SCOPED_FILE_URL(@MethaneGPT.PUBLIC.MethaneRAGStorage, RELATIVE_PATH)) as page_count
    FROM pdf_data
    WHERE SIZE <= 7
    GROUP BY RELATIVE_PATH, SIZE, pdf_pages
),
tagged_embeddings AS (
    -- Generate embeddings for pdf_tag
    SELECT 
        RELATIVE_PATH,
        SIZE,
        page_count,
        pdf_tag,
        SNOWFLAKE.CORTEX.EMBED_TEXT_768('snowflake-arctic-embed-m', pdf_tag) AS pdf_tag_embeddings
    FROM tagged_data
)
SELECT 
    RELATIVE_PATH,
    SIZE,
    page_count,
    pdf_tag,
    pdf_tag_embeddings
FROM tagged_embeddings;

select * from MethaneGPT.PUBLIC.pdf_tags_table;

--------------------


-- -- Find the closest relative path in pdf_tags_table
-- WITH ClosestPDF AS (
--     SELECT 
--         RELATIVE_PATH
--     FROM 
--         MethaneGPT.PUBLIC.pdf_tags_table
--     ORDER BY 
--         VECTOR_L2_DISTANCE(
--             SNOWFLAKE.CORTEX.EMBED_TEXT_768('snowflake-arctic-embed-m', 'How to reduce methan emission as a farmer?'),
--             PDF_TAG_EMBEDDINGS
--         )
--     LIMIT 5
-- ),

-- -- Find the closest page chunks in MethaneRAGResults for the identified RELATIVE_PATH
-- ClosestPage AS (
--     SELECT 
--         CHUNK,
--         VECTOR_L2_DISTANCE(
--             SNOWFLAKE.CORTEX.EMBED_TEXT_768('snowflake-arctic-embed-m', 'How to reduce methan emission as a farmer?'),
--             EMBEDDING_VECTOR
--         ) AS DISTANCE
--     FROM 
--         MethaneGPT.PUBLIC.MethaneRAGResults
--     WHERE 
--         RELATIVE_PATH = (SELECT RELATIVE_PATH FROM ClosestPDF)
--     ORDER BY 
--         DISTANCE
--     LIMIT 10
-- )

-- -- Retrieve the answer
-- SELECT 
--     CONCAT(
--         'Answer the question based on the context. Be concise. ',
--         'Context: ', 
--         (SELECT CHUNK FROM ClosestPage), 
--         ' Question: ', 
--         'How to reduce methan emission as a farmer?', 
--         ' Answer: '
--     ) AS Prompt;4


-----------------------------------------
-- Queries Testing section
-- Testing various types of queries for efficient retrieval of data.




SELECT snowflake.cortex.complete(
    'mistral-large2', 
    CONCAT( 
        'Answer the question based on the context. Be concise. Context: ',
        (
        SELECT LISTAGG(CHUNK, ' ') WITHIN GROUP (ORDER BY VECTOR_L2_DISTANCE(
        SNOWFLAKE.CORTEX.EMBED_TEXT_768('snowflake-arctic-embed-m-v1.5',
        'give me data driven insights on global methane emission trends over the years.'
        ), Embedding_Vector
        ))
        FROM MethaneGPT.PUBLIC.METHANERAGRESULTS
        LIMIT 1

        ),
        ' Question: ', 
        'give me data driven insights on global methane emission trends over the years.',
        'Answer: '
    )
) as response;



        

WITH input_query AS (
    -- Step 1: Embed the user query
    SELECT 
        SNOWFLAKE.CORTEX.EMBED_TEXT_768(
            'snowflake-arctic-embed-m', 
            'can you tell me benefits of feed additives and their cost to carbon credit profit comparison. Give me data driven insights so that i can model the profits i can produce'
        ) AS query_embedding
),

top_tags AS (
    -- Step 2: Find the top 3 rows with the least vector distance based on PDF_TAG_EMBEDDINGS
    SELECT 
        RELATIVE_PATH,
        PDF_TAG,
        VECTOR_L2_DISTANCE(
            (SELECT query_embedding FROM input_query), 
            PDF_TAG_EMBEDDINGS
        ) AS distance
    FROM 
        QueryTable_For_fullPDF
    ORDER BY 
        distance ASC
    LIMIT 3
),

relevant_chunks AS (
    -- Step 3: Retrieve the most relevant chunks from the pdf_vectors of the top 3 rows
    SELECT 
        RELATIVE_PATH,
        pdf_split,
        VECTOR_L2_DISTANCE(
            (SELECT query_embedding FROM input_query), 
            pdf_vectors
        ) AS distance
    FROM 
        QueryTable_For_fullPDF
    WHERE 
        RELATIVE_PATH IN (SELECT RELATIVE_PATH FROM top_tags)
    ORDER BY 
        distance ASC
    LIMIT 3
),

-- Step 4: Aggregate all content of relevant PDFs into one context
aggregated_content AS (
    SELECT 
        RELATIVE_PATH,
        VECTOR_L2_DISTANCE(
            SNOWFLAKE.CORTEX.EMBED_TEXT_768(
                'snowflake-arctic-embed-m', 
                LISTAGG(value, ' ') WITHIN GROUP (ORDER BY distance) -- Concatenate all chunks
            ),
            (SELECT query_embedding FROM input_query) -- Compare with the pre-calculated query embedding
        ) AS similarity_score
    FROM 
        relevant_chunks,
        LATERAL FLATTEN(input => pdf_split)
    GROUP BY 
        RELATIVE_PATH
)

select * from aggregated_content;
-- -- Final step: Generate the response using SNOWFLAKE.CORTEX.COMPLETE
-- SELECT 
--     SNOWFLAKE.CORTEX.COMPLETE(
--         'mistral-large2', 
--         CONCAT( 
--             'Answer the question based on the context. Be concise. Context: ',
--              (SELECT LISTAGG(extracted_context, ' ') FROM aggregated_content),  -- Concatenate all extracted contexts
--             ' Question: ',
--             'can you tell me benefits of feed additives and their cost to carbon credit profit comparison. give me data driven insights so that i can model the profits i can produce',
--             ' Answer: '
--         )
--     ) AS response;

--select * from aggregated_content;

    

-- Tables info
-- 1> pdf_tags_table with: PDF_TAG_EMBEDDINGS VECTOR column
-- 2> pdf_fullinfo table with: pdf_vectors column

CREATE OR REPLACE TABLE QueryTable_For_fullPDF AS
SELECT 
    t1.RELATIVE_PATH,
    t1.SIZE,
    t1.PAGE_COUNT,
    t1.PDF_TAG,
    t1.PDF_TAG_EMBEDDINGS,
    t2.pdf_split,
    t2.pdf_vectors
FROM 
    pdf_tags_table AS t1
JOIN 
    pdf_fullinfo AS t2
ON 
    t1.RELATIVE_PATH = t2.RELATIVE_PATH;

select * from querytable_for_fullpdf;




SELECT snowflake.cortex.complete(
    'mistral-large2', 
    CONCAT( 
        'Answer the question based on the context. Be concise.','Context: ',
        (
           SELECT LISTAGG(pdf_split[0], ' ')  from MethaneGPT.PUBLIC.PDF_FullInfo
            ORDER BY VECTOR_L2_DISTANCE(
            SNOWFLAKE.CORTEX.EMBED_TEXT_768('snowflake-arctic-embed-m', 
            'give me data driven insights on global methane emission trends over the years.'), pdf_vectors) limit 5
        ),
        ' Question: ', 
        'give me data driven insights on global methane emission trends over the years.',
        'Answer: '
    )
) as response;



select * from pdf_fullinfo;


SELECT LISTAGG(chunk[0], ' ') FROM MethaneGPT.PUBLIC.methaneragresults
    ORDER BY VECTOR_L2_DISTANCE(
    SNOWFLAKE.CORTEX.EMBED_TEXT_768('e5-base-v2',
                    'give me data driven insights on global methane emission trends over the years.'), Embedding_Vector) LIMIT 7;










