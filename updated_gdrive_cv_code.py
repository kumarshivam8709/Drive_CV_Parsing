import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
import gdown
import requests
import PyPDF2
import os
import json
import docx
from dotenv import load_dotenv
from langchain.embeddings.openai import OpenAIEmbeddings
from langchain.text_splitter import CharacterTextSplitter
from langchain_community.vectorstores import FAISS
from langchain.chains.question_answering import load_qa_chain
from langchain_community.llms import OpenAI

load_dotenv()
openai_api_key = os.getenv("OPENAI_API_KEY")
os.environ["OPENAI_API_KEY"] = openai_api_key

def parse_result_string(result_str):
    extracted_data = {}
    for line in result_str.split('\n'):
        if ':' in line:
            key, value = line.split(': ', 1)
            extracted_data[key] = value
    return extracted_data

SERVICE_ACCOUNT_KEY_PATH = 'D:\Teksands\Assignment_3\secure-bonus-417409-862221b1de5b.json'
SCOPES = ['https://www.googleapis.com/auth/drive']

folder_id = input("Enter folder ID: ")

credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_KEY_PATH, scopes=SCOPES)
service = build('drive', 'v3', credentials=credentials)

def get_files_in_folder(parent_id):
    query = f"'{parent_id}' in parents and trashed=false"
    response = service.files().list(q=query, fields='files(name,id,mimeType,webViewLink,createdTime,modifiedTime)').execute()
    files = response.get('files', [])
    '''dfs = [pd.DataFrame(files)]
    for file in files:
        if file['mimeType'] in ['application/pdf', 'application/vnd.openxmlformats-officedocument.wordprocessingml.document', 'application/msword']:
            dfs.append(get_files_in_folder(file['id']))
    return pd.concat(dfs, ignore_index=True)'''
    dfs = []
    for file in files:
        dfs.append(pd.DataFrame([file]))
        if file['mimeType'] in ['application/vnd.google-apps.folder']:
            dfs.append(get_files_in_folder(file['id']))  # Recursive call for subfolders
    return pd.concat(dfs, ignore_index=True)
folder = service.files().get(fileId=folder_id, fields='name').execute()
df = get_files_in_folder(folder_id)
df = df[df['mimeType'].isin(['application/pdf', 'application/vnd.openxmlformats-officedocument.wordprocessingml.document', 'application/msword'])]
df["text"] = ""
if not os.path.exists("./temp_dir"):
    os.makedirs("./temp_dir")
for index, value in df['webViewLink'].items():
        url = value
        r = requests.get(url)
        if r.status_code == 200:
            output = r"./temp_dir/" + df["name"][index] 
            gdown.download(url, output, fuzzy=True)

extracted_data_list = []

for index, value in df['name'].items():
        output = r"./temp_dir/" + df["name"][index]
        text = ""
        count = 0 
        if df.loc[index, 'mimeType'] == 'application/pdf':
             pdfFileObject = open(output, 'rb')
             pdfReader = PyPDF2.PdfReader(pdfFileObject)
             count = len(pdfReader.pages)
             for i in range(count):
                page = pdfReader.pages[i]
                text += page.extract_text() + "\n"
        else:
             document = docx.Document(output)
             text = []
             for paragraph in document.paragraphs:
                text.append(paragraph.text)
             text = "\n".join(text)
        
        text_splitter = CharacterTextSplitter(
            separator="\n",
            chunk_size=800,
            chunk_overlap=200,
            length_function=len,
        )
        texts = text_splitter.split_text(text)

        embeddings = OpenAIEmbeddings()
        document_search = FAISS.from_texts(texts, embeddings)
        chain = load_qa_chain(OpenAI(), chain_type="stuff")
        query = "Extract all the following values: Full Name, Email, Phone Number, Key Skills, Education, Location, Current Company, Current Designation, and Total Years of Experience from the PDF.Look for the name at the beggining of the document. Years of experience should be numeric and not tagged with any text as it has to be stored in a numeric field"
        docs = document_search.similarity_search(query)
        result = chain.run(input_documents=docs, question=query)
        extracted_data = parse_result_string(result)
        extracted_data_dict = {
            "Full Name": extracted_data.get("Full Name", ""),
            "Email": extracted_data.get("Email", ""),
            "Phone Number": extracted_data.get("Phone Number", ""),
            "Key Skills": extracted_data.get("Key Skills", ""),
            "Education": extracted_data.get("Education", ""),
            "Location": extracted_data.get("Location", ""),
            "Current Company": extracted_data.get("Current Company", ""),
            "Current Designation": extracted_data.get("Current Designation", ""),
            "Total Years of Experience": extracted_data.get("Total Years of Experience", "")
        }
        extracted_data_list.append(extracted_data_dict)

json_data = json.dumps(extracted_data_list, indent=2)
parsed_json_data = json.loads(json_data)
df1 = pd.DataFrame(parsed_json_data)
df1.to_excel("excel.xlsx", index=False)