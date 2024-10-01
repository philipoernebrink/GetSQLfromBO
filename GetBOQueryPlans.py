import xml.etree.ElementTree as ET
import pandas as pd
import aiohttp
import asyncio
import time

# Lists to store data
documentIds = []
documentNames = []
documentCuid = []
dataProviderIds = []
dataProviderNames = []
dataSourceTypes = []
sqlStatements = []
missingProvidersIndex = []
globalOrder = []
sqlTempKeeper = []
doc_index_counter = 0
inner_list_tracker = 0
sqltest = []

# Your login credentials and base URL
user_name = input("Username: ")
password = input("Password: ")
http_protocol = input("Protocol, http or https: ")
localhost = input("localhost: ")
port = input("port: ")
base_url = f'{http_protocol}://{localhost}:{port}/biprws'
folder_ids = input("Vilka mapp id:n? format 'id1','id2' etc. : ")
kinds = input("Vilka typer av dokument, format 'typ1', 'typ2' etc. : ")
folder_term = input("1 eller 2.\n1 = 'SI_PARENT_FOLDER (id)'\n2 = 'SI_ANCESTOR'\nsvar: ")
nr_of_objects = input("Hur många object vill du hämta max: ")

# Token to be used in API requests (initialized as None)
logon_token = None

# Asynchronous function to log in and get the token
async def login(session):
    global logon_token
    login_url = f"{base_url}/logon/long"
    login_payload = f'<attrs xmlns="http://www.sap.com/rws/bip"><attr name="password" '\
                f'type="string">{password}</attr><attr name="clientType" '\
                f'type="string">Enterprise</attr>'\
                f'<attr name="auth" type="string" possibilities='\
                f'"secEnterprise,secLDAP,secWinAD,secSAPR3">secEnterprise</attr>'\
                f'<attr name="userName" type="string">{user_name}</attr></attrs>'
    header = {"content-type": "application/xml", "accept": "application/xml"}
    
    async with session.post(login_url, data=login_payload, headers=header) as response:
        if response.status == 200:
            response_text = await response.text()
            root = ET.fromstring(response_text)
            logon_token = root.find('.//{http://www.sap.com/rws/bip}attr[@name="logonToken"]').text
            print(f"Logged in. Token: {logon_token}")
        else:
            print(f"Login failed with status code {response.status}")
            logon_token = None
            cont = input("fortsätt")

# Asynchronous function to log out
async def logout(session):
    logout_url = f"{base_url}/logoff"
    headers = {"X-SAP-LogonToken": f'"{logon_token}"', "accept":"application/xml", "content-type":"application/xml"}
    
    async with session.post(logout_url, headers=headers) as response:
        if response.status == 200:
            print("Logged out successfully.")
        else:
            print(f"Logout failed with status code {response.status}")
            cont = input("fortsätt")

# Asynchronous function to make API requests
async def fetch_ids(session, url, payload,identifier):
    headers = {"X-SAP-LogonToken": f'"{logon_token}"', "accept":"application/xml", "content-type":"application/xml"}
    try:
        async with session.post(url, data=payload, headers=headers) as response:
                        response_text = await response.text()
                        print("Acquired")
                        await asyncio.sleep(1)
                        print("Released")
                        return response_text, identifier
    except Exception as e:
            print(f"Error fetching data from {url}: {str(e)}")
            cont = input("fortsätt")
            return None, identifier

async def fetch_data(session, url, payload, identifier, semaphore):
    headers = {"X-SAP-LogonToken": f'"{logon_token}"', "accept":"application/xml", "content-type":"application/xml"}
    try:
        async with session.get(url, data=payload, headers=headers) as response:
                response_text = await response.text()
                print("Acquired")
                await asyncio.sleep(1)
                print("Released")
                return response_text, identifier
    except Exception as e:
        print(f"Error fetching data from {url}: {str(e)}")
        cont = input("fortsätt")
        return None, identifier

async def close_document(session, doc_numb, semaphore):
        headers = {"X-SAP-LogonToken": f'"{logon_token}"', "accept":"application/xml", "content-type":"application/xml"}   
        url_status_change = f'{base_url}/raylight/v1/documents/{doc_numb}'
        xml_status_change = "<document><state>Unused</state></document>"
        try:
            async with session.put(url_status_change, data=xml_status_change, headers=headers) as response:
                    status_code = response.status
                    print("changed state: ", status_code)
                    print("Acquired")
                    await asyncio.sleep(1)
                    print("Released")
        except Exception as e:
            print(f"Error fetching data from {url_status_change}: {str(e)}")
                

# Function to process the first API response and extract multiple fields
def process_xml_data_for_doc_ids(response_text):
    try:
        root = ET.fromstring(response_text)
        namespace2 = {"sap": "http://www.sap.com/rws/bip"}
        for attribute in root.findall('.//sap:attr', namespace2):
            attr_name = attribute.get("name")
            if attr_name == "SI_ID":
                documentIds.append(attribute.text)
            elif attr_name == "SI_NAME":
                documentNames.append(attribute.text)
            elif attr_name == "SI_CUID":
                documentCuid.append(attribute.text)
    except Exception as e:
        print("Error prasing xml for doc: ", e)
        cont = input("fortsätt")

def process_xml_data_for_dataproviders(response_text):
    try:
        root = ET.fromstring(response_text)
        provider_nbs = []
        provider_nmes = []
        provider_types = []
        if len(root) == 0:
            missingProvidersIndex.append(len(dataProviderIds))
            #provider_nbs.append("None")
            provider_nmes.append("None")
            provider_types.append("None")
        elif root.tag == "error":
            missingProvidersIndex.append(len(dataProviderIds))
            #provider_nbs.append("None")
            message_text = root.find(".//message").text
            provider_nmes.append(message_text)
            provider_types.append("None")
        else:
            for provider in root.findall('.//dataprovider'):      
                provider_id = provider.find(".//id").text
                provider_name = provider.find(".//name").text
                provider_sourcetype = provider.find(".//dataSourceType").text
                
                provider_nbs.append(provider_id)
                provider_nmes.append(provider_name)
                provider_types.append(provider_sourcetype)

        dataProviderIds.append(provider_nbs)
        dataProviderNames.append(provider_nmes)
        dataSourceTypes.append(provider_types)
    except Exception as e:
        print("Error prasing xml for dataproviders: ", e)
        cont = input("fortsätt")

def group_sql_statements(responses):
    try:
        for index in range(len(documentIds)):
            if index in missingProvidersIndex:
                sqlStatements.append(["finns ingen dataprovider"])
                continue
            nb_of_providers = len(dataProviderIds[index])
            sql_collection = []
            try:
                for respons in range(0,nb_of_providers):
                    sql_codes = process_xml_data_for_queryplan(responses[respons])
                    sql_collection.append(sql_codes)
                sqlStatements.append(sql_collection)
            except Exception as e:
                print("no providers, error: ", e)
            responses = responses[nb_of_providers:]

    except Exception as e:
        print("Error parsing xml for queries: ", e)
        print(responses)
        cont = input("fortsätt")

def process_xml_data_for_queryplan(response_text):
    sqlcodes = []
    root = ET.fromstring(response_text)
    if root.tag == "queryplan":
        for statement in root.findall(".//statement"):
            sql = statement.text
            sqlcodes.append(sql)
    elif root.tag == "dataprovider":
        for statement in root.findall('.//property[@key="sql"]'):
            sql = statement.text
            sqlcodes.append(sql)
    elif root.tag == "error":
        message_text = root.find(".//message").text
        sqlcodes.append("Ingen SQL hittad. Felmeddelande: {}".format(message_text))
    return sqlcodes

# Dispatcher function to call the correct processing function based on the identifier
def process_xml_data(response_text, identifier):
    if identifier == 'documentid':
        process_xml_data_for_doc_ids(response_text)  # Extract doc_ids and other fields
    elif identifier == 'dataprovider':
        process_xml_data_for_dataproviders(response_text)  # Process further data

async def request_chain(session,url,xml_payload,identifier,method,semaphore,id_numb):
    async with semaphore:
        retrive_data = await fetch_data(session, url, xml_payload, identifier,semaphore=semaphore)
        document_change_status = await close_document(session, id_numb, semaphore)
        return retrive_data

# Asynchronous batch request handler
async def fetch_all_data(api_urls, xml_payloads,identifier, method,doc_ids=None):
    # Limit the number of concurrent requests
    semaphore = asyncio.Semaphore(5)
    async with aiohttp.ClientSession() as session:
        tasks = []
        # use function to get id, is only one request
        if method == "post":
            for url, xml_payload in zip(api_urls, xml_payloads):
                tasks.append(fetch_ids(session, url, xml_payload, identifier))
        # use function to get provider ids and sql:s, is many requests
        else: 
            for url, xml_payload,id_numb in zip(api_urls, xml_payloads, doc_ids):
                tasks.append(request_chain(session, url, xml_payload, identifier, method,semaphore=semaphore,id_numb=id_numb))
        responses = await asyncio.gather(*tasks)
        if method != "put":
            responses_list = []
            for response, identifier in responses:
                responses_list.append(response)
            if identifier == "queryplan":
                print(len(responses_list))
                group_sql_statements(responses_list) 
            else:
                for response, identifier in responses:  
                    if response:
                        process_xml_data(response, identifier)
                    else:
                        print("inget response här + ", identifier)

# Main workflow
async def main_workflow():
    async with aiohttp.ClientSession() as session:
        # Step 1: Log in and get the token
        await login(session)
        if not logon_token:
            print("Exiting because login failed.")
            return

        # Step 2: Prepare initial API request (with XML payload)
        if folder_term == "1":
            print("detta funkar")
            folder_search = "SI_PARENT_FOLDER"
        elif folder_term == "2":
            folder_search = "SI_ANCESTOR"
        initial_url = f"{base_url}/v1/cmsquery?page=1&pagesize={nr_of_objects}"
        initial_xml_payload = F"""
        <attrs xmlns="http://www.sap.com/rws/bip">
            <attr name="query" type="string">SELECT SI_ID, SI_NAME, SI_CUID, SI_PARENT_FOLDER, SI_PARENT_FOLDER_CUID, SI_KIND FROM 
            CI_INFOOBJECTS WHERE SI_KIND not in ('Folder','FavoritesFolder','Inbox','PersonalCategory') and SI_INSTANCE_OBJECT = 0 and SI_INSTANCE != 1 AND 
            SI_KIND in ({kinds}) AND {folder_search} IN ({folder_ids}) ORDER BY SI_ID</attr>
        </attrs>
        """
        
        # Fetch initial response with document IDs and other data
        await fetch_all_data([initial_url], [initial_xml_payload], doc_ids=[], identifier="documentid", method="post")
        
        
        # Step 3: Prepare subsequent API requests using the extracted doc_ids
        print("Har hämtat dokument id, namn och cuid.")
        print("Antal dokument: ", len(documentIds))
        print("Tar en kort paus för att respektera rate limit")
        time.sleep(10)
        print("Igång igen, Fortsätter till dataprovider attribut")
        subsequent_urls = []
        subsequent_xml_payloads = []
        for doc_id_index in range(0, len(documentIds)):
            doc_id = documentIds[doc_id_index]
            new_url = f'{base_url}/raylight/v1/documents/{doc_id}/dataproviders/'
            subsequent_urls.append(new_url)
            # Construct the XML payload for each subsequent request
            subsequent_xml_payload = ""
            subsequent_xml_payloads.append(subsequent_xml_payload)

        # Step 4: Fetch subsequent data using the constructed URLs and payloads
        await fetch_all_data(subsequent_urls, subsequent_xml_payloads,identifier="dataprovider",method="get",doc_ids=documentIds)
        
        print("Har hämtat dokument och dataprovider attribut. Går vidare till sql:er")
        print("Tar en kort paus för att respektera rate limit")
        time.sleep(10)
        print("Igång igen. Går vidare till sql:er")
        queryplan_urls = []
        queryplan_xml_payloads = []
        query_doc_id = []
        for doc_id_index in range(0, len(documentIds)):
            doc_id = documentIds[doc_id_index]
            try:
                sources_list = dataSourceTypes[doc_id_index]
            except Exception as e:
                print("saknas dataprovider för detta dokument: ", documentNames[doc_id_index], " error: ", e)
                continue
            if sources_list[0] == "None":
                continue
            for source_type_index in range(0,len(sources_list)):
                provider_id = dataProviderIds[doc_id_index][source_type_index]
                if dataSourceTypes[doc_id_index][source_type_index] == "fhsql":
                    new_url2 = f"{base_url}/raylight/v1/documents/{doc_id}/dataproviders/{provider_id}/"
                    queryplan_urls.append(new_url2)
                    queryplan_xml_payloads.append("")
                    query_doc_id.append(documentIds[doc_id_index])
                elif dataSourceTypes[doc_id_index][source_type_index]:
                    new_url2 = f"{base_url}/raylight/v1/documents/{doc_id}/dataproviders/{provider_id}/queryplan"
                    queryplan_urls.append(new_url2)
                    queryplan_xml_payloads.append("")
                    query_doc_id.append(documentIds[doc_id_index])
        print("Antal url:er för queryplan: ", len(queryplan_urls))
        print("Antal docs utan dataprovider: ", len(missingProvidersIndex))
        # Step 5: Fetch queryplan using constructed URLs and payloads
        queryplan_responses = await fetch_all_data(queryplan_urls, queryplan_xml_payloads, identifier="queryplan",method="get",doc_ids=query_doc_id)
        print("Har hämtat dokument och dataprovider attribut, samt sql:er.")
        
        '''
        print("Tar en kort paus för att respektera rate limit")
        time.sleep(10)
        print("Igång igen. Går vidare till att ändra state på dokument till 'unused'")
        change_state_urls = []
        change_state_xml_payloads = []
        for doc_id_index2 in range(0, len(documentIds)):
            doc_id3 = documentIds[doc_id_index2]
            new_url3 = f'{base_url}/raylight/v1/documents/{doc_id3}'
            change_state_urls.append(new_url3)
            change_state_xml_payloads.append("<document><state>Unused</state></document>")
        # Step 6: change state of webi docs to "unused" to release memory from webi processing server, using put requests
        changeState_requests = await fetch_all_data(change_state_urls, change_state_xml_payloads, identifier="", method="put")
        '''
        # Step 6: Log out
        await logout(session)

# Run the main workflow
asyncio.run(main_workflow())

# Saving the collected data to a CSV file
'''
print(len(documentIds))
print(len(documentNames))
print(len(documentCuid))
print(len(dataProviderIds))
print(len(dataProviderNames))
print(len(dataSourceTypes))
print(len(sqlStatements))
print(documentIds)
print(dataProviderIds)

print('Antal index för docs utan dataprovider: ', len(missingProvidersIndex))
print("index för docs utan dataprovider: ", missingProvidersIndex)
print("document ids och dataprovider listor")
print(documentIds)

print(dataProviderNames)
print(dataSourceTypes)
print(sqlStatements)
print(sqltest)
'''

df = pd.DataFrame({
    'Document ID': documentIds,
    'Document Name': documentNames,
    'Document Cuid': documentCuid,
    'Data Provider Name': dataProviderNames,
    "DataSourceTypes" : dataSourceTypes,
    'SQL Statement': sqlStatements
})


# Save to CSV
df.to_csv('output1.csv', index=False, encoding = "utf-8-sig", sep="¤")
print("Data saved to output.csv")

exit_program = input("vill du avsluta programmet?: ")
