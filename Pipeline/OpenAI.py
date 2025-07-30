import os
from dotenv import load_dotenv
from openai import OpenAI
import pandas as pd
oj = os.path.join
load_dotenv()


class OpenAI_Bot:
    def __init__(self):
        self.key_OpenAI = os.getenv('key_OpenAI')
        self.client = OpenAI(api_key=self.key_OpenAI)
        self.model_gpt4 = 'gpt-4-turbo'
        self.model_gpt3 = 'gpt-3.5-turbo'
        self.model_gpt41_mini = 'gpt-4.1-mini'


    def get_responseContent(self, Prompt: str):
        response = self.client.chat.completions.create(
            model=self.model_gpt41_mini,
            messages=[{"role": "system", "content": "You are a helpful, accurate and concise assistance, which gives me exactly one clear answer"},
                      {"role": "user", "content": Prompt}]
        )

        return response.choices[0].message.content

    
    def get_vkCategory(self, s_Subject: list) -> dict:
        response = self.client.chat.completions.create(
            model=self.model_gpt41_mini,
            messages=[
                {"role": "system", "content": "You are a helpful, accurate and concise AI assistance, which gives me exactly one clear answer"},
                {"role": "user", "content": f"""
                    Please categorize each name by first identifying if it is a 
                    company, club, or organization. If it is, classify it based on 
                    its industry.

                    Return categories in increasing specificity. For example:  
                    - "Golf" → Sport, Golf
                    - "Golfpark" → Sport, Golf 
                    - "Salt" (Telecom Company) → Telecommunication  
                    - "Sky" → Entertainment, Streaming  
                    - "Lufthansa" → Travel, Flight
                    - "hostel" → Travel, Accomodation

                    Use the format: name: category1, category2 (if applicable). Return always 2 categoires, only if you have no idea return Unknown.
                    Please return values for every name you are recieving.

                    Example categories: Accommodation, Travel, Flight, Sport, Golf, 
                    Biking, Running, Streaming, Food, Restaurants, Groceries, 
                    Telecommunication, Transportation, Rental.

                    If a name is ambiguous or unknown, **estimate the most likely 
                    categories** based on similar transactions and typical expenses.
                    Try to ignore the not relevant information for example for "My hostel 1234" the relevant information is hostel.
                    Also identify core business on their website and compare with most common categories to identify the right category.
                    If you aren't sure with your guess, return Unknnown as the second category.  

                    Names to categorize: f'{str(s_Subject)}'
                    
                """}
            ]
        )

        res = response.choices[0].message.content

        # transform response into a dictionary
        vk_Category = {}
        for line in res.split('\n'):
            if line.strip() and ':' in line:
                merchant, category = line.split(':', 1)
                merchant = merchant.replace('-', '')
                vk_Category[merchant.strip()] = category.strip()

        return vk_Category
    

    def get_df_ZKBTransactionDetails(self, vk_Subject: dict, batch_size=50):
        s_result = []
        for i in range(0, len(vk_Subject), batch_size):
            vk_Subject_batch = vk_Subject[i:i + batch_size]

            Prompt = f"""
                Please extract following information from **each** transactions descriptions seperated by semicolon:
                
                1. **UID**: They come from the input. UID's help us match results. Keep them unchanged.
                2. **Company Name**: The name of the shop or the company without any address details (street name, postal code, city, st.).
                3. **City**: The city where the company is located. If no city is explicitly mentioned, extract it from the postal code.
                4. **Main Category**: A general classification of the transaction 
                    (e.g., "Groceries", "Transport", "Restaurant", "Pub", "Healthcare", "Gas station", "Entertainment", "Parking", "Retail", "Travel", "Sport").
                5. **Second Category**: A more specific sub-category (e.g., for "Transport", it could be "Taxi", "Train", "Car Rental").

                If the input is a person's name and a phone numer just put as Main Category "Friend" and keep the full name as company name.

                Make sure that similar and/or the same names get the same categories.

                If you aren't sure you can browse the internet to find the category of the company or place.
                If that doesn't work try to guess it from the name.
                If this also doesn't work, put NA.

                Return the result seperated by semicolon like this:

                Coop City; Zurich; Groceries; Supermarket
                Migros; Zurich; Groceries; Supermarket
                Uber; Berlin; Transport; Taxi
                Raststätte; Berlin; Gas station; Car
                Hello Pub; Berlin; Restaurant; Pub
                Golfpark; Berlin; Sport; Golf

                Transaction decriptions to analye: {'; '.join([f"{item['uid']}|{item['Subject']}" for item in vk_Subject_batch])}, return an object to every entry

                It's very important to return for every transaction description one object with information.
                Never return nothing. Always return something.
                Please also remove any commas which are inside the company name.

                Please return each transaction on a new line in the format:
                UID; Company Name; City; Main Category; Subcategory
                """

            res = self.get_responseContent(Prompt)
            for line in res.split('\n'):
                if line.strip():
                    s_part = [p.strip() for p in line.split(';')]
                    uid = s_part[0] if len(s_part) > 0 else 'NA'

                    if len(s_part) == 5:
                        company, city, cat1, cat2 = s_part[1:5]
                    else:
                        company, city, cat1, cat2 = 'NA', 'NA', 'NA', 'NA'

                    s_result.append({
                        'uid': uid,
                        'nm_subject': company,
                        'city': city,
                        'category_main': cat1,
                        'category_second': cat2
                })

        # convert results into dataframe
        df = pd.DataFrame(s_result)

        return df

