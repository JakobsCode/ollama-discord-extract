from ollama import chat
from pydantic import BaseModel

import csv
import json
import redis

class AmazonProduct(BaseModel):
  asin: str
  amazon_link: str
  buy_link: str
  ROI: float
  profit: float
  EK: float
  VK: float
  title: str

r = redis.Redis(host='localhost', port=6379, db=2, decode_responses=True)

# Pfad zur CSV-Datei
csv_datei = 'discord_v1.csv'

with open(csv_datei, mode='r', encoding='utf-8') as file:
    reader = csv.DictReader(file, delimiter='\t')  # \t als Separator

    # Durch jede Zeile in der CSV iterieren
    for row in reader:
        # Annahme: Die Spalte 'raw_embed_json' enthält den JSON-String
        raw_embed_json = row.get('raw_embed_json', '')
        message_id = row.get('message_id', '')
        if r.exists(f"discord:csv:1:{message_id}"):
            print(f"Key already exists for message_id: {message_id}")
            continue
        if raw_embed_json:  # Wenn der JSON-String nicht leer ist
            raw_embed_json = '{"'+raw_embed_json.replace("\'","")[1:-1]
            json_dict = json.loads(raw_embed_json)
            if not "footer" in json_dict:
                continue

            del json_dict['image']
            del json_dict['footer']['icon_url']
            del json_dict['footer']['proxy_icon_url']
            del json_dict['color']
            del json_dict['type']
            
            response = chat(
            messages=[
                                {
                'role': 'user',
                'content': 
                    'Deine Aufgabe ist es jetzt, aus dem Discord Embend als JSON dastellt, die benötigten Informationen auszulesen und zurückzugeben. Es stellt eine Benachrichtung für einen Amazon Deal da, also den Einkauf eines Produktes bei einem Einzelhändler und dann den Verkauf mit Gewinn dieses Produktes bei Amazon.'
                    'asin stellt die ASIN des Produktes da, das auch im Text vorkommt bzw in dem Amazon Link hier: https://www.amazon.<Marktplatz Domain>/dp/<ASIN> so zum Beispiel ist der "amazon_link" wo das Produkt gekauft werden soll https://www.amazon.de/dp/B09D3OLKPM und die asin somit B09D3OLKPM.'
                    '"buy_link" stellt den Link (also AUSSCHLIEßLICH ein HTTPS LINK) da, wo das Produkt gekauft werden soll, welcher oft im "url" Key im JSON vorkommt. "ROI" stell den Return on Invest da der auch im Text vorkommt. "profit" ist der Gewinn, der auch im Text vorkommt. "EK" ist der Einkaufspreis, der auch im Text vorkommt. "VK" ist der Verkaufspreis, der auch im Text vorkommt. "title" ist der Titel des Produktes, das auch im Text vorkommt.',
                },
                {
                'role': 'user',
                'content': json.dumps(json_dict),
                }
            ],
            model='deepseek-r1:14b',
            format=AmazonProduct.model_json_schema(),
            )

            product = AmazonProduct.model_validate_json(response.message.content)
            print(product)

            r.json().set(f"discord:csv:1:{message_id}", '$', {"embed": json_dict, "result": product.model_dump()})