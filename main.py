from ollama import chat
from pydantic import BaseModel
import time
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

print("Starting...")
r = redis.Redis(host='localhost', port=6379, db=3, decode_responses=True)

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
            start = time.time()
            
            raw_embed_json = '{"'+raw_embed_json.replace("\'","")[1:-1]
            json_dict = json.loads(raw_embed_json)
            if not "footer" in json_dict:
                continue

            # Überprüfe, ob der Schlüssel existiert, bevor du ihn entfernst
            if 'image' in json_dict:
                del json_dict['image']

            if 'footer' in json_dict:
                if 'icon_url' in json_dict['footer']:
                    del json_dict['footer']['icon_url']
                if 'proxy_icon_url' in json_dict['footer']:
                    del json_dict['footer']['proxy_icon_url']

            if 'color' in json_dict:
                del json_dict['color']

            if 'type' in json_dict:
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
            #model='deepseek-r1:7b',
            #model='deepseek-r1:1.5b',
            #model='deepseek-r1:14b',
            model='deepseek-r1:32b',
            format=AmazonProduct.model_json_schema(),
            )
            
            try: 
                product = AmazonProduct.model_validate_json(response.message.content)
            except Exception as e:
                print(f"Error: {e}")
                print(response.message.content)
                continue

            end = time.time()
            dauer = end - start
            print(f"{product.asin} - {dauer:.2f} Sekunden")

            r.json().set(f"discord:csv:1:{message_id}", '$', {"embed": json_dict, "result": product.model_dump()})

