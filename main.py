from ollama import chat
from pydantic import BaseModel
import time
import csv
import json
import redis


class AmazonProduct(BaseModel):
    vk_asin: str | None
    vk_marketplace: str | None
    ek_link: str | None
    EK: float | None
    VK: float | None
    title: str | None
    margin: float | None
    profit: float | None


print("Starting...")
r = redis.Redis(host='localhost', port=6379, db=2, decode_responses=True)

# Pfad zur CSV-Datei
csv_datei = 'dd_deals2.csv'

finished = False

while True:
    try:
        with open(csv_datei, mode='r', encoding='utf-8') as file:
            reader = csv.DictReader(file, delimiter=',')  # \t als Separator

            # Durch jede Zeile in der CSV iterieren
            for row in reader:
                # Annahme: Die Spalte 'raw_embed_json' enthält den JSON-String
                raw_embed_json = row.get('raw_embed_json', '')
                raw_message_json = row.get('raw_message_json', '')
                message_id = row.get('message_id', '')
                if r.exists(f"discord:csv:{message_id}"):
                    print(f"Key already exists for message_id: {message_id}")
                    continue
                # if raw_embed_json:  # Wenn der JSON-String nicht leer ist
                start = time.time()

                # raw_embed_json = '{"'+raw_embed_json.replace("\'", "")[1:-1]
                # json_dict = json.loads(raw_embed_json)
                # if not "footer" in json_dict:
                #     continue

                # # Überprüfe, ob der Schlüssel existiert, bevor du ihn entfernst
                # if 'image' in json_dict:
                #     del json_dict['image']

                # if 'footer' in json_dict:
                #     if 'icon_url' in json_dict['footer']:
                #         del json_dict['footer']['icon_url']
                #     if 'proxy_icon_url' in json_dict['footer']:
                #         del json_dict['footer']['proxy_icon_url']

                # if 'color' in json_dict:
                #     del json_dict['color']

                # if 'type' in json_dict:
                #     del json_dict['type']

                response = chat(
                    messages=[
                        {
                            'role': 'user',
                            'content':
                            'Deine Aufgabe ist es jetzt, aus dem Discord Embend als JSON dastellt und einer Discord Nachricht (auch als json dargestellt), die benötigten Informationen auszulesen und zurückzugeben. Pro Deal Nachricht erhälst du also immer beides ein Embed json  und ein message json. Es ist öfters mals so, dass nur eins von beiden mit werten befüllt ist.  Besonders wenn nur eine Nachricht verfügbar ist und kein embed, ist die Wahrscheinlichkeit relativ hoch, dass es sich einfach nur um eine Notiz oder einen Hinweis handelt. Solche Nachrichten sind als "Notiz" zu markieren .Es handelt sich um Nachrichten aus einem Discord Channel, welche meist Deals beinhalten oder Deal bezogene Informationen bereitstellen, also den Einkauf eines Produktes bei einem Einzelhändler und dann den Verkauf mit Gewinn dieses Produktes bei Amazon. Es ist wichtig das du immer versuchst so viele Attribute wie möglich zu extrahiern. Solltest du mehr als 2 Attribute nicht finden, markiere bitte den Deal als "unvollständig"  '
                            'asin stellt die ASIN des Produktes da, das auch im Text vorkommt bzw in dem Amazon Link hier: https://www.amazon/.<Marktplatz Domain>/dp/<ASIN> so zum Beispiel ist der "amazon_link" wo das Produkt verkauft werden soll https://www.amazon.de/dp/B09D3OLKPM und die asin somit B09D3OLKPM.'
                            '"buy_link" stellt den Link (also AUSSCHLIEßLICH ein HTTPS LINK) da, wo das Produkt gekauft werden soll, welcher oft im "url" Key im JSON vorkommt.  "ROI" stell den Return on Invest da der auch im Text vorkommt. "profit" ist der Gewinn, der auch im Text vorkommt. "EK" ist der Einkaufspreis, der auch im Text vorkommt. "VK" ist der Verkaufspreis, der auch im Text vorkommt. "title" ist der Titel des Produktes, das auch im Text vorkommt.'
                        },
                        {
                            'role': 'user',
                            'content': 'raw_embed_json: ' + raw_embed_json,
                        },
                        {
                            'role': 'user',
                            'content': 'raw_message_json: ' + raw_message_json,
                        }
                    ],
                    # model='deepseek-r1:7b',
                    # model='deepseek-r1:1.5b',
                    # model='deepseek-r1:14b',
                    model='deepseek-r1:14b',
                    format=AmazonProduct.model_json_schema(),
                )

                try:
                    product = AmazonProduct.model_validate_json(
                        response.message.content)
                except Exception as e:
                    print(f"Error: {e}")
                    print(response.message.content)
                    continue

                end = time.time()
                dauer = end - start
                print(f"{product} - {dauer:.2f} Sekunden")

                r.json().set(f"discord:csv:{message_id}", '$', {
                    "message_id": message_id, "raw_embed_json": raw_embed_json, "raw_message_json": raw_message_json, "result": product.model_dump()})
        break
    except:
        print("Fehler beim Verarbeiten der CSV-Datei. Warte 10 Sekunden und versuche es erneut.")
        time.sleep(10)
        continue
