import redis
import json

# Verbinde zu Redis (Standard-Host und Port: localhost:6379)
r = redis.StrictRedis(host='localhost', port=6379, db=2, decode_responses=True)

# Holen Sie sich alle Schlüssel in der Datenbank
keys = r.keys('*')

# Öffne eine Datei, in der wir die JSON-Daten speichern
with open('redis_dump.json', 'w', encoding='utf-8') as file:
    # Iteriere über alle Keys und hole die JSON-Daten
    for key in keys:
        try:
            # Hole den JSON-String und lade ihn in ein Dictionary
            raw_data = r.get(key)
            
            # Überprüfen, ob der gespeicherte Wert als JSON vorliegt
            try:
                json_data = json.loads(raw_data)
            except json.JSONDecodeError:
                # Falls der Wert kein gültiger JSON ist, überspringen
                print(f"Schlüssel {key} ist kein gültiger JSON.")
                continue
            
            # Schreibe das JSON-Datenobjekt in die Datei (mit Schlüssel als Identifikator)
            json.dump({key: json_data}, file, ensure_ascii=False, indent=4)
            file.write("\n")  # Jede JSON-Daten pro Zeile

        except Exception as e:
            print(f"Fehler beim Verarbeiten von Schlüssel {key}: {e}")

print("Redis-Daten wurden erfolgreich in 'redis_dump.json' gespeichert.")
