import redis
import json

# Verbinde zu Redis (Standard-Host und Port: localhost:6379)
r = redis.StrictRedis(host='localhost', port=6379, db=2, decode_responses=True)

# Holen Sie sich alle Schlüssel in der Datenbank
keys = r.keys('discord:csv:*')

count = 0

# Öffne eine Datei, in der wir die JSON-Daten speichern
with open('redis_dump.json', 'w', encoding='utf-8') as file:
    # Iteriere über alle Keys und hole die JSON-Daten
    redis_list = []
    for key in keys:
        json_data = r.json().get(key)
        redis_list.append(json_data)

    json.dump(redis_list, file, ensure_ascii=False, indent=4)

print(f"Anzahl der Einträge mit 'vk_asin': {count}")
print(f"Anzahl der Einträge insgesamt: {len(redis_list)}")


print("Redis-Daten wurden erfolgreich in 'redis_dump.json' gespeichert.")
