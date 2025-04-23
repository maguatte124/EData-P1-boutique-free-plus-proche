#Lecture
import pandas as pd
df =pd.read_csv("free_shop.csv", sep=";")


import sqlite3
from math import radians, cos, sin, asin, sqrt


# === 2. Connexion SQLite ===
conn = sqlite3.connect("boutiques.db")
cursor = conn.cursor()

# === 3. Créer la table ===
cursor.execute('''
    DROP TABLE IF EXISTS boutique
''')

cursor.execute('''
    CREATE TABLE boutique (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        longitude REAL,
        latitude REAL,
        shopDescription TEXT,
        sunusng INTEGER,
        ipom INTEGER,
        weiwei INTEGER
    )
''')

# === 4. Insérer les données ===
df.to_sql("boutique", conn, if_exists='append', index=False)

# === 5. Fonction Haversine pour calculer la distance ===
def haversine(lon1, lat1, lon2, lat2):
    # convert degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    km = 6371 * c
    return km

# === 6. Entrée utilisateur ===
user_lat = float(input("Entrez votre latitude : "))
user_lon = float(input("Entrez votre longitude : "))
mobile = input("Entrez le nom du mobile recherché (sunusng, ipom, weiwei) : ").strip().lower()

if mobile not in ["sunusng", "ipom", "weiwei"]:
    print("❌ Mobile inconnu.")
    exit()

# === 7. Récupérer toutes les boutiques ===
cursor.execute("SELECT * FROM boutique")
rows = cursor.fetchall()

# === 8. Trouver la boutique la plus proche avec stock > 0 ===
min_distance = float("inf")
closest_shop = None

for row in rows:
    shop_id, lon, lat, desc, sunusng, ipom, weiwei = row
    stock = {"sunusng": sunusng, "ipom": ipom, "weiwei": weiwei}[mobile]
    if stock > 0:
        distance = haversine(user_lon, user_lat, lon, lat)
        if distance < min_distance:
            min_distance = distance
            closest_shop = {
                "id": shop_id,
                "description": desc,
                "latitude": lat,
                "longitude": lon,
                "stock": stock,
                "distance_km": round(distance, 2)
            }

# === 9. Affichage résultat ===
if closest_shop:
    print("\n✅ Boutique la plus proche :")
    print(f"📍 {closest_shop['description']}")
    print(f"📦 Stock de {mobile} : {closest_shop['stock']}")
    print(f"📏 Distance : {closest_shop['distance_km']} km")
    # Bonus : lien Google Maps
    print(f"🌍 Carte : https://www.google.com/maps?q={closest_shop['latitude']},{closest_shop['longitude']}")
else:
    print("❌ Aucune boutique avec ce mobile en stock.")

conn.close()
