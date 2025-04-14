from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, radians, sin, cos, asin, sqrt
from pyspark.sql.types import DoubleType
from decouple import config



# === Charger les infos depuis .env === avec #la bibliotheque  python-decouple
db_name = config("DB_NAME")
db_user = config("DB_USER")
db_password = config("DB_PASSWORD")
db_host = config("DB_HOST")
db_port = config("DB_PORT")
db_table = config("DB_TABLE")

jdbc_url = f"jdbc:postgresql://{db_host}:{db_port}/{db_name}"




# === 1. Créer la session Spark avec le driver PostgreSQL ===
spark = SparkSession.builder \
    .appName("FreeShop") \
    .config("spark.jars", "/home/etienne/Documents/etienne/Documents/VDE Python/EData-P1-boutique-free-plus-proche/postgresql-42.7.5.jar") \
    .getOrCreate()

# === 2. Lire le fichier CSV ===
csv_path = "/home/etienne/Documents/etienne/Documents/VDE Python/EData-P1-boutique-free-plus-proche/free_shop.csv"
df = spark.read.csv(csv_path, sep=";", header=True, inferSchema=True)

# === 3. Écrire dans PostgreSQL ===


# === Écriture vers PostgreSQL ===
df.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", db_table) \
    .option("user", db_user) \
    .option("password", db_password) \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()


# === 4. Lire depuis PostgreSQL ===
boutiques = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", db_table) \
    .option("user", db_user) \
    .option("password", db_password) \
    .option("driver", "org.postgresql.Driver") \
    .load()

# === 5. Entrée utilisateur ===
user_lat = float(input("Entrez votre latitude : \n"))
user_lon = float(input("Entrez votre longitude : \n"))
mobile = input("Entrez le nom du mobile recherché (sunusng, ipom, weiwei) : \n").strip().lower()

if mobile not in ["sunusng", "ipom", "weiwei"]:
    print("❌ Mobile inconnu.")
    spark.stop()
    exit()

# === 6. Calcul Haversine ===
df_haversine = boutiques.withColumn("lat_rad", radians(col("latitude"))) \
    .withColumn("lon_rad", radians(col("longitude"))) \
    .withColumn("user_lat_rad", radians(lit(user_lat))) \
    .withColumn("user_lon_rad", radians(lit(user_lon))) \
    .withColumn("dlat", col("lat_rad") - col("user_lat_rad")) \
    .withColumn("dlon", col("lon_rad") - col("user_lon_rad")) \
    .withColumn("a", sin(col("dlat") / 2) ** 2 + cos(col("user_lat_rad")) * cos(col("lat_rad")) * sin(col("dlon") / 2) ** 2) \
    .withColumn("c", 2 * asin(sqrt(col("a")))) \
    .withColumn("distance_km", col("c") * 6371)

# === 7. Filtrer sur stock > 0 ===
result = df_haversine.filter(col(mobile) > 0).orderBy("distance_km").limit(1).collect()

# === 8. Affichage ===
if result:
    shop = result[0]
    print("\n✅ Boutique la plus proche :")
    print(f"📍 {shop['shopDescription']}")
    print(f"📦 Stock de {mobile} : {shop[mobile]}")
    print(f"📏 Distance : {round(shop['distance_km'], 2)} km")
    print(f"🌍 Carte : https://www.google.com/maps?q={shop['latitude']},{shop['longitude']}")
else:
    print("❌ Aucune boutique avec ce mobile en stock.")

spark.stop()
