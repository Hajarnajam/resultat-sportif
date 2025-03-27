import os
import time
import pandas as pd
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from pymongo import MongoClient
import chardet  


client = MongoClient(
    "mongodb+srv://hajarnajam:hajarhajar123@sitesportif.fpfdh.mongodb.net/sports_data?retryWrites=true&w=majority&appName=SiteSportif"
)

db = client["SiteSportif"]     
collection = db["results"]     


INCOMING = r"C:\Users\hajar\OneDrive\Desktop\Projet-sportif-\watcher\INCOMING"
PROCESSED = r"C:\Users\hajar\OneDrive\Desktop\Projet-sportif-\watcher\PROCESSED"
CSV = r"C:\Users\hajar\OneDrive\Desktop\Projet-sportif-\watcher\CSV"   

class FileHandler(FileSystemEventHandler):
    def on_created(self, event):
        
        if event.is_directory or not event.src_path.endswith(".txt"):
            return
        
        print(f"📂 Nouveau fichier détecté : {event.src_path}")
        process_txt_file(event.src_path)

def process_txt_file(file_path):
    try:
        
        file_name = os.path.basename(file_path)
        csv_filename = os.path.splitext(file_name)[0] + ".csv"
        csv_filepath = os.path.join(CSV, csv_filename)

        if os.path.exists(csv_filepath):
            print(f"⚠ Le fichier {csv_filename} existe déjà dans le dossier CSV. Aucun ajout effectué.")
            return  
        
        with open(file_path, 'rb') as f:
            raw_data = f.read()
            result = chardet.detect(raw_data)
            encoding = result['encoding']
            print(f"📄 Encodage détecté : {encoding}")

        df = pd.read_csv(file_path, delimiter='\t', engine='python', header=0, dtype=str, encoding=encoding, skipinitialspace=True, keep_default_na=False, na_filter=False)

        df.columns = df.columns.str.replace(r"(\s?\.\s?)", "_", regex=True)  
        df.columns = df.columns.str.replace(r"[ .]", "_", regex=True)  
        df.columns = df.columns.str.replace("-", "_")  
        df.columns = df.columns.str.replace("#", "") 
        df.columns = df.columns.str.rstrip("_")  

        print("Aperçu des noms de colonnes après nettoyage final :")
        print(df.head())

        def clean_keys(record):
            return {key.replace(".", "").replace(" ", "").replace("-", "_"): value for key, value in record.items()}
     
        data = df.to_dict(orient="records")
        
        data = [clean_keys(record) for record in data]

      
        for record in data:
            record["file_name"] = file_name  

        if data:
            collection.insert_many(data)
            print(f"✅ {len(data)} enregistrements insérés dans MongoDB.")
        else:
            print("⚠ Aucun enregistrement trouvé dans le fichier.")

        if not os.path.exists(CSV):
            os.makedirs(CSV)
        df.to_csv(csv_filepath, index=False)
        print(f"✅ Fichier converti en CSV : {csv_filepath}")

        
        if not os.path.exists(PROCESSED):
            os.makedirs(PROCESSED)
        archive_filepath = os.path.join(PROCESSED, file_name)
        os.rename(file_path, archive_filepath)
        print(f"📁 Fichier original déplacé vers : {archive_filepath}")

    except Exception as e:
        print(f"❌ Erreur lors du traitement de {file_path} : {e}")

if __name__ == "_main_":
    
    event_handler = FileHandler()
    observer = Observer()
    observer.schedule(event_handler, INCOMING, recursive=False)
    
    print(f"🚀 Surveillance du dossier : {INCOMING}")
    observer.start()
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    
    observer.join()