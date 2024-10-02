import os 
import zipfile 
import duckdb 
 
 
def extract_zip_file(zip_file_path, target_dir): 
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        zip_ref.extractall(target_dir)
 
 
def load_json_gz_files_in_duck_db(json_dir): 
    con = duckdb.connect("github_stars.db")
    json_path = os.path.join(json_dir,"*.json.gz")
    con.sql("CREATE SCHEMA IF NOT EXISTS source")
    con.sql(f"CREATE TABLE source.gharchive AS SELECT * FROM '{json_path}'")
    con.close()
 
if __name__ == "__main__": 
    data_dir = os.path.join(os.path.dirname(__file__), "..", "data") 
 
    zip_file_path = os.path.join(data_dir, "gharchive_sample.zip") 
    temp_folder = "./tmp" 
    extract_zip_file(zip_file_path, temp_folder) 
 
    unpacked_zip_dir = os.path.join(temp_folder, "gharchive_sample") 
    load_json_gz_files_in_duck_db(unpacked_zip_dir) 


