import sqlite3
import pandas as pd
from argostranslate import package, translate
import multiprocessing
from tqdm import tqdm

# STEP 1: Install Dutch-English translation package
def install_language_package():
    print("Checking for Dutch-English language package...")
    available_packages = package.get_available_packages()
    dutch_to_english = next(
        (pkg for pkg in available_packages if pkg.from_code == "nl" and pkg.to_code == "en"),
        None
    )
    if dutch_to_english:
        print("Installing Dutch-English package...")
        package.install_from_path(dutch_to_english.download())
    else:
        print("Dutch-English package not found or already installed.")

# STEP 2: Load Argos translator object for Dutch to English
def get_translator():
    installed_languages = translate.load_installed_languages()
    dutch = next(lang for lang in installed_languages if lang.code == "nl")
    english = next(lang for lang in installed_languages if lang.code == "en")
    return dutch.get_translation(english)

# STEP 3: Translate safely (for multiprocessing)
# noinspection PyUnresolvedReferences
def translate_row(row):
    global translator
    result = row.copy()
    try:
        # Only translate if there's actual text; otherwise leave as empty string
        result['verdachte_en']   = translator.translate(str(row.get('verdachte', '')  )) if str(row.get('verdachte', '')).strip()   else ""
        result['beslissing_en']  = translator.translate(str(row.get('beslissing', '') )) if str(row.get('beslissing', '')).strip()  else ""
        result['strafmaat_en']   = translator.translate(str(row.get('strafmaat', '')  )) if str(row.get('strafmaat', '')).strip()   else ""
    except Exception as e:
        print(f"Error translating row ID {row.get('id')}: {e}")
        result['verdachte_en']   = ""
        result['beslissing_en']  = ""
        result['strafmaat_en']   = ""
    return result

# Needed for multiprocessing translator setup
translator = None

def init_worker():
    global translator
    translator = get_translator()

# STEP 4: Process and translate entire SQLite table with multiprocessing
# noinspection SqlNoDataSourceInspection,PyShadowingNames
def translate_sqlite_table(db_path, table_name, output_table_name):
    conn = sqlite3.connect(db_path)

    df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
    print(f"Loaded {len(df)} rows (and {len(df.columns)} columns) from '{table_name}'.")

    # Install and load language model
    install_language_package()
    translate.load_installed_languages()  # Needed for subprocess cache

    # Translate in parallel with progress bar
    print("Starting parallel translation...")
    with multiprocessing.Pool(initializer=init_worker) as pool:
        translated_rows = list(
            tqdm(
                pool.imap(translate_row, df.to_dict(orient="records")),
                total=len(df),
                desc="Translating rows"
            )
        )

    # Build a DataFrame that now contains every original column + your three new "_en" columns
    translated_df = pd.DataFrame(translated_rows)

    # Save to new table (will replace if already exists)
    translated_df.to_sql(output_table_name, conn, if_exists="replace", index=False)
    print(f"Saved translated data to new table '{output_table_name}'.")
    conn.close()

# ====== MAIN SCRIPT ======
if __name__ == "__main__":
    db_path = "C:\\Users\\Sammy\\Documents\\SQL DBs\\rechtspraak_sqlite.db"
    table_name = "filtered_rechtspraak"
    output_table_name = "filtered_rechtspraak_en"
    translate_sqlite_table(db_path, table_name, output_table_name)
