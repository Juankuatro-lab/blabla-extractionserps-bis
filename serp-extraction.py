import streamlit as st
import aiohttp
import asyncio
import pandas as pd
import re
import uuid

st.set_page_config(page_title="Extraction SERP DataForSEO", layout="wide")
st.title("🔍 Extraction SERP - DataForSEO")

# --- Sidebar config ---
st.sidebar.header("🔧 Configuration")

api_user = st.sidebar.text_input("Identifiant API (login)", value="ilan.lellouche@hetic.net")
api_password = st.sidebar.text_input("Mot de passe API", value="c66104772aea8703", type="password")
domain = st.sidebar.selectbox("Domaine Google", [
    "google.fr", "google.be", "google.ca", "google.com", "google.de", "google.co.uk", "google.it", 
    "google.es", "google.nl", "google.com.au", "google.com.br", "google.co.in", "google.co.jp", 
    "google.co.kr", "google.se", "google.no", "google.dk", "google.fi", "google.co.za", "google.ch",
    "google.co.il", "google.com.mx", "google.pl", "google.at", "google.ru", "google.com.sa", "google.ae",
    "google.gr", "google.hu", "google.com.tr", "google.co.th", "google.co.id", "google.com.ph"
], index=0)
location = st.sidebar.selectbox("Pays", [
    "France", "United States", "Germany", "United Kingdom", "Italy", "Spain", "Netherlands", "Australia",
    "Brazil", "India", "Japan", "South Korea", "Sweden", "Norway", "Denmark", "Finland", "South Africa",
    "Switzerland", "Israel", "Mexico", "Poland", "Austria", "Russia", "Turkey", "Thailand", "Indonesia",
    "Philippines", "Belgium", "Canada", "China"
], index=0)
language = st.sidebar.selectbox("Langue", [
    "French", "English", "German", "Spanish", "Italian", "Dutch", "Portuguese", "Russian", "Turkish", 
    "Japanese", "Korean", "Swedish", "Norwegian", "Danish", "Finnish", "Chinese", "Arabic"
], index=0)
depth = st.sidebar.slider("Nombre de résultats à extraire", 10, 100, 100, step=10)
batch_size = st.sidebar.slider("Taille des batchs", 1, 100, 100)

st.sidebar.markdown("---")

# --- Upload keywords or manual entry ---
keywords = []
input_mode = st.radio("Mode de saisie des mots-clés", ["Saisie manuelle", "Import CSV"])
if input_mode == "Saisie manuelle":
    keyword_input = st.text_area("Entrez vos mots-clés (un par ligne)")
    if keyword_input:
        keywords = [kw.strip() for kw in keyword_input.strip().split("\n") if kw.strip()]
else:
    uploaded_file = st.file_uploader("Upload d'un fichier CSV avec une colonne 'keyword'", type=["csv"])
    if uploaded_file:
        df = pd.read_csv(uploaded_file)
        if "keyword" in df.columns:
            keywords = df["keyword"].dropna().unique().tolist()
        else:
            st.error("Le fichier CSV doit contenir une colonne 'keyword'.")

# --- Estimation du coût ---
def estimate_cost(keywords, cost_per_task=0.00075):
    # Calculer le nombre total de tâches
    total_tasks = len(keywords)
    # Calculer le coût estimé
    estimated_cost = total_tasks * cost_per_task
    return estimated_cost

# Afficher l'estimation du coût
if keywords:
    estimated_cost = estimate_cost(keywords)
    st.sidebar.write(f"Coût estimé : ${estimated_cost:.4f}")

output_df = pd.DataFrame(columns=["keyword", "url", "domain", "position"])

# --- Fonctions utilitaires ---
def extract_domain(url):
    match = re.match(r"https?://([^/]+)/?", url)
    return match.group(1) if match else url

def chunkify(lst, size):
    return [lst[i:i + size] for i in range(0, len(lst), size)]

async def fetch(session, url, method="GET", payload=None):
    auth = aiohttp.BasicAuth(api_user, api_password)
    headers = {"Content-Type": "application/json"}
    async with session.request(method, url, json=payload, auth=auth, headers=headers) as response:
        if response.status != 200:
            text = await response.text()
            raise Exception(f"Erreur API: {response.status} - {text}")
        return await response.json()

async def process_batch(session, batch_keywords):
    payload = [
        {
            "language_name": language,
            "location_name": location,
            "keyword": kw,
            "depth": depth,
            "se_domain": domain,
        }
        for kw in batch_keywords
    ]

    post_url = "https://api.dataforseo.com/v3/serp/google/organic/live/advanced"
    result = await fetch(session, post_url, method="POST", payload=payload)
    batch_results = []

    for task in result.get("tasks", []):
        keyword = task.get("data", {}).get("keyword", "")
        if not task.get("result"):
            batch_results.append({
                "keyword": keyword,
                "url": None,
                "domain": None,
                "position": None
            })
            continue

        items = task["result"][0].get("items", [])
        for item in items:
            if item.get("type") == "organic":
                url = item.get("url", "")
                domain_name = extract_domain(url)
                position = item.get("rank_group")
                batch_results.append({
                    "keyword": keyword,
                    "url": url,
                    "domain": domain_name,
                    "position": position
                })
    return batch_results

async def run_extraction_parallel(all_keywords):
    tasks = []  # Liste des tâches asynchrones
    progress_bar = st.progress(0)
    all_results = []

    async with aiohttp.ClientSession() as session:
        for i, keyword in enumerate(all_keywords):
            task = asyncio.create_task(process_batch(session, [keyword]))  # Crée une tâche asynchrone pour chaque mot-clé
            tasks.append(task)
            
            if len(tasks) >= batch_size:  # Paralléliser les requêtes par lot
                results = await asyncio.gather(*tasks)  # Attendre que tous les batchs se terminent
                for result in results:
                    all_results.extend(result)  # Ajouter les résultats au tableau final
                tasks.clear()  # Réinitialiser la liste de tâches pour le prochain lot
                
            progress_bar.progress((i + 1) / len(all_keywords))  # Mettre à jour la barre de progression
            await asyncio.sleep(0.1)  # Moins de pause pour rendre l'extraction plus rapide

        # Récupérer les résultats restants (pour les mots-clés restants dans le dernier lot)
        if tasks:
            results = await asyncio.gather(*tasks)
            for result in results:
                all_results.extend(result)

    return all_results

if st.button("🚀 Lancer l'extraction") and keywords:
    if not api_user:
        st.warning("Veuillez renseigner vos identifiants API.")
    else:
        st.info(f"Extraction en cours pour {len(keywords)} mot(s)-clé(s)...")
        results = asyncio.run(run_extraction_parallel(keywords))
        output_df = pd.DataFrame(results)
        st.success(f"Extraction terminée. {len(output_df)} résultats trouvés.")
        st.dataframe(output_df)

        # Export CSV
        csv = output_df.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="📂 Télécharger les résultats en CSV",
            data=csv,
            file_name='serp_results.csv',
            mime='text/csv'
        )
