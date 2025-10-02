import streamlit as st
import requests
import pandas as pd
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

st.set_page_config(page_title="Extraction SERP DataForSEO", layout="wide")
st.title("🔍 Extraction SERP - DataForSEO")

# --- Sidebar config ---
st.sidebar.header("🔧 Configuration")

api_user = st.sidebar.text_input("Identifiant API (login)", value="youradress")
api_password = st.sidebar.text_input("Mot de passe API", value="yourpassword", type="password")
domain = st.sidebar.selectbox("Domaine Google", [
    "google.com", "google.fr", "google.be", "google.ca", "google.com", "google.de", "google.co.uk", "google.it", 
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
max_workers = st.sidebar.slider("Nombre de threads simultanés", 1, 10, 5, help="Nombre de requêtes API lancées en parallèle. "
         "Valeur recommandée : 3-5 pour éviter le rate-limiting.")


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

# --- Estimation du coût MISE À JOUR ---
def estimate_cost(depth, num_keywords, priority="normal"):
    base_price = 0.0006  # Normal priority, 1ère page
    num_pages = max(1, depth // 10)
    
    if num_pages == 1:
        cost_per_keyword = base_price
    else:
        cost_per_keyword = base_price + (num_pages - 1) * (0.75 * base_price)
    
    total_cost = cost_per_keyword * num_keywords
    return total_cost, cost_per_keyword, num_pages

# Afficher l'estimation du coût
if keywords:
    estimated_cost, cost_per_kw, num_pages = estimate_cost(depth, len(keywords))
    
    st.sidebar.markdown("---")
    st.sidebar.markdown("### 💰 Estimation des coûts")
    st.sidebar.write(f"**Pages par mot-clé :** {num_pages}")
    st.sidebar.write(f"**Coût/mot-clé :** ${cost_per_kw:.6f}")
    st.sidebar.write(f"**Coût total :** ${estimated_cost:.4f}")
    
    if estimated_cost > 1:
        st.sidebar.warning(f"⚠️ Coût > $1")

# --- Fonctions utilitaires ---
def extract_domain(url):
    match = re.match(r"https?://([^/]+)/?", url)
    return match.group(1) if match else url

def process_keyword(keyword):
    """Traite un seul mot-clé"""
    payload = [{
        "language_name": language,
        "location_name": location,
        "keyword": keyword,
        "depth": depth,
        "se_domain": domain,
    }]
    
    url = "https://api.dataforseo.com/v3/serp/google/organic/live/advanced"
    headers = {"Content-Type": "application/json"}
    auth = (api_user, api_password)
    
    try:
        response = requests.post(url, json=payload, auth=auth, headers=headers, timeout=30)
        
        if response.status_code != 200:
            return [{
                "keyword": keyword,
                "url": None,
                "domain": None,
                "position": None,
                "error": f"API Error: {response.status_code}"
            }]
        
        result = response.json()
        keyword_results = []
        
        for task in result.get("tasks", []):
            if not task.get("result"):
                keyword_results.append({
                    "keyword": keyword,
                    "url": None,
                    "domain": None,
                    "position": None,
                    "error": "No results"
                })
                continue
            
            items = task["result"][0].get("items", [])
            for item in items:
                if item.get("type") == "organic":
                    url_result = item.get("url", "")
                    domain_name = extract_domain(url_result)
                    position = item.get("rank_group")
                    keyword_results.append({
                        "keyword": keyword,
                        "url": url_result,
                        "domain": domain_name,
                        "position": position,
                        "error": None
                    })
        
        return keyword_results
        
    except Exception as e:
        return [{
            "keyword": keyword,
            "url": None,
            "domain": None,
            "position": None,
            "error": str(e)
        }]

def run_extraction_parallel(keywords_list, max_workers=5):
    """Exécute l'extraction en parallèle avec ThreadPoolExecutor"""
    all_results = []
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Soumettre toutes les tâches
        future_to_keyword = {executor.submit(process_keyword, kw): kw for kw in keywords_list}
        
        completed = 0
        for future in as_completed(future_to_keyword):
            keyword = future_to_keyword[future]
            try:
                results = future.result()
                all_results.extend(results)
            except Exception as e:
                st.error(f"Erreur pour le mot-clé '{keyword}': {e}")
                all_results.append({
                    "keyword": keyword,
                    "url": None,
                    "domain": None,
                    "position": None,
                    "error": str(e)
                })
            
            completed += 1
            progress = completed / len(keywords_list)
            progress_bar.progress(progress)
            status_text.text(f"Traité: {completed}/{len(keywords_list)} mots-clés")
            
            # Petite pause pour éviter de surcharger l'API
            time.sleep(0.1)
    
    status_text.empty()
    return all_results

# --- Interface principale ---
if st.button("🚀 Lancer l'extraction") and keywords:
    if not api_user or api_user == "youradress":
        st.warning("Veuillez renseigner vos identifiants API.")
    else:
        st.info(f"Extraction en cours pour {len(keywords)} mot(s)-clé(s)...")
        
        results = run_extraction_parallel(keywords, max_workers)
        output_df = pd.DataFrame(results)
        
        # Filtrer les erreurs pour l'affichage principal
        success_df = output_df[output_df['error'].isna()].drop('error', axis=1)
        error_df = output_df[output_df['error'].notna()]
        
        st.success(f"Extraction terminée. {len(success_df)} résultats trouvés.")
        
        if len(error_df) > 0:
            st.warning(f"{len(error_df)} erreurs rencontrées.")
            with st.expander("Voir les erreurs"):
                st.dataframe(error_df[['keyword', 'error']])
        
        if len(success_df) > 0:
            st.dataframe(success_df)
            
            # Export CSV
            csv = success_df.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="📂 Télécharger les résultats en CSV",
                data=csv,
                file_name='serp_results.csv',
                mime='text/csv'
            )
