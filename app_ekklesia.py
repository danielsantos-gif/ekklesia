"""
╔══════════════════════════════════════════════════════╗
║           EKKLESIA — Monitor de Narrativas           ║
║   Brandwatch · SuperMetrics · Stilingue · Apify      ║
║   Nexus — Pesquisa e Inteligência de Dados           ║
╚══════════════════════════════════════════════════════╝
"""

import io, re, unicodedata, warnings
from pathlib import Path
from datetime import timedelta
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse

import numpy as np
import pandas as pd
import streamlit as st
import plotly.graph_objects as go
import plotly.express as px

warnings.filterwarnings("ignore")

# ════════════════════════════════════════════════════════════════
# CONSTANTES VISUAIS — IDENTIDADE NEXUS/EKKLESIA
# ════════════════════════════════════════════════════════════════
COR_LARANJA   = "#E8770A"
COR_LARANJA2  = "#F59B2B"
COR_FUNDO     = "#0D0D0D"
COR_CARD      = "#1A1A1A"
COR_BORDA     = "#2A2A2A"
COR_TEXTO     = "#F0F0F0"
COR_SUBTEXTO  = "#A0A0A0"

# Paleta com as novas cores para as redes sociais
CORES_REDES = {
    "instagram": "#F689E6",     # Rosa neon
    "facebook": "#0689D6",      # Azul metrô
    "tiktok": "#1A1718",        # Preto profundo
    "x": "#7CA6BE",             # Azul acinzentado
    "youtube": "#6E160D",       # Vermelho tijolo escuro
    "linkedin": "#067755",      # Verde linha
    "reddit": "#F25905",        # Laranja vibrante
    "threads": "#EEE9EA",       # Branco suave
    "bluesky": "#C377AE",       # Rosa pastel
    "desconhecido": "#924027"   # Marrom tijolo
}
# ════════════════════════════════════════════════════════════════
# HELPERS GERAIS
# ════════════════════════════════════════════════════════════════
def fig_to_png(fig, width=1400, height=700):
    try:
        return fig.to_image(format="png", width=width, height=height, scale=2)
    except Exception:
        return None

def df_to_excel_bytes(df):
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        df.to_excel(w, index=False, sheet_name="dados")
    return buf.getvalue()

def dl_row(cols_cfg):
    cols = st.columns(len(cols_cfg))
    for col, (label, data, filename, mime) in zip(cols, cols_cfg):
        with col:
            if data:
                st.download_button(label, data=data, file_name=filename, mime=mime,
                                   use_container_width=True)
            else:
                st.button(label, disabled=True, use_container_width=True,
                          help="Gere o gráfico primeiro")

def strip_accents(s):
    if pd.isna(s): return ""
    return "".join(c for c in unicodedata.normalize("NFKD", str(s)) if not unicodedata.combining(c))

def to_float(s):
    s = pd.Series([s]) if not isinstance(s, pd.Series) else s
    s = s.astype(str).str.replace(".", "", regex=False).str.replace(",", ".", regex=False)
    s = s.str.replace(r"[^0-9.\-]", "", regex=True).replace({"": np.nan, ".": np.nan, "-": np.nan})
    return pd.to_numeric(s, errors="coerce")

def norm_canal(val):
    if pd.isna(val): return pd.NA
    v = strip_accents(str(val)).lower().strip()
    v = re.sub(r"https?://|www\.", "", v).split("/")[0].split(".")[0]
    MAP = {
        "instagram_public": "instagram", "instagram": "instagram", "ig": "instagram",
        "facebook": "facebook", "fb": "facebook",
        "tiktok": "tiktok",
        "twitter": "x", "x": "x",
        "youtube": "youtube", "yt": "youtube",
        "linkedin": "linkedin",
        "reddit": "reddit",
        "threads": "threads",
        "bluesky": "bluesky",
    }
    for k, v2 in MAP.items():
        if k in v: return v2
    return v if v else pd.NA

def norm_sentimento(val):
    if pd.isna(val): return pd.NA
    s = strip_accents(str(val)).lower().strip()
    return {"positive":"positivo","positivo":"positivo","very positive":"muito positivo",
            "negative":"negativo","negativo":"negativo","very negative":"muito negativo",
            "neutral":"neutro","neutro":"neutro"}.get(s, s)

def canon_url(u):
    if pd.isna(u) or not str(u).strip(): return pd.NA
    s = str(u).strip()
    if not re.match(r"^https?://", s, re.I): s = "https://" + s
    try:
        p = urlparse(s)
        host = (p.netloc or "").lower()
        path = (p.path or "").rstrip("/") or "/"
        qs = [(k, v) for k, v in parse_qsl(p.query, keep_blank_values=True)
              if not (k.lower().startswith("utm_") or k.lower() in {"gclid","fbclid"})]
        return urlunparse(("", host, path, "", urlencode(qs, doseq=True), "")).lstrip("/")
    except Exception:
        return pd.NA

def sum_cols(df, cols):
    present = [c for c in cols if c in df.columns]
    if not present: return pd.Series(np.nan, index=df.index)
    arr = df[present].apply(lambda c: pd.to_numeric(c, errors="coerce"))
    return arr.sum(axis=1, skipna=True).where(~arr.isna().all(axis=1), np.nan)

def safe_date_filter(df, periodo):
    """Filtra por período tratando corretamente timezones mistos."""
    if not periodo or len(periodo) != 2:
        return df
    try:
        col = df["data"].copy()
        if hasattr(col.dtype, "tz") and col.dtype.tz is not None:
            col = col.dt.tz_localize(None)
        elif col.dtype == object:
            col = pd.to_datetime(col, errors="coerce")
        col_date = col.dt.date
        mask = (col_date >= periodo[0]) & (col_date <= periodo[1])
        return df[mask]
    except Exception:
        try:
            datas = pd.to_datetime(df["data"], errors="coerce", utc=True).dt.tz_localize(None)
            mask = (datas.dt.date >= periodo[0]) & (datas.dt.date <= periodo[1])
            return df[mask]
        except Exception:
            return df

FINAL_COLS = [
    "fonte","canal","data","link_publicacao","link_publicador",
    "nome_publicador","id_publicacao","tipo_midia","titulo","conteudo",
    "hashtags","sentimento","seguidores","curtidas","comentarios",
    "compartilhamentos","visualizacoes","outras_reacoes","interacoes",
    "pais","idioma","score_relevancia",
]

def to_schema(rows):
    df = pd.DataFrame(rows)
    for c in FINAL_COLS:
        if c not in df.columns: df[c] = pd.NA
    return df[FINAL_COLS]

# ════════════════════════════════════════════════════════════════
# PARSERS
# ════════════════════════════════════════════════════════════════
def parse_brandwatch(file_bytes):
    df = pd.read_excel(io.BytesIO(file_bytes), header=6, dtype=str, engine="openpyxl")
    df.columns = df.columns.str.strip()
    FOLLOWERS = ["Instagram Followers","X Followers","Youtube Subscriber Count",
                 "Subreddit Subscribers","Bluesky Followers"]
    rows = []
    for _, r in df.iterrows():
        seg = next((r.get(f) for f in FOLLOWERS
                    if pd.notna(r.get(f)) and str(r.get(f)).strip() not in ("","0","nan")), pd.NA)
        author = r.get("Author") if pd.notna(r.get("Author")) else r.get("Full Name")
        rows.append({
            "fonte": "brandwatch",
            "canal": norm_canal(str(r.get("Page Type","")) or str(r.get("Domain",""))),
            "data": r.get("Date"),
            "link_publicacao": r.get("Url"),
            "link_publicador": r.get("Original Url"),
            "nome_publicador": author,
            "id_publicacao": r.get("Resource Id"),
            "tipo_midia": r.get("Subtype") or r.get("Facebook Subtype"),
            "titulo": r.get("Title"),
            "conteudo": r.get("Full Text"),
            "hashtags": r.get("Hashtags"),
            "sentimento": norm_sentimento(r.get("Sentiment")),
            "seguidores": seg,
            "curtidas": pd.NA, "comentarios": pd.NA,
            "compartilhamentos": pd.NA, "visualizacoes": pd.NA, "outras_reacoes": pd.NA,
            "interacoes": r.get("Engagement Score"),
            "pais": r.get("Country"),
            "idioma": r.get("Language"),
            "score_relevancia": pd.NA,
        })
    return to_schema(rows)

def parse_supermetrics(file_bytes):
    xl = pd.ExcelFile(io.BytesIO(file_bytes), engine="openpyxl")

    def load_followers(sheet, user_col, date_col, fol_col):
        if sheet not in xl.sheet_names: return pd.DataFrame()
        df = xl.parse(sheet, dtype=str)
        df.columns = df.columns.str.strip()
        if not all(c in df.columns for c in [user_col, date_col, fol_col]): return pd.DataFrame()
        df = df.rename(columns={user_col:"_u", date_col:"_d", fol_col:"_f"})
        df["_d"] = pd.to_datetime(df["_d"], errors="coerce", dayfirst=True)
        df["_u"] = df["_u"].astype(str).str.lower().str.strip()
        df["_f"] = pd.to_numeric(df["_f"], errors="coerce")
        return df[["_u","_d","_f"]].dropna(subset=["_u","_d"])

    fb_seg = load_followers("facebook seguidores", "Username", "Date", "Page followers")
    ig_seg = load_followers("instagram seguidores", "Username", "Date", "Profile followers")

    def get_followers(username, date, seg_df):
        if seg_df.empty or pd.isna(username) or pd.isna(date): return pd.NA
        u = str(username).lower().strip()
        try:
            d = pd.to_datetime(date, errors="coerce", dayfirst=True)
            if pd.isna(d): return pd.NA
        except Exception: return pd.NA
        sub = seg_df[seg_df["_u"] == u].copy()
        if sub.empty: return pd.NA
        sub = sub.sort_values("_d")
        idx = (sub["_d"] - d).abs().idxmin()
        return sub.loc[idx, "_f"]

    rows = []
    REAC_FB = ["Reactions: Love","Reactions: Wow","Reactions: Haha",
               "Reactions: Sad","Reactions: Angry","Reactions: Thankful","Reactions: Pride"]

    if "facebook" in xl.sheet_names:
        df_fb = xl.parse("facebook", dtype=str)
        df_fb.columns = df_fb.columns.str.strip()
        for _, r in df_fb.iterrows():
            cur = to_float(r.get("Likes","")).iloc[0]
            rea = sum_cols(pd.DataFrame([r]), REAC_FB).iloc[0]
            com = to_float(r.get("Comments","")).iloc[0]
            sha = to_float(r.get("Post shares","")).iloc[0]
            vals = [x for x in [cur, rea, com, sha] if pd.notna(x)]
            rows.append({
                "fonte":"supermetrics","canal":"facebook",
                "data": r.get("Created date"),
                "link_publicacao": r.get("Link to post"),
                "link_publicador": r.get("Link to page"),
                "nome_publicador": r.get("Name (Profile)"),
                "id_publicacao": r.get("Post ID"),
                "tipo_midia": r.get("Post type"),
                "titulo": r.get("Caption"),
                "conteudo": r.get("Message"),
                "hashtags": pd.NA, "sentimento": pd.NA,
                "seguidores": get_followers(r.get("Name (Profile)"), r.get("Created date"), fb_seg),
                "curtidas": cur, "comentarios": com, "compartilhamentos": sha,
                "visualizacoes": pd.NA, "outras_reacoes": rea,
                "interacoes": sum(vals) if vals else np.nan,
                "pais": pd.NA, "idioma": pd.NA, "score_relevancia": pd.NA,
            })

    if "instagram" in xl.sheet_names:
        df_ig = xl.parse("instagram", dtype=str)
        df_ig.columns = df_ig.columns.str.strip()
        for _, r in df_ig.iterrows():
            cur = to_float(r.get("Likes","")).iloc[0]
            com = to_float(r.get("Comments","")).iloc[0]
            vis = to_float(r.get("Reels views","")).iloc[0]
            vals = [x for x in [cur, com, vis] if pd.notna(x)]
            rows.append({
                "fonte":"supermetrics","canal":"instagram",
                "data": r.get("Date"),
                "link_publicacao": r.get("Link to post"),
                "link_publicador": pd.NA,
                "nome_publicador": r.get("Name") or r.get("Username"),
                "id_publicacao": r.get("Post ID"),
                "tipo_midia": r.get("Post type"),
                "titulo": pd.NA,
                "conteudo": r.get("Post caption"),
                "hashtags": pd.NA, "sentimento": pd.NA,
                "seguidores": get_followers(r.get("Username"), r.get("Date"), ig_seg),
                "curtidas": cur, "comentarios": com, "compartilhamentos": pd.NA,
                "visualizacoes": vis, "outras_reacoes": pd.NA,
                "interacoes": sum(vals) if vals else np.nan,
                "pais": pd.NA, "idioma": pd.NA, "score_relevancia": pd.NA,
            })

    return to_schema(rows)

def parse_stilingue(file_bytes):
    df = pd.read_excel(io.BytesIO(file_bytes), dtype=str, engine="openpyxl")
    df.columns = df.columns.str.strip()
    REAC = ["Amei","Haha","Uau","Triste","Raiva","Outras Reações"]
    rows = []
    for _, r in df.iterrows():
        cur  = to_float(r.get("Curtidas/Gostei","")).iloc[0]
        tot  = to_float(r.get("Total de Reações","")).iloc[0]
        com  = to_float(r.get("Comentários","")).iloc[0]
        sha  = to_float(r.get("Compartilhamentos","")).iloc[0]
        outr = sum_cols(pd.DataFrame([r]), REAC).iloc[0]
        base = tot if pd.notna(tot) else cur
        vals = [x for x in [base, com, sha] if pd.notna(x)]
        rows.append({
            "fonte":"stilingue",
            "canal": norm_canal(r.get("Canal") or r.get("Mídia")),
            "data": r.get("Data"),
            "link_publicacao": r.get("Link"),
            "link_publicador": r.get("Autor Link"),
            "nome_publicador": r.get("Autor Nome"),
            "id_publicacao": r.get("ID"),
            "tipo_midia": r.get("Canal"),
            "titulo": r.get("Título"),
            "conteudo": r.get("Conteúdo"),
            "hashtags": pd.NA,
            "sentimento": norm_sentimento(r.get("Polaridade")),
            "seguidores": r.get("Seguidores/Inscritos"),
            "curtidas": cur, "comentarios": com, "compartilhamentos": sha,
            "visualizacoes": pd.NA, "outras_reacoes": outr,
            "interacoes": sum(vals) if vals else np.nan,
            "pais": pd.NA, "idioma": pd.NA, "score_relevancia": pd.NA,
        })
    return to_schema(rows)

def parse_apify(file_bytes):
    COLS_NEEDED = [
        "id","createTimeISO","webVideoUrl","text","textLanguage",
        "authorMeta/nickName","authorMeta/profileUrl","authorMeta/fans",
        "diggCount","commentCount","shareCount","playCount","collectCount",
        "locationMeta/countryCode",
        *[f"hashtags/{i}/name" for i in range(15)],
    ]
    df = pd.read_excel(
        io.BytesIO(file_bytes), dtype=str, engine="openpyxl",
        usecols=lambda c: c in set(COLS_NEEDED)
    )
    df.columns = df.columns.str.strip()
    rows = []
    for _, r in df.iterrows():
        cur  = to_float(r.get("diggCount","")).iloc[0]
        com  = to_float(r.get("commentCount","")).iloc[0]
        sha  = to_float(r.get("shareCount","")).iloc[0]
        vis  = to_float(r.get("playCount","")).iloc[0]
        sav  = to_float(r.get("collectCount","")).iloc[0]
        vals = [x for x in [cur, com, sha, vis, sav] if pd.notna(x)]
        htags = ", ".join(
            str(r.get(f"hashtags/{i}/name")).strip().lower()
            for i in range(15)
            if pd.notna(r.get(f"hashtags/{i}/name")) and str(r.get(f"hashtags/{i}/name")).strip()
        ) or pd.NA
        rows.append({
            "fonte":"apify_tiktok","canal":"tiktok",
            "data": r.get("createTimeISO"),
            "link_publicacao": r.get("webVideoUrl"),
            "link_publicador": r.get("authorMeta/profileUrl"),
            "nome_publicador": r.get("authorMeta/nickName"),
            "id_publicacao": r.get("id"),
            "tipo_midia": "video",
            "titulo": pd.NA,
            "conteudo": r.get("text"),
            "hashtags": htags,
            "sentimento": pd.NA,
            "seguidores": r.get("authorMeta/fans"),
            "curtidas": cur, "comentarios": com, "compartilhamentos": sha,
            "visualizacoes": vis, "outras_reacoes": sav,
            "interacoes": sum(vals) if vals else np.nan,
            "pais": r.get("locationMeta/countryCode"),
            "idioma": r.get("textLanguage"),
            "score_relevancia": pd.NA,
        })
    return to_schema(rows)

def parse_ir2(file_bytes):
    xl = pd.ExcelFile(io.BytesIO(file_bytes), engine="openpyxl")

    aba_global = None
    for sheet in xl.sheet_names:
        if sheet.strip().lower() == "global":
            aba_global = sheet
            break

    if aba_global is None:
        aba_global = xl.sheet_names[0]

    df = xl.parse(aba_global, dtype=str)
    df.columns = df.columns.str.strip()

    col_autor = df.columns[0]
    for c in df.columns:
        if "autor" in c.lower():
            col_autor = c
            break

    col_score = None
    for c in df.columns:
        if "global" in c.lower() and "ranking" in c.lower():
            col_score = c
            break
    if col_score is None:
        for c in reversed(df.columns):
            if c != col_autor:
                vals = pd.to_numeric(df[c], errors="coerce").dropna()
                if not vals.empty:
                    col_score = c
                    break

    if col_score is None:
        return {}

    scores = {}
    for _, row in df.iterrows():
        autor = row.get(col_autor)
        score = row.get(col_score)
        if pd.isna(autor) or str(autor).strip() == "":
            continue
        score_num = pd.to_numeric(str(score).replace(",", "."), errors="coerce")
        if pd.notna(score_num):
            key = str(autor).strip().lower()
            scores[key] = round(float(score_num), 4)

    return scores

def parse_ir2_full(file_bytes):
    xl = pd.ExcelFile(io.BytesIO(file_bytes), engine="openpyxl")
    aba_global = None
    for sheet in xl.sheet_names:
        if sheet.strip().lower() == "global":
            aba_global = sheet
            break
    if aba_global is None:
        aba_global = xl.sheet_names[0]

    df = xl.parse(aba_global, dtype=str)
    df.columns = df.columns.str.strip()
    return df

# ════════════════════════════════════════════════════════════════
# CORPUS IRAMUTEQ
# ════════════════════════════════════════════════════════════════
@st.cache_resource(show_spinner="Carregando recursos de linguagem…")
def load_stopwords_pt():
    try:
        import nltk
        try:
            return set(nltk.corpus.stopwords.words("portuguese"))
        except LookupError:
            nltk.download("stopwords", quiet=True)
            return set(nltk.corpus.stopwords.words("portuguese"))
    except Exception:
        return set([
            "de","a","o","que","e","do","da","em","um","para","com","uma","os","no",
            "se","na","por","mais","as","dos","como","mas","ao","ele","das","à","seu",
            "sua","ou","quando","muito","nos","já","eu","também","só","pelo","pela",
            "até","isso","ela","entre","depois","sem","mesmo","aos","ter","seus","suas",
            "nem","há","foi","ser","esta","foi","não","está","são","este","esse","essa",
            "isso","aqui","meu","minha","nos","lhe","esse","neste","nessa","nesse",
        ])

def gerar_corpus(df_filtrado):
    import re as _re
    stopwords_pt = load_stopwords_pt()

    def limpa(t):
        t = str(t).replace("\n", " ").replace("\r", " ")
        t = _re.sub(r"https?://\S+", " ", t)
        t = _re.sub(r"[\"\'\\$%\*#@]", " ", t)
        t = _re.sub(r"\s+", " ", t)
        return t.strip()

    def tokeniza(texto):
        texto = limpa(texto).lower()
        texto = strip_accents(texto)
        tokens = _re.findall(r"\b[a-z]{3,}\b", texto)
        return [t for t in tokens if t not in stopwords_pt]

    textos = (df_filtrado["titulo"].fillna("") + " " + df_filtrado["conteudo"].fillna("")).tolist()
    textos = [t for t in textos if str(t).strip() and str(t).strip() != " "]
    if not textos:
        return ""

    from collections import Counter
    n = len(textos)
    n_termos = 150 if n <= 1000 else 300 if n <= 5000 else 400

    tokens_por_doc = [tokeniza(t) for t in textos]
    freq = Counter(p for sub in tokens_por_doc for p in sub)
    top = set(p for p, _ in freq.most_common(n_termos))

    linhas = []
    for i, tokens in enumerate(tokens_por_doc):
        filtrados = [p for p in tokens if p in top]
        if filtrados:
            linhas.append(f"**** *suj_{str(i+1).zfill(4)}")
            linhas.append(" ".join(filtrados))
    return "\n".join(linhas)

# ════════════════════════════════════════════════════════════════
# GRAFO INTERATIVO
# ════════════════════════════════════════════════════════════════
def gerar_grafo(df_filtrado, dias_bloco=7, max_termos=12):
    import networkx as nx
    from sklearn.feature_extraction.text import TfidfVectorizer

    stopwords_pt = load_stopwords_pt()

    df_filtrado = df_filtrado.copy()
    df_filtrado["_texto"] = (df_filtrado["titulo"].fillna("") + " " + df_filtrado["conteudo"].fillna("")).str.lower()
    df_filtrado["_data"]  = pd.to_datetime(df_filtrado["data"], errors="coerce").dt.date
    df_filtrado = df_filtrado.dropna(subset=["_data","_texto"])
    if df_filtrado.empty: return None

    data_min = pd.to_datetime(df_filtrado["_data"].min())
    df_filtrado["_bloco"] = df_filtrado["_data"].apply(
        lambda d: data_min + timedelta(days=((pd.to_datetime(d) - data_min).days // dias_bloco) * dias_bloco)
    )

    palette = [COR_LARANJA, COR_LARANJA2, "#C45C08", "#FF9F45", "#F5C842",
               "#E87D2B", "#FF6B35", "#FFAA33", "#CC6600", "#FFA500"]
    frames, slider_steps = [], []

    for bloco in sorted(df_filtrado["_bloco"].unique()):
        docs = df_filtrado[df_filtrado["_bloco"] == bloco]["_texto"]
        if len(docs) < 2: continue
        try:
            vec = TfidfVectorizer(
                max_features=max_termos, ngram_range=(1,2),
                stop_words=list(stopwords_pt)
            )
            X = vec.fit_transform(docs)
        except Exception:
            continue

        adj = X.T * X
        adj.setdiag(0)
        G = nx.Graph()
        words = vec.get_feature_names_out()
        for i, j in zip(*adj.nonzero()):
            if i < j and adj[i, j] > 0.05:
                G.add_edge(words[i], words[j], weight=float(adj[i, j]))
        if len(G.nodes()) < 2: continue

        cent = nx.degree_centrality(G)
        max_c = max(cent.values()) or 1
        pos = nx.spring_layout(G, k=2.5/np.sqrt(len(G.nodes())), iterations=50, seed=42)

        nodes = list(G.nodes())
        for _ in range(50):
            moved = False
            for i in range(len(nodes)):
                for j in range(i+1, len(nodes)):
                    n1, n2 = nodes[i], nodes[j]
                    dx = pos[n1][0]-pos[n2][0]; dy = pos[n1][1]-pos[n2][1]
                    dist = max(abs(dx), abs(dy))
                    min_d = (len(n1)+len(n2)) * 0.018 + 0.1
                    if dist < min_d:
                        push = (min_d - dist) * 0.5
                        sx = np.sign(dx) or 1; sy = np.sign(dy) or 1
                        pos[n1][0] += push*sx; pos[n2][0] -= push*sx
                        pos[n1][1] += push*sy; pos[n2][1] -= push*sy
                        moved = True
            if not moved:
                break

        communities = list(nx.community.greedy_modularity_communities(G))
        color_map = {n: palette[idx % len(palette)] for idx, c in enumerate(communities) for n in c}

        ex, ey = [], []
        for e in G.edges():
            x0,y0=pos[e[0]]; x1,y1=pos[e[1]]
            ex += [x0,x1,None]; ey += [y0,y1,None]

        sizes = [14 + (cent[n]/max_c)**1.8 * 40 for n in G.nodes()]
        top3  = sorted(cent, key=cent.get, reverse=True)[:3]
        labels = [f"<b>{n.capitalize()}</b>" if n in top3 else n.capitalize() for n in G.nodes()]

        x_vals = [pos[n][0] for n in G.nodes()]
        y_vals = [pos[n][1] for n in G.nodes()]
        pad_x = (max(x_vals)-min(x_vals))*0.3 or 0.5
        pad_y = (max(y_vals)-min(y_vals))*0.3 or 0.5

        label = bloco.strftime("%d/%m/%Y") if hasattr(bloco, "strftime") else str(bloco)
        frame = go.Frame(
            data=[
                go.Scatter(x=ex, y=ey, mode="lines",
                           line=dict(width=0.8, color="rgba(232,119,10,0.2)"), hoverinfo="none"),
                go.Scatter(
                    x=[pos[n][0] for n in G.nodes()],
                    y=[pos[n][1] for n in G.nodes()],
                    mode="text", text=labels,
                    textfont=dict(size=sizes, color=[color_map.get(n, COR_LARANJA) for n in G.nodes()]),
                    hovertext=[f"{n} (centralidade: {cent[n]:.2f})" for n in G.nodes()],
                    hoverinfo="text",
                )
            ],
            name=label,
            layout=go.Layout(
                xaxis=dict(range=[min(x_vals)-pad_x, max(x_vals)+pad_x]),
                yaxis=dict(range=[min(y_vals)-pad_y, max(y_vals)+pad_y]),
            )
        )
        frames.append(frame)
        slider_steps.append({
            "args": [[label], {"frame": {"duration": 800, "redraw": True}, "mode": "immediate"}],
            "label": label, "method": "animate"
        })

    if not frames: return None

    fig = go.Figure(
        data=frames[0].data,
        layout=go.Layout(
            plot_bgcolor="#111111",
            paper_bgcolor="#111111",
            xaxis=dict(visible=False, autorange=False),
            yaxis=dict(visible=False, autorange=False),
            margin=dict(l=20, r=20, t=40, b=100),
            height=560,
            font=dict(color=COR_TEXTO),
            updatemenus=[{
                "type": "buttons", "x": 0.05, "y": -0.08,
                "bgcolor": "#1A1A1A",
                "font": {"color": COR_LARANJA},
                "buttons": [
                    {"label": "▶ Play", "method": "animate",
                     "args": [None, {"frame": {"duration": 1500, "redraw": True},
                                     "transition": {"duration": 600}}]},
                    {"label": "⏸ Pausar", "method": "animate",
                     "args": [[None], {"frame": {"duration": 0}, "mode": "immediate"}]}
                ]
            }],
            sliders=[{
                "active": 0, "y": -0.04, "len": 0.9, "x": 0.05,
                "bgcolor": "#1A1A1A",
                "currentvalue": {"prefix": "Período: ", "font": {"size": 14, "color": COR_LARANJA}},
                "steps": slider_steps
            }],
        ),
        frames=frames,
    )
    return fig

# ════════════════════════════════════════════════════════════════
# UNIFICAÇÃO E DEDUPLICAÇÃO
# ════════════════════════════════════════════════════════════════
def unificar(frames, ir2_scores):
    df = pd.concat(frames, ignore_index=True)

    df["data"] = pd.to_datetime(df["data"], errors="coerce", utc=False)
    try:
        if hasattr(df["data"].dtype, "tz") and df["data"].dtype.tz is not None:
            df["data"] = df["data"].dt.tz_localize(None)
    except Exception:
        try:
            df["data"] = pd.to_datetime(df["data"], errors="coerce", utc=True).dt.tz_localize(None)
        except Exception:
            pass

    NUM = ["seguidores","curtidas","comentarios","compartilhamentos",
           "visualizacoes","outras_reacoes","interacoes","score_relevancia"]
    for c in NUM:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df["canal"] = df["canal"].map(lambda x: norm_canal(x) if pd.notna(x) else pd.NA)

    if ir2_scores:
        def get_score(nome):
            if pd.isna(nome): return pd.NA
            key = str(nome).lower().strip()
            if key in ir2_scores: return ir2_scores[key]
            for k, v in ir2_scores.items():
                if k in key or key in k: return v
            return pd.NA
        df["score_relevancia"] = df["nome_publicador"].map(get_score)

    ICOLS = ["curtidas","comentarios","compartilhamentos","outras_reacoes"]
    mask = df["interacoes"].isna()
    df.loc[mask, "interacoes"] = (
        df.loc[mask, ICOLS].sum(axis=1, skipna=True)
        .where(~df.loc[mask, ICOLS].isna().all(axis=1), np.nan)
    )

    SCORE_COLS = ["titulo","conteudo","sentimento","seguidores","curtidas",
                  "comentarios","compartilhamentos","visualizacoes","hashtags","pais"]
    df["__score"] = df[SCORE_COLS].notna().sum(axis=1)
    df["__url"]   = df["link_publicacao"].map(canon_url)
    df["__nome"]  = df["nome_publicador"].astype(str).str.lower().str.strip()
    df["__canal"] = df["canal"].astype(str).str.lower().str.strip()
    df["__hora"]  = df["data"].dt.floor("h")
    df["__txt"]   = df["conteudo"].astype(str).str[:80].str.lower().str.strip()

    df = df.sort_values("__score", ascending=False)
    mask_url = df["__url"].notna()
    df = pd.concat([
        df[mask_url].drop_duplicates(subset=["__url"], keep="first"),
        df[~mask_url].drop_duplicates(subset=["__canal","__nome","__hora","__txt"], keep="first"),
    ], ignore_index=True)

    df = df.drop(columns=[c for c in df.columns if c.startswith("__")])
    for c in FINAL_COLS:
        if c not in df.columns: df[c] = pd.NA
    return df[FINAL_COLS].reset_index(drop=True)

# ════════════════════════════════════════════════════════════════
# ANÁLISE DE IA
# ════════════════════════════════════════════════════════════════
def gerar_analise_ia(df_resumo: dict) -> str:
    try:
        import anthropic
        client = anthropic.Anthropic(api_key=st.secrets["ANTHROPIC_API_KEY"])

        prompt = f"""Você é um analista especialista em monitoramento de redes sociais e narrativas digitais.
Analise os dados abaixo e escreva EXATAMENTE 2 parágrafos em português:
- Parágrafo 1: Contexto geral da base monitorada (volume, redes, período, publicadores)
- Parágrafo 2: Destaques, padrões e insights relevantes

Dados da base monitorada:
{df_resumo}

Seja direto, analítico e use linguagem profissional. Não use bullet points, apenas parágrafos corridos."""

        message = client.messages.create(
            model="claude-opus-4-5",
            max_tokens=600,
            messages=[{"role": "user", "content": prompt}]
        )
        return message.content[0].text
    except ImportError:
        return "❌ Biblioteca `anthropic` não instalada. Adicione `anthropic` ao requirements.txt."
    except KeyError:
        return "❌ Chave de API não configurada. Adicione `ANTHROPIC_API_KEY` nos Secrets do Streamlit Cloud."
    except Exception as e:
        return f"❌ Erro ao chamar a API: {str(e)}"

# ════════════════════════════════════════════════════════════════
# LAYOUT STREAMLIT — CONFIGURAÇÃO E ESTILO
# ════════════════════════════════════════════════════════════════
st.set_page_config(
    page_title="Ekklesia — Monitor de Narrativas",
    page_icon="🏛️",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(f"""
<style>
/* ── Reset e base escura ── */
html, body, [data-testid="stApp"] {{
    background-color: {COR_FUNDO} !important;
    color: {COR_TEXTO} !important;
}}

/* ── Sidebar ── */
[data-testid="stSidebar"] {{
    background: #111111 !important;
    border-right: 1px solid {COR_BORDA};
}}
[data-testid="stSidebar"] * {{
    color: {COR_TEXTO} !important;
}}
[data-testid="stSidebar"] .stButton > button {{
    background: {COR_LARANJA} !important;
    color: #000 !important;
    font-weight: 700 !important;
    border: none !important;
    border-radius: 8px !important;
}}
[data-testid="stSidebar"] .stButton > button:hover {{
    background: {COR_LARANJA2} !important;
}}

/* ── Cards / métricas ── */
[data-testid="metric-container"] {{
    background: {COR_CARD} !important;
    border: 1px solid {COR_BORDA} !important;
    border-left: 4px solid {COR_LARANJA} !important;
    border-radius: 10px !important;
    padding: 16px 20px !important;
}}
[data-testid="stMetricValue"] {{
    color: {COR_LARANJA} !important;
    font-size: 2rem !important;
    font-weight: 800 !important;
}}
[data-testid="stMetricLabel"] {{
    color: {COR_SUBTEXTO} !important;
    font-size: 0.85rem !important;
    text-transform: uppercase !important;
    letter-spacing: 0.05em !important;
}}

/* ── Tabs ── */
[data-testid="stTabs"] [role="tablist"] {{
    background: #111111 !important;
    border-bottom: 2px solid {COR_BORDA} !important;
    gap: 4px;
}}
[data-testid="stTabs"] [role="tab"] {{
    color: {COR_SUBTEXTO} !important;
    border-radius: 8px 8px 0 0 !important;
    padding: 8px 18px !important;
    font-weight: 500;
}}
[data-testid="stTabs"] [role="tab"][aria-selected="true"] {{
    color: {COR_LARANJA} !important;
    border-bottom: 3px solid {COR_LARANJA} !important;
    font-weight: 700 !important;
}}

/* ── Botões primários ── */
.stButton > button[kind="primary"] {{
    background: {COR_LARANJA} !important;
    color: #000 !important;
    font-weight: 700 !important;
    border: none !important;
    border-radius: 8px !important;
}}
.stButton > button[kind="primary"]:hover {{
    background: {COR_LARANJA2} !important;
}}

/* ── Download buttons ── */
.stDownloadButton > button {{
    background: {COR_CARD} !important;
    color: {COR_LARANJA} !important;
    border: 1px solid {COR_LARANJA} !important;
    border-radius: 8px !important;
    font-weight: 600 !important;
}}
.stDownloadButton > button:hover {{
    background: {COR_LARANJA} !important;
    color: #000 !important;
}}

/* ── Labels dos Filtros (Select, MultiSelect, Date) ── */
[data-testid="stSelectbox"] label p,
[data-testid="stMultiSelect"] label p,
[data-testid="stDateInput"] label p {{
    font-size: 1.15rem !important;
    font-weight: 600 !important;
    color: {COR_LARANJA} !important;
}}

/* ── Tabelas / dataframes ── */
[data-testid="stDataFrame"] {{
    border: 1px solid {COR_BORDA} !important;
    border-radius: 10px !important;
}}

/* ── Inputs e selects ── */
[data-testid="stSelectbox"] > div,
[data-testid="stMultiSelect"] > div {{
    background: {COR_CARD} !important;
    border-color: {COR_BORDA} !important;
}}

/* ── Dividers ── */
hr {{
    border-color: {COR_BORDA} !important;
}}

/* ── Expanders ── */
[data-testid="stExpander"] {{
    background: {COR_CARD} !important;
    border: 1px solid {COR_BORDA} !important;
    border-radius: 8px !important;
}}

/* ── Info/warning boxes ── */
[data-testid="stAlert"] {{
    background: {COR_CARD} !important;
    border-left: 4px solid {COR_LARANJA} !important;
}}

/* ── Upload area ── */
[data-testid="stFileUploader"] {{
    background: {COR_CARD} !important;
    border: 1px dashed {COR_BORDA} !important;
    border-radius: 8px !important;
    padding: 8px !important;
}}

/* ── Scrollbar ── */
::-webkit-scrollbar {{ width: 6px; height: 6px; }}
::-webkit-scrollbar-track {{ background: {COR_FUNDO}; }}
::-webkit-scrollbar-thumb {{ background: {COR_LARANJA}; border-radius: 3px; }}

/* ── Título principal ── */
h1 {{ color: {COR_TEXTO} !important; font-size: 1.8rem !important; }}
h2, h3 {{ color: {COR_TEXTO} !important; }}
h4, h5, h6 {{ color: {COR_SUBTEXTO} !important; }}

/* ── Status box ── */
[data-testid="stStatus"] {{
    background: {COR_CARD} !important;
    border: 1px solid {COR_BORDA} !important;
}}

/* ── Análise IA box ── */
.analise-ia-box {{
    background: linear-gradient(135deg, #1A1A1A 0%, #1F1500 100%);
    border: 1px solid {COR_LARANJA};
    border-radius: 12px;
    padding: 20px 24px;
    margin: 16px 0;
    line-height: 1.8;
    color: {COR_TEXTO};
}}

/* ── Upload card ── */
.upload-card {{
    background: {COR_CARD};
    border: 1px solid {COR_BORDA};
    border-radius: 12px;
    padding: 20px;
    margin-bottom: 16px;
}}
</style>
""", unsafe_allow_html=True)

# ════════════════════════════════════════════════════════════════
# ESTADO DA SESSÃO
# ════════════════════════════════════════════════════════════════
if "df" not in st.session_state:
    st.session_state.df = None
if "ir2_scores" not in st.session_state:
    st.session_state.ir2_scores = {}
if "ir2_df_full" not in st.session_state:
    st.session_state.ir2_df_full = None
if "etapa" not in st.session_state:
    st.session_state.etapa = "upload"
if "analise_ia_texto" not in st.session_state:
    st.session_state.analise_ia_texto = None
if "fig_grafo" not in st.session_state:
    st.session_state.fig_grafo = None
if "corpus_txt" not in st.session_state:
    st.session_state.corpus_txt = None

# ════════════════════════════════════════════════════════════════
# SIDEBAR
# ════════════════════════════════════════════════════════════════
with st.sidebar:
    # Logo Ekklesia (usando a imagem enviada)
    st.image("ekklesia_logo_4k.png", use_container_width=True)
    st.markdown(f"""
    <div style="text-align:center; margin-bottom: 16px;">
        <div style="font-size:0.7rem; color:{COR_SUBTEXTO}; letter-spacing:0.15em; margin-top:-4px;">
            MONITOR DE NARRATIVAS
        </div>
    </div>
    """, unsafe_allow_html=True)

    st.divider()

    # Navegação
    if st.session_state.df is not None:
        col_nav1, col_nav2 = st.columns(2)
        with col_nav1:
            if st.button("📁 Upload", use_container_width=True,
                         type="primary" if st.session_state.etapa == "upload" else "secondary"):
                st.session_state.etapa = "upload"
                st.rerun()
        with col_nav2:
            if st.button("📊 Dashboard", use_container_width=True,
                         type="primary" if st.session_state.etapa == "dashboard" else "secondary"):
                st.session_state.etapa = "dashboard"
                st.rerun()
        st.divider()

    # Status da base
    if st.session_state.df is not None:
        n = len(st.session_state.df)
        st.markdown(f"""
        <div style="background:{COR_CARD}; border:1px solid {COR_BORDA};
             border-left:4px solid {COR_LARANJA}; border-radius:8px;
             padding:10px 14px; margin-bottom:16px;">
            <div style="color:{COR_LARANJA}; font-weight:700; font-size:1.1rem;">{n:,}</div>
            <div style="color:{COR_SUBTEXTO}; font-size:0.75rem;">publicações carregadas</div>
        </div>
        """, unsafe_allow_html=True)

    # Logo Nexus no rodapé
    st.markdown("<br><br>", unsafe_allow_html=True)
    st.markdown(f"""
    <div style="text-align:center; padding-top:8px; border-top:1px solid {COR_BORDA}; margin-top:auto;">
        <span style="font-size:1.1rem; font-weight:800; color:{COR_TEXTO};">nexus.</span>
        <div style="font-size:0.6rem; color:{COR_SUBTEXTO}; letter-spacing:0.1em;">
            PESQUISA E INTELIGÊNCIA DE DADOS
        </div>
    </div>
    """, unsafe_allow_html=True)

# ════════════════════════════════════════════════════════════════
# ETAPA 1 — UPLOAD
# ════════════════════════════════════════════════════════════════
# ════════════════════════════════════════════════════════════════
# ETAPA 1 — UPLOAD
# ════════════════════════════════════════════════════════════════
if st.session_state.etapa == "upload":
    st.markdown(f"""
    <div style="margin-bottom: 1rem;">
        <h1 style="margin-bottom:4px;">🏛️ Ekklesia</h1>
        <p style="color:{COR_SUBTEXTO}; font-size:1rem; margin:0;">
            Faça upload das bases de dados para iniciar a análise.
        </p>
    </div>
    """, unsafe_allow_html=True)

    # ── INSTRUÇÕES HORIZONTAIS NO TOPO ──
    st.markdown(f"""
    <div style="background:{COR_CARD}; border:1px solid {COR_BORDA}; border-left:4px solid {COR_LARANJA}; border-radius:10px; padding:20px 24px; margin-bottom:2.5rem;">
        <h4 style="margin-top:0; color:{COR_TEXTO}; font-size: 1.1rem; margin-bottom: 16px;">ℹ️ Como funciona</h4>
        <div style="display: flex; flex-wrap: wrap; gap: 24px; color:{COR_SUBTEXTO}; font-size: 0.9rem; line-height: 1.5;">
            <div style="flex: 1; min-width: 200px;">
                <b style="color:{COR_TEXTO}">1. Upload</b><br>
                Envie os arquivos de uma ou mais plataformas abaixo. <span style="color:{COR_LARANJA}">Você pode combinar fontes!</span>
            </div>
            <div style="flex: 1; min-width: 200px;">
                <b style="color:{COR_TEXTO}">2. Processamento</b><br>
                A unificação, limpeza e deduplicação das bases são feitas automaticamente.
            </div>
            <div style="flex: 1; min-width: 200px;">
                <b style="color:{COR_TEXTO}">3. Dashboard</b><br>
                Navegue para a próxima etapa e explore os dados com filtros e gráficos interativos.
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown(f"### 📁 Upload das bases")
    st.markdown("<br>", unsafe_allow_html=True)

    # ── GRADE SIMÉTRICA DE UPLOADS ──
    c1, c2 = st.columns(2, gap="large")
    
    with c1:
        st.markdown(f"<div class='upload-card'><b style='color:{COR_LARANJA}'>📡 Brandwatch</b><br><small style='color:{COR_SUBTEXTO}'>*Uso_Geral*.xlsx</small></div>", unsafe_allow_html=True)
        f_bw = st.file_uploader("Brandwatch", type=["xlsx","csv"], key="bw", label_visibility="collapsed")
        
        st.markdown("<br>", unsafe_allow_html=True) # Respiro

        st.markdown(f"<div class='upload-card'><b style='color:{COR_LARANJA}'>🏆 IR² (ranking)</b><br><small style='color:{COR_SUBTEXTO}'>ranking-compilado*.xlsx — aba 'global'</small></div>", unsafe_allow_html=True)
        f_ir2 = st.file_uploader("IR²", type=["xlsx","csv"], key="ir2", label_visibility="collapsed")

    with c2:
        st.markdown(f"<div class='upload-card'><b style='color:{COR_LARANJA}'>📈 SuperMetrics</b><br><small style='color:{COR_SUBTEXTO}'>*Energisa*.xlsx</small></div>", unsafe_allow_html=True)
        f_sm = st.file_uploader("SuperMetrics", type=["xlsx","csv"], key="sm", label_visibility="collapsed")
        
        st.markdown("<br>", unsafe_allow_html=True) # Respiro

        st.markdown(f"<div class='upload-card'><b style='color:{COR_LARANJA}'>📊 Stilingue</b><br><small style='color:{COR_SUBTEXTO}'>RelatorioExpress*.xlsx</small></div>", unsafe_allow_html=True)
        f_st = st.file_uploader("Stilingue", type=["xlsx","csv"], key="st", label_visibility="collapsed")

    st.markdown("<br>", unsafe_allow_html=True)
    
    # ── ÚLTIMO CARD CENTRALIZADO ──
    c_center1, c_center2, c_center3 = st.columns([1, 2, 1])
    with c_center2:
        st.markdown(f"<div class='upload-card'><b style='color:{COR_LARANJA}'>🎵 Apify / TikTok</b><br><small style='color:{COR_SUBTEXTO}'>dataset_tiktok*.xlsx</small></div>", unsafe_allow_html=True)
        f_ap = st.file_uploader("Apify / TikTok", type=["xlsx","csv"], key="ap", label_visibility="collapsed")

    st.markdown("<br><br>", unsafe_allow_html=True)
    
    # ── BOTÃO CENTRALIZADO ──
    c_btn1, c_btn2, c_btn3 = st.columns([1, 1, 1])
    with c_btn2:
        processar = st.button("⚙️ Processar bases", type="primary", use_container_width=True)

    # ── LÓGICA DE PROCESSAMENTO ──
    if processar:
        if not any([f_bw, f_sm, f_st, f_ap]):
            st.warning("⚠️ Envie ao menos uma base para processar.")
        else:
            frames = []
            with st.status("Processando bases…", expanded=True) as status:
                if f_bw:
                    st.write("📡 Lendo Brandwatch…")
                    frames.append(parse_brandwatch(f_bw.read()))
                if f_sm:
                    st.write("📈 Lendo SuperMetrics…")
                    frames.append(parse_supermetrics(f_sm.read()))
                if f_st:
                    st.write("📊 Lendo Stilingue…")
                    frames.append(parse_stilingue(f_st.read()))
                if f_ap:
                    st.write("🎵 Lendo Apify / TikTok…")
                    frames.append(parse_apify(f_ap.read()))
                if f_ir2:
                    st.write("🏆 Lendo IR²…")
                    ir2_bytes = f_ir2.read()
                    st.session_state.ir2_scores = parse_ir2(ir2_bytes)
                    st.session_state.ir2_df_full = parse_ir2_full(ir2_bytes)
                st.write("🔗 Unificando e deduplicando…")
                st.session_state.df = unificar(frames, st.session_state.ir2_scores)
                n = len(st.session_state.df)
                status.update(label=f"✅ {n:,} publicações processadas!", state="complete")

            st.session_state.etapa = "dashboard"
            st.session_state.analise_ia_texto = None
            st.session_state.fig_grafo = None
            st.session_state.corpus_txt = None
            st.rerun()

    if st.session_state.df is not None and not processar:
        st.info(f"✅ Base já carregada com **{len(st.session_state.df):,}** publicações. Clique em **Dashboard** no menu lateral para visualizar.")

# ════════════════════════════════════════════════════════════════
# ETAPA 2 — DASHBOARD
# ════════════════════════════════════════════════════════════════
elif st.session_state.etapa == "dashboard":

    df_full = st.session_state.df

    if df_full is None:
        st.warning("Nenhuma base carregada. Vá para Upload e processe as bases primeiro.")
        st.stop()

    # ── Filtros globais ────────────────────────────────────────────
    with st.container():
        col_f1, col_f2, col_f3 = st.columns([2, 2, 1])
        with col_f1:
            redes_disponiveis = sorted(df_full["canal"].dropna().unique().tolist())
            redes_sel = st.multiselect("🌐 Rede social", ["Todas"] + redes_disponiveis, default=["Todas"])
        with col_f2:
            datas_validas = df_full["data"].dropna()
            if not datas_validas.empty:
                d_min = datas_validas.min().date()
                d_max = datas_validas.max().date()
                periodo = st.date_input("📅 Período", value=(d_min, d_max),
                                        min_value=d_min, max_value=d_max)
            else:
                periodo = None
        with col_f3:
            st.markdown("<br>", unsafe_allow_html=True)
            st.button("Aplicar filtro", use_container_width=True)

    df = df_full.copy()
    if "Todas" not in redes_sel and redes_sel:
        df = df[df["canal"].isin(redes_sel)]
    if periodo and len(periodo) == 2:
        df = safe_date_filter(df, periodo)

    st.divider()

    tab_vis, tab_grafo, tab_ir2, tab_tempo, tab_pub, tab_corpus = st.tabs([
        "📊 Visão geral",
        "🔗 Grafo de narrativas",
        "🏆 Ranking IR²",
        "📅 Linha do tempo",
        "📋 Publicações",
        "📝 Corpus Iramuteq",
    ])

    XLSX = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    PNG  = "image/png"

    def plotly_dark_layout(**kwargs):
        return dict(
            plot_bgcolor="#111111",
            paper_bgcolor="#111111",
            font=dict(color=COR_TEXTO, family="Inter, sans-serif"),
            **kwargs
        )

    # ════════ TAB 1 — VISÃO GERAL ════════
    with tab_vis:
        total_posts = len(df)
        total_inter = int(df["interacoes"].sum(skipna=True))
        total_pubs  = df["nome_publicador"].nunique()
        redes_n     = df["canal"].nunique()

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Publicações", f"{total_posts:,}")
        c2.metric("Interações",  f"{total_inter:,}")
        c3.metric("Publicadores únicos", f"{total_pubs:,}")
        c4.metric("Redes monitoradas", redes_n)

        st.markdown(f"<h4 style='color:{COR_LARANJA}; margin-top:1.5rem;'>🤖 Análise de IA</h4>", unsafe_allow_html=True)

        col_ia1, col_ia2 = st.columns([1, 4])
        with col_ia1:
            gerar_ia = st.button("✨ Gerar análise", type="primary", use_container_width=True)
        with col_ia2:
            if gerar_ia:
                by_canal_resumo = df.groupby("canal", dropna=False).agg(
                    publicacoes=("canal","count"),
                    interacoes=("interacoes", lambda x: int(x.sum(skipna=True)))
                ).reset_index().to_dict("records")

                resumo = {
                    "total_publicacoes": total_posts,
                    "total_interacoes": total_inter,
                    "publicadores_unicos": total_pubs,
                    "redes": redes_n,
                    "periodo_inicio": str(df["data"].dropna().min().date()) if df["data"].dropna().any() else "N/A",
                    "periodo_fim": str(df["data"].dropna().max().date()) if df["data"].dropna().any() else "N/A",
                    "distribuicao_por_rede": by_canal_resumo,
                    "sentimentos": df["sentimento"].value_counts(dropna=True).to_dict(),
                    "top_publicadores": df["nome_publicador"].value_counts().head(5).to_dict(),
                }

                with st.spinner("Gerando análise…"):
                    st.session_state.analise_ia_texto = gerar_analise_ia(str(resumo))

        if st.session_state.analise_ia_texto:
            st.markdown(f"""
            <div class="analise-ia-box">
            {st.session_state.analise_ia_texto.replace(chr(10), '<br>')}
            </div>
            """, unsafe_allow_html=True)
            st.download_button(
                "⬇️ Baixar análise (.txt)",
                data=st.session_state.analise_ia_texto.encode("utf-8"),
                file_name="ekklesia_analise_ia.txt",
                mime="text/plain"
            )

        st.markdown("---")

        by_canal = (
            df.groupby("canal", dropna=False)
            .agg(publicacoes=("canal","count"),
                 interacoes=("interacoes", lambda x: int(x.sum(skipna=True))),
                 publicadores=("nome_publicador","nunique"))
            .reset_index()
            .sort_values("publicacoes", ascending=False)
        )
        by_canal["canal"] = by_canal["canal"].fillna("desconhecido")
        by_canal["% publicações"] = (by_canal["publicacoes"]/total_posts*100).round(1)
        by_canal["% interações"]  = (by_canal["interacoes"]/(total_inter or 1)*100).round(1)
        by_canal["% publicadores"]= (by_canal["publicadores"]/(total_pubs or 1)*100).round(1)

        col_a, col_b = st.columns(2)
        with col_a:
            fig_posts = px.pie(by_canal, names="canal", values="publicacoes",
                               title="Publicações por rede",
                               hole=0.5, color="canal", color_discrete_map=CORES_REDES)
            fig_posts.update_traces(textinfo="percent+label", textfont_color="white")
            fig_posts.update_layout(**plotly_dark_layout(
                title_font_color=COR_LARANJA,
                showlegend=False
            ))
            st.plotly_chart(fig_posts, use_container_width=True)

        with col_b:
            fig_inter = px.pie(by_canal, names="canal", values="interacoes",
                               title="Interações por rede",
                               hole=0.5, color="canal", color_discrete_map=CORES_REDES)
            fig_inter.update_traces(textinfo="percent+label", textfont_color="white")
            fig_inter.update_layout(**plotly_dark_layout(
                title_font_color=COR_LARANJA,
                showlegend=False
            ))
            st.plotly_chart(fig_inter, use_container_width=True)

        st.markdown(f"#### Tabela por rede")
        st.dataframe(
            by_canal[["canal","publicacoes","% publicações","interacoes","% interações","publicadores","% publicadores"]],
            use_container_width=True, hide_index=True
        )

        if df["sentimento"].notna().any():
            st.markdown(f"#### Distribuição de sentimento")
            sent = df["sentimento"].value_counts(dropna=True).reset_index()
            sent.columns = ["sentimento","contagem"]
            COR_SENTIMENTO = {
                "positivo": "#4CAF50", "muito positivo": "#2E7D32",
                "negativo": "#E53935", "muito negativo": "#B71C1C",
                "neutro": COR_LARANJA,
            }
            colors = [COR_SENTIMENTO.get(s, COR_LARANJA2) for s in sent["sentimento"]]
            fig_sent = px.bar(sent, x="sentimento", y="contagem",
                              text="contagem", color="sentimento",
                              color_discrete_sequence=colors)
            fig_sent.update_layout(**plotly_dark_layout(showlegend=False,
                                   xaxis_title="", yaxis_title="publicações",
                                   title_font_color=COR_LARANJA))
            fig_sent.update_traces(textposition="outside", textfont_color=COR_TEXTO)
            st.plotly_chart(fig_sent, use_container_width=True)

        st.markdown("---")
        st.markdown("##### ⬇️ Downloads")
        dl_row([
            ("📄 Base filtrada (.xlsx)", df_to_excel_bytes(df), "ekklesia_base_filtrada.xlsx", XLSX),
            ("📄 Base completa (.xlsx)", df_to_excel_bytes(df_full), "ekklesia_base_completa.xlsx", XLSX),
            ("📊 Resumo por rede (.xlsx)", df_to_excel_bytes(
                by_canal[["canal","publicacoes","% publicações","interacoes","% interações","publicadores","% publicadores"]]
            ), "ekklesia_resumo_redes.xlsx", XLSX),
        ])
        png_posts = fig_to_png(fig_posts, height=500)
        png_inter = fig_to_png(fig_inter, height=500)
        png_sent  = fig_to_png(fig_sent, height=400) if df["sentimento"].notna().any() else None
        dl_row([
            ("🖼️ Gráfico publicações (.png)", png_posts, "ekklesia_publicacoes_por_rede.png", PNG),
            ("🖼️ Gráfico interações (.png)", png_inter, "ekklesia_interacoes_por_rede.png", PNG),
            ("🖼️ Gráfico sentimento (.png)", png_sent, "ekklesia_sentimento.png", PNG),
        ])

    # ════════ TAB 2 — GRAFO ════════
    with tab_grafo:
        st.markdown(f"#### 🔗 Grafo interativo de narrativas")
        st.markdown(f"<p style='color:{COR_SUBTEXTO}; font-size:0.85rem;'>Algoritmo TF-IDF + NetworkX. Detecta termos mais relevantes por período de tempo.</p>", unsafe_allow_html=True)

        col_g1, col_g2 = st.columns([1, 3])
        with col_g1:
            dias_bloco = st.select_slider("Agrupamento", options=[1,3,7,14,30], value=7,
                                          format_func=lambda x: f"{x} dia{'s' if x>1 else ''}")
            max_termos = st.slider("Termos por período", 6, 20, 12)
            gerar_btn  = st.button("🔄 Gerar grafo", type="primary", use_container_width=True)

        with col_g2:
            if gerar_btn:
                if df["conteudo"].dropna().empty:
                    st.warning("Nenhum conteúdo disponível para gerar o grafo.")
                else:
                    with st.spinner("Calculando grafo…"):
                        fig_grafo = gerar_grafo(df, dias_bloco, max_termos)
                    if fig_grafo:
                        st.session_state.fig_grafo = fig_grafo
                    else:
                        st.warning("Não foi possível gerar o grafo com os dados filtrados.")

            if st.session_state.fig_grafo is not None:
                st.plotly_chart(st.session_state.fig_grafo, use_container_width=True)

                st.markdown("---")
                st.markdown("##### ⬇️ Downloads do grafo")
                fig_g = st.session_state.fig_grafo
                png_grafo = fig_to_png(fig_g, width=1600, height=800)

                grafo_rows = []
                for frame in fig_g.frames:
                    per = frame.name
                    traces = frame.data
                    if len(traces) >= 2:
                        node_trace = traces[1]
                        labels = node_trace.text if node_trace.text else []
                        sizes  = node_trace.textfont.size if node_trace.textfont and node_trace.textfont.size else []
                        for i, lbl in enumerate(labels):
                            grafo_rows.append({
                                "periodo": per,
                                "termo": str(lbl).replace("<b>","").replace("</b>",""),
                                "centralidade_relativa": round(sizes[i], 2) if i < len(sizes) else None,
                            })
                df_grafo_xl = pd.DataFrame(grafo_rows)
                dl_row([
                    ("🖼️ Grafo PNG", png_grafo, "ekklesia_grafo.png", PNG),
                    ("📊 Dados do grafo (.xlsx)",
                     df_to_excel_bytes(df_grafo_xl) if not df_grafo_xl.empty else None,
                     "ekklesia_grafo_termos.xlsx", XLSX),
                ])

    # ════════ TAB 3 — RANKING IR² ════════
    with tab_ir2:
        st.markdown(f"#### 🏆 Ranking IR² — Relevância dos publicadores")

        if st.session_state.ir2_df_full is not None:
            df_ir2_raw = st.session_state.ir2_df_full.copy()
            st.markdown(f"<p style='color:{COR_SUBTEXTO}; font-size:0.85rem;'>Tabela completa da aba 'global' do arquivo IR². Score de relevância por plataforma (0–100).</p>", unsafe_allow_html=True)

            col_autor_ir2 = df_ir2_raw.columns[0]
            for c in df_ir2_raw.columns:
                if "autor" in c.lower():
                    col_autor_ir2 = c
                    break

            cols_num = [c for c in df_ir2_raw.columns if c != col_autor_ir2]
            for c in cols_num:
                df_ir2_raw[c] = pd.to_numeric(
                    df_ir2_raw[c].astype(str).str.replace(",","."), errors="coerce"
                ).round(2)

            perfis_base = df_full["nome_publicador"].dropna().str.lower().str.strip().unique()
            def match_perfil(k):
                if pd.isna(k): return "—"
                k2 = str(k).strip().lower()
                if k2 in perfis_base: return "✅ na base"
                if any(k2 in p or p in k2 for p in perfis_base): return "⚠️ parcial"
                return "❌ ausente"
            df_ir2_raw["na base?"] = df_ir2_raw[col_autor_ir2].map(match_perfil)

            col_global = None
            for c in df_ir2_raw.columns:
                if "global" in c.lower():
                    col_global = c
                    break

            if col_global:
                df_ir2_raw = df_ir2_raw.sort_values(col_global, ascending=False, na_position="last")
                df_top15 = df_ir2_raw.head(15).dropna(subset=[col_global])
                fig_rank = px.bar(
                    df_top15, x=col_global, y=col_autor_ir2, orientation="h",
                    color=col_global, color_continuous_scale=[[0, "#3D1F00"], [1, COR_LARANJA]],
                    title="Top 15 perfis — Score Global IR²",
                    text=col_global,
                )
                fig_rank.update_layout(**plotly_dark_layout(
                    yaxis=dict(autorange="reversed", color=COR_TEXTO),
                    xaxis=dict(color=COR_TEXTO),
                    coloraxis_showscale=False,
                    title_font_color=COR_LARANJA,
                ))
                fig_rank.update_traces(
                    texttemplate="%{text:.1f}",
                    textposition="outside",
                    textfont_color=COR_TEXTO,
                )
                st.plotly_chart(fig_rank, use_container_width=True)

            st.markdown("##### Tabela completa IR²")
            st.dataframe(df_ir2_raw, use_container_width=True, hide_index=True)

            png_rank = fig_to_png(fig_rank, height=500) if col_global else None
            dl_row([
                ("🖼️ Ranking IR² (.png)", png_rank, "ekklesia_ranking_ir2.png", PNG),
                ("📊 Ranking IR² (.xlsx)", df_to_excel_bytes(df_ir2_raw), "ekklesia_ranking_ir2.xlsx", XLSX),
            ])

        elif st.session_state.ir2_scores:
            scores = st.session_state.ir2_scores
            df_rank = pd.DataFrame([{"perfil": k, "score_global": v}
                                     for k, v in sorted(scores.items(), key=lambda x: -x[1])])
            df_rank.insert(0, "posição", range(1, len(df_rank)+1))
            perfis_base = df_full["nome_publicador"].dropna().str.lower().str.strip().unique()
            def match(k):
                if k in perfis_base: return "✅ na base"
                if any(k in p or p in k for p in perfis_base): return "⚠️ parcial"
                return "❌ ausente"
            df_rank["na base?"] = df_rank["perfil"].str.lower().str.strip().map(match)
            fig_rank = px.bar(df_rank.head(15), x="score_global", y="perfil", orientation="h",
                              color="score_global", color_continuous_scale=[[0,"#3D1F00"],[1,COR_LARANJA]],
                              title="Top 15 perfis por score IR²")
            fig_rank.update_layout(**plotly_dark_layout(
                yaxis=dict(autorange="reversed"), coloraxis_showscale=False,
                title_font_color=COR_LARANJA
            ))
            st.plotly_chart(fig_rank, use_container_width=True)
            st.dataframe(df_rank, use_container_width=True, hide_index=True)
        else:
            st.info("📂 Envie o arquivo IR² (ranking-compilado) na tela de Upload e reprocesse.")

    # ════════ TAB 4 — LINHA DO TEMPO ════════
    with tab_tempo:
        st.markdown(f"#### 📅 Evolução temporal")
        agrup = st.radio("Agrupar por", ["Dia","Semana","Mês"], horizontal=True, index=2)

        try:
            pd.period_range("2024-01", periods=1, freq="ME")
            _mes = "ME"
        except Exception:
            _mes = "M"
        freq_map = {"Dia":"D","Semana":"W","Mês":_mes}

        df_t = df.copy()
        df_t["_periodo"] = df_t["data"].dt.to_period(freq_map[agrup]).dt.to_timestamp()
        df_tempo = (
            df_t.groupby(["_periodo","canal"], dropna=False)
            .agg(publicacoes=("canal","count"),
                 interacoes=("interacoes", lambda x: x.sum(skipna=True)))
            .reset_index()
            .rename(columns={"_periodo":"período","canal":"rede"})
        )
        df_tempo["rede"] = df_tempo["rede"].fillna("desconhecido")

        fig_t1 = px.bar(df_tempo, x="período", y="publicacoes", color="rede",
                        title="Publicações ao longo do tempo",
                        labels={"publicacoes":"publicações"},
                        color_discrete_map=CORES_REDES)
        fig_t1.update_layout(**plotly_dark_layout(
            title_font_color=COR_LARANJA,
            legend=dict(font=dict(color=COR_TEXTO))
        ))
        st.plotly_chart(fig_t1, use_container_width=True)

        df_totais_tempo = df_tempo.groupby("período")["interacoes"].sum().reset_index()
        fig_t2 = px.line(df_totais_tempo, x="período", y="interacoes",
                         title="Interações totais ao longo do tempo", markers=True)
        fig_t2.update_traces(line_color=COR_LARANJA, marker_color=COR_LARANJA2)
        fig_t2.update_layout(**plotly_dark_layout(title_font_color=COR_LARANJA))
        st.plotly_chart(fig_t2, use_container_width=True)

        st.markdown("---")
        st.markdown("##### ⬇️ Downloads")
        buf_tempo = io.BytesIO()
        df_totais = df_tempo.groupby("período")[["publicacoes","interacoes"]].sum().reset_index()
        with pd.ExcelWriter(buf_tempo, engine="openpyxl") as w:
            df_tempo.to_excel(w, index=False, sheet_name="por_rede")
            df_totais.to_excel(w, index=False, sheet_name="totais")

        png_t1 = fig_to_png(fig_t1, width=1400, height=600)
        png_t2 = fig_to_png(fig_t2, width=1400, height=500)
        dl_row([
            ("🖼️ Publicações por rede (.png)", png_t1, "ekklesia_tempo_publicacoes.png", PNG),
            ("🖼️ Interações totais (.png)", png_t2, "ekklesia_tempo_interacoes.png", PNG),
            ("📊 Dados linha do tempo (.xlsx)", buf_tempo.getvalue(), "ekklesia_linha_do_tempo.xlsx", XLSX),
        ])

    # ════════ TAB 5 — PUBLICAÇÕES ════════
    with tab_pub:
        st.markdown(f"#### 📋 Tabela de publicações")
        st.markdown(f"<p style='color:{COR_SUBTEXTO}; font-size:0.85rem;'>Filtros globais já aplicados.</p>", unsafe_allow_html=True)

        COLS_TAB = ["data","canal","nome_publicador","conteudo","interacoes","link_publicacao"]
        df_pub = df[COLS_TAB].copy()
        
        # Formatando as colunas
        df_pub["data"] = pd.to_datetime(df_pub["data"], errors="coerce").dt.strftime("%d/%m/%Y")
        df_pub["interacoes"] = pd.to_numeric(df_pub["interacoes"], errors="coerce").fillna(0).astype(int)
        
        # Renomeando colunas para a tabela nativa
        df_pub.columns = ["Data","Rede","Publicador","Conteúdo","Interações","Link"]

        # Tabela nativa interativa do Streamlit
        st.dataframe(
            df_pub,
            column_config={
                "Link": st.column_config.LinkColumn("Link", display_text="🔗 acessar")
            },
            hide_index=True,
            use_container_width=True,
            height=600
        )

        st.markdown("---")
        st.markdown("##### ⬇️ Downloads")

        df_pub_xl = df[[c for c in COLS_TAB if c in df.columns]].copy()
        df_pub_xl.columns = ["Data","Rede","Publicador","Conteúdo","Interações","Link"]
        dl_row([
            ("📊 Publicações (.xlsx)", df_to_excel_bytes(df_pub_xl), "ekklesia_publicacoes.xlsx", XLSX),
        ])

    # ════════ TAB 6 — CORPUS IRAMUTEQ ════════
    with tab_corpus:
        st.markdown(f"#### 📝 Gerador de corpus Iramuteq")
        st.info("O corpus será gerado com os dados do filtro ativo (rede e período selecionados). Usa tokenização via NLTK.")

        col_c1, col_c2 = st.columns([1, 3])
        with col_c1:
            gerar_corpus_btn = st.button("📝 Gerar corpus", type="primary", use_container_width=True)
        with col_c2:
            if gerar_corpus_btn:
                n = df["conteudo"].dropna().shape[0]
                if n == 0:
                    st.warning("Nenhum conteúdo disponível.")
                else:
                    with st.spinner(f"Processando {n:,} textos…"):
                        corpus_txt = gerar_corpus(df)
                    if corpus_txt:
                        st.session_state.corpus_txt = corpus_txt
                        st.success(f"✅ Corpus gerado: {corpus_txt.count('****')} documentos")
                    else:
                        st.warning("Corpus vazio — verifique o conteúdo das publicações.")

            if st.session_state.corpus_txt:
                st.download_button(
                    "⬇️ Baixar corpus_iramuteq.txt",
                    data=st.session_state.corpus_txt.encode("utf-8"),
                    file_name="ekklesia_corpus_iramuteq.txt",
                    mime="text/plain",
                )
                with st.expander("Prévia do corpus (primeiras 50 linhas)"):
                    linhas = st.session_state.corpus_txt.split("\n")[:50]
                    st.code("\n".join(linhas), language="text")
