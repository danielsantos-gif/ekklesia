"""
╔══════════════════════════════════════════════════════╗
║           EKKLESIA — Monitor de Narrativas           ║
║   Brandwatch · SuperMetrics · Stilingue · Apify      ║
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

# ── Export helpers ────────────────────────────────────────────────
def fig_to_png(fig, width=1400, height=700):
    """Renderiza figura Plotly como PNG usando kaleido."""
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
    """Renderiza uma linha de botões de download. cols_cfg = lista de (label, data, filename, mime)."""
    cols = st.columns(len(cols_cfg))
    for col, (label, data, filename, mime) in zip(cols, cols_cfg):
        with col:
            if data:
                st.download_button(label, data=data, file_name=filename, mime=mime,
                                   use_container_width=True)
            else:
                st.button(label, disabled=True, use_container_width=True,
                          help="Gere o gráfico primeiro")

# ── NLP (lazy load) ──────────────────────────────────────────────
@st.cache_resource(show_spinner="Carregando modelo de linguagem…")
def load_nlp():
    import spacy
    try:
        return spacy.load("pt_core_news_sm")
    except OSError:
        import subprocess, sys
        subprocess.run([sys.executable, "-m", "spacy", "download", "pt_core_news_sm"], check=True)
        return spacy.load("pt_core_news_sm")

# ════════════════════════════════════════════════════════════════
# HELPERS
# ════════════════════════════════════════════════════════════════
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
        # merge_asof: encontra a data mais próxima sem loop
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
    # Lê só as colunas necessárias — muito mais rápido que carregar as 879
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
    scores = {}
    for sheet in xl.sheet_names:
        df = xl.parse(sheet, dtype=str)
        df.columns = df.columns.str.strip()
        for col in df.columns[1:]:
            vals = pd.to_numeric(df[col], errors="coerce").dropna()
            if not vals.empty:
                key = col.strip().lower()
                scores[key] = max(scores.get(key, 0), round(float(vals.mean()), 2))
    return scores

# ════════════════════════════════════════════════════════════════
# UNIFICAÇÃO E DEDUPLICAÇÃO
# ════════════════════════════════════════════════════════════════
def unificar(frames, ir2_scores):
    df = pd.concat(frames, ignore_index=True)
    df["data"] = pd.to_datetime(df["data"], errors="coerce", utc=False)
    try:
        if df["data"].dt.tz is not None:
            df["data"] = df["data"].dt.tz_localize(None)
    except Exception:
        pass

    NUM = ["seguidores","curtidas","comentarios","compartilhamentos",
           "visualizacoes","outras_reacoes","interacoes","score_relevancia"]
    for c in NUM:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df["canal"] = df["canal"].map(lambda x: norm_canal(x) if pd.notna(x) else pd.NA)

    # Enriquecer com IR²
    if ir2_scores:
        def get_score(nome):
            if pd.isna(nome): return pd.NA
            key = str(nome).lower().strip()
            if key in ir2_scores: return ir2_scores[key]
            for k, v in ir2_scores.items():
                if k in key or key in k: return v
            return pd.NA
        df["score_relevancia"] = df["nome_publicador"].map(get_score)

    # Calcular interacoes ausentes
    ICOLS = ["curtidas","comentarios","compartilhamentos","outras_reacoes"]
    mask = df["interacoes"].isna()
    df.loc[mask, "interacoes"] = (
        df.loc[mask, ICOLS].sum(axis=1, skipna=True)
        .where(~df.loc[mask, ICOLS].isna().all(axis=1), np.nan)
    )

    # Deduplicação
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
# CORPUS IRAMUTEQ
# ════════════════════════════════════════════════════════════════
def gerar_corpus(df_filtrado):
    nlp = load_nlp()
    TAGS = {"NOUN","VERB","ADJ","PROPN"}
    def limpa(t):
        t = str(t).replace("\n"," ").replace("\r"," ")
        return re.sub(r'["\'\\-\\$%\\*]', " ", t)
    def tokeniza(doc):
        return [tk.lemma_.lower() for tk in doc
                if tk.pos_ in TAGS and not tk.is_stop and len(tk.text) > 2]

    textos = (df_filtrado["titulo"].fillna("") + " " + df_filtrado["conteudo"].fillna("")).tolist()
    textos = [limpa(t) for t in textos if str(t).strip()]
    if not textos: return ""

    n = len(textos)
    n_termos = 150 if n <= 1000 else 300 if n <= 5000 else 400
    docs = list(nlp.pipe(textos, batch_size=500, disable=["parser","ner"]))
    tokens_por_doc = [tokeniza(d) for d in docs]

    from collections import Counter
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
# GRAFO INTERATIVO (TF-IDF + NetworkX)
# ════════════════════════════════════════════════════════════════
def gerar_grafo(df_filtrado, dias_bloco=7, max_termos=12):
    import networkx as nx
    from sklearn.feature_extraction.text import TfidfVectorizer
    import nltk
    try:
        stop_pt = nltk.corpus.stopwords.words("portuguese")
    except Exception:
        nltk.download("stopwords", quiet=True)
        stop_pt = nltk.corpus.stopwords.words("portuguese")

    df_filtrado = df_filtrado.copy()
    df_filtrado["_texto"] = (df_filtrado["titulo"].fillna("") + " " + df_filtrado["conteudo"].fillna("")).str.lower()
    df_filtrado["_data"]  = pd.to_datetime(df_filtrado["data"], errors="coerce").dt.date
    df_filtrado = df_filtrado.dropna(subset=["_data","_texto"])
    if df_filtrado.empty: return None

    data_min = pd.to_datetime(df_filtrado["_data"].min())
    df_filtrado["_bloco"] = df_filtrado["_data"].apply(
        lambda d: data_min + timedelta(days=((pd.to_datetime(d) - data_min).days // dias_bloco) * dias_bloco)
    )

    palette = ["#0078D4","#5C2D91","#008272","#D83B01","#E81123","#FFB900","#107C10"]
    frames, slider_steps = [], []

    for bloco in sorted(df_filtrado["_bloco"].unique()):
        docs = df_filtrado[df_filtrado["_bloco"] == bloco]["_texto"]
        if len(docs) < 2: continue
        try:
            vec = TfidfVectorizer(max_features=max_termos, ngram_range=(1,2), stop_words=stop_pt)
            X = vec.fit_transform(docs)
        except Exception: continue

        adj = X.T * X; adj.setdiag(0)
        G = nx.Graph()
        words = vec.get_feature_names_out()
        for i, j in zip(*adj.nonzero()):
            if i < j and adj[i, j] > 0.05:
                G.add_edge(words[i], words[j], weight=float(adj[i, j]))
        if len(G.nodes()) < 2: continue

        cent = nx.degree_centrality(G)
        max_c = max(cent.values()) or 1
        pos = nx.spring_layout(G, k=2.5/np.sqrt(len(G.nodes())), iterations=50, seed=42)

        # Anti-colisão limitada (50 iter para performance)
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
            if not moved: break

        communities = list(nx.community.greedy_modularity_communities(G))
        color_map = {n: palette[idx % len(palette)] for idx, c in enumerate(communities) for n in c}

        ex, ey = [], []
        for e in G.edges():
            x0,y0=pos[e[0]]; x1,y1=pos[e[1]]
            ex += [x0,x1,None]; ey += [y0,y1,None]

        sizes = [13 + (cent[n]/max_c)**1.8 * 42 for n in G.nodes()]
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
                           line=dict(width=0.6, color="rgba(150,150,150,0.3)"), hoverinfo="none"),
                go.Scatter(
                    x=[pos[n][0] for n in G.nodes()],
                    y=[pos[n][1] for n in G.nodes()],
                    mode="text", text=labels,
                    textfont=dict(size=sizes, color=[color_map.get(n,"#333") for n in G.nodes()]),
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
        slider_steps.append({"args":[[label],{"frame":{"duration":800,"redraw":True},"mode":"immediate"}],
                              "label":label, "method":"animate"})

    if not frames: return None

    fig = go.Figure(
        data=frames[0].data,
        layout=go.Layout(
            plot_bgcolor="white", paper_bgcolor="white",
            xaxis=dict(visible=False, autorange=False),
            yaxis=dict(visible=False, autorange=False),
            margin=dict(l=20, r=20, t=40, b=100),
            height=560,
            updatemenus=[{"type":"buttons","x":0.05,"y":-0.08,
                          "buttons":[{"label":"▶ Play","method":"animate",
                                      "args":[None,{"frame":{"duration":1500,"redraw":True},
                                                    "transition":{"duration":600}}]},
                                     {"label":"⏸ Pausar","method":"animate",
                                      "args":[[None],{"frame":{"duration":0},"mode":"immediate"}]}]}],
            sliders=[{"active":0,"y":-0.04,"len":0.9,"x":0.05,
                      "currentvalue":{"prefix":"Período: ","font":{"size":14}},
                      "steps":slider_steps}],
        ),
        frames=frames,
    )
    return fig

# ════════════════════════════════════════════════════════════════
# LAYOUT STREAMLIT
# ════════════════════════════════════════════════════════════════
st.set_page_config(page_title="Ekklesia", page_icon="🏛️", layout="wide")

st.markdown("""
<style>
[data-testid="stSidebar"] { background: #0f1117; }
[data-testid="stSidebar"] * { color: #e0e0e0 !important; }
.metric-card { background: var(--background-color); border: 1px solid rgba(128,128,128,0.2);
               border-radius: 10px; padding: 16px 20px; }
h1 { font-size: 1.6rem !important; }
</style>
""", unsafe_allow_html=True)

# ── Sidebar ─────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## 🏛️ Ekklesia")
    st.markdown("Monitor de Narrativas")
    st.divider()

    st.markdown("### 📁 Upload das bases")
    f_bw  = st.file_uploader("Brandwatch",   type=["xlsx","csv"], key="bw")
    f_sm  = st.file_uploader("SuperMetrics", type=["xlsx","csv"], key="sm")
    f_st  = st.file_uploader("Stilingue",    type=["xlsx","csv"], key="st")
    f_ap  = st.file_uploader("Apify / TikTok", type=["xlsx","csv"], key="ap")
    f_ir2 = st.file_uploader("IR² (ranking)", type=["xlsx","csv"], key="ir2")

    st.divider()
    processar = st.button("⚙️ Processar bases", type="primary", use_container_width=True)

# ── Estado da sessão ─────────────────────────────────────────────
if "df" not in st.session_state:
    st.session_state.df = None
if "ir2_scores" not in st.session_state:
    st.session_state.ir2_scores = {}

# ── Processamento ────────────────────────────────────────────────
if processar:
    if not any([f_bw, f_sm, f_st, f_ap]):
        st.warning("Envie ao menos uma base para processar.")
    else:
        frames = []
        with st.status("Processando bases…", expanded=True) as status:
            if f_bw:
                st.write("Lendo Brandwatch…")
                frames.append(parse_brandwatch(f_bw.read()))
            if f_sm:
                st.write("Lendo SuperMetrics…")
                frames.append(parse_supermetrics(f_sm.read()))
            if f_st:
                st.write("Lendo Stilingue…")
                frames.append(parse_stilingue(f_st.read()))
            if f_ap:
                st.write("Lendo Apify / TikTok…")
                frames.append(parse_apify(f_ap.read()))
            if f_ir2:
                st.write("Lendo IR²…")
                st.session_state.ir2_scores = parse_ir2(f_ir2.read())
            st.write("Unificando e deduplicando…")
            st.session_state.df = unificar(frames, st.session_state.ir2_scores)
            status.update(label=f"✅ {len(st.session_state.df):,} publicações processadas", state="complete")

# ── Corpo principal ──────────────────────────────────────────────
df_full = st.session_state.df

if df_full is None:
    st.title("🏛️ Ekklesia")
    st.markdown("Faça upload das bases na barra lateral e clique em **Processar bases**.")
    st.stop()

# ── Filtros globais ───────────────────────────────────────────────
redes_disponiveis = sorted(df_full["canal"].dropna().unique().tolist())
col_f1, col_f2, col_f3 = st.columns([2,2,1])
with col_f1:
    redes_sel = st.multiselect("🌐 Rede social", ["Todas"] + redes_disponiveis, default=["Todas"])
with col_f2:
    datas_validas = df_full["data"].dropna()
    if not datas_validas.empty:
        d_min = datas_validas.min().date()
        d_max = datas_validas.max().date()
        periodo = st.date_input("📅 Período", value=(d_min, d_max), min_value=d_min, max_value=d_max)
    else:
        periodo = None
with col_f3:
    st.markdown("<br>", unsafe_allow_html=True)
    aplicar = st.button("Aplicar filtro", use_container_width=True)

# aplica filtro
df = df_full.copy()
if "Todas" not in redes_sel and redes_sel:
    df = df[df["canal"].isin(redes_sel)]
if periodo and len(periodo) == 2:
    df = df[(df["data"].dt.date >= periodo[0]) & (df["data"].dt.date <= periodo[1])]

st.divider()

# ── Tabs ─────────────────────────────────────────────────────────
tab_vis, tab_grafo, tab_ir2, tab_tempo, tab_corpus = st.tabs([
    "📊 Visão geral",
    "🔗 Grafo de narrativas",
    "🏆 Ranking IR²",
    "📅 Linha do tempo",
    "📝 Corpus Iramuteq",
])

# ════════════ TAB 1 — VISÃO GERAL ════════════
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
                           title="Distribuição de publicações por rede",
                           hole=0.45, color_discrete_sequence=px.colors.qualitative.Set2)
        fig_posts.update_traces(textinfo="percent+label")
        st.plotly_chart(fig_posts, use_container_width=True)
    with col_b:
        fig_inter = px.pie(by_canal, names="canal", values="interacoes",
                           title="Distribuição de interações por rede",
                           hole=0.45, color_discrete_sequence=px.colors.qualitative.Set2)
        fig_inter.update_traces(textinfo="percent+label")
        st.plotly_chart(fig_inter, use_container_width=True)

    st.markdown("#### Tabela por rede")
    st.dataframe(by_canal[["canal","publicacoes","% publicações","interacoes","% interações","publicadores","% publicadores"]],
                 use_container_width=True, hide_index=True)

    # Sentimento
    if df["sentimento"].notna().any():
        st.markdown("#### Distribuição de sentimento")
        sent = df["sentimento"].value_counts(dropna=True).reset_index()
        sent.columns = ["sentimento","contagem"]
        fig_sent = px.bar(sent, x="sentimento", y="contagem",
                          color="sentimento", text="contagem",
                          color_discrete_sequence=px.colors.qualitative.Pastel)
        fig_sent.update_layout(showlegend=False, xaxis_title="", yaxis_title="publicações")
        st.plotly_chart(fig_sent, use_container_width=True)

    # ── Downloads Visão Geral ─────────────────────────────────────
    st.markdown("---")
    st.markdown("##### ⬇️ Downloads")

    XLSX = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    PNG  = "image/png"

    # Linha 1 — bases brutas
    dl_row([
        ("📄 Base filtrada (.xlsx)",  df_to_excel_bytes(df),      "ekklesia_base_filtrada.xlsx",  XLSX),
        ("📄 Base completa (.xlsx)",  df_to_excel_bytes(df_full), "ekklesia_base_completa.xlsx",  XLSX),
        ("📊 Resumo por rede (.xlsx)", df_to_excel_bytes(
            by_canal[["canal","publicacoes","% publicações","interacoes",
                       "% interações","publicadores","% publicadores"]]
        ), "ekklesia_resumo_redes.xlsx", XLSX),
    ])

    # Linha 2 — PNGs dos gráficos desta aba
    png_posts = fig_to_png(fig_posts, height=500)
    png_inter = fig_to_png(fig_inter, height=500)
    png_sent  = fig_to_png(fig_sent,  height=400) if df["sentimento"].notna().any() else None
    dl_row([
        ("🖼️ Gráfico publicações (.png)", png_posts, "ekklesia_publicacoes_por_rede.png", PNG),
        ("🖼️ Gráfico interações (.png)",  png_inter, "ekklesia_interacoes_por_rede.png",  PNG),
        ("🖼️ Gráfico sentimento (.png)",  png_sent,  "ekklesia_sentimento.png",            PNG),
    ])

    # Excel detalhado de sentimento (se disponível)
    if df["sentimento"].notna().any():
        sent_detalhe = (
            df.groupby(["canal","sentimento"], dropna=False)
            .agg(publicacoes=("sentimento","count"))
            .reset_index()
        )
        dl_row([
            ("📊 Sentimento por rede (.xlsx)", df_to_excel_bytes(sent_detalhe),
             "ekklesia_sentimento_por_rede.xlsx", XLSX),
        ])

# ════════════ TAB 2 — GRAFO ════════════
with tab_grafo:
    st.markdown("#### Grafo interativo de narrativas (TF-IDF + NetworkX)")
    col_g1, col_g2 = st.columns([1,3])
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

        if "fig_grafo" in st.session_state:
            st.plotly_chart(st.session_state.fig_grafo, use_container_width=True)

            # Downloads do grafo
            st.markdown("---")
            st.markdown("##### ⬇️ Downloads do grafo")

            XLSX = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            PNG  = "image/png"

            # PNG do frame atual (primeiro frame visível)
            fig_g = st.session_state.fig_grafo
            png_grafo = fig_to_png(fig_g, width=1600, height=800)

            # Excel com dados dos termos (extraído dos frames)
            grafo_rows = []
            for frame in fig_g.frames:
                periodo = frame.name
                traces = frame.data
                if len(traces) >= 2:
                    node_trace = traces[1]
                    labels = node_trace.text if node_trace.text else []
                    sizes  = node_trace.textfont.size if node_trace.textfont and node_trace.textfont.size else []
                    for i, lbl in enumerate(labels):
                        grafo_rows.append({
                            "periodo":     periodo,
                            "termo":       str(lbl).replace("<b>","").replace("</b>",""),
                            "centralidade_relativa": round(sizes[i], 2) if i < len(sizes) else None,
                        })
            df_grafo_xl = pd.DataFrame(grafo_rows)

            dl_row([
                ("🖼️ Grafo PNG (frame atual)", png_grafo, "ekklesia_grafo.png", PNG),
                ("📊 Dados do grafo (.xlsx)",
                 df_to_excel_bytes(df_grafo_xl) if not df_grafo_xl.empty else None,
                 "ekklesia_grafo_termos.xlsx", XLSX),
            ])

# ════════════ TAB 3 — RANKING IR² ════════════
with tab_ir2:
    st.markdown("#### Ranking IR² — Relevância dos publicadores")
    if not st.session_state.ir2_scores:
        st.info("Envie o arquivo IR² (ranking-compilado) na barra lateral e reprocesse.")
    else:
        scores = st.session_state.ir2_scores
        df_rank = pd.DataFrame([{"perfil": k, "score_global": v} for k, v in sorted(scores.items(), key=lambda x: -x[1])])
        df_rank.insert(0, "posição", range(1, len(df_rank)+1))

        # Cruzamento com base
        perfis_base = df_full["nome_publicador"].dropna().str.lower().str.strip().unique()
        def match(k):
            if k in perfis_base: return "✅ na base"
            if any(k in p or p in k for p in perfis_base): return "⚠️ parcial"
            return "❌ ausente"
        df_rank["na base?"] = df_rank["perfil"].str.lower().str.strip().map(match)

        fig_rank = px.bar(df_rank.head(15), x="score_global", y="perfil", orientation="h",
                          color="score_global", color_continuous_scale="Blues",
                          title="Top 15 perfis por score IR²")
        fig_rank.update_layout(yaxis=dict(autorange="reversed"), coloraxis_showscale=False)
        st.plotly_chart(fig_rank, use_container_width=True)
        st.dataframe(df_rank, use_container_width=True, hide_index=True)

        XLSX = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        PNG  = "image/png"
        png_rank = fig_to_png(fig_rank, height=500)
        dl_row([
            ("🖼️ Ranking IR² (.png)",   png_rank,                       "ekklesia_ranking_ir2.png",  PNG),
            ("📊 Ranking IR² (.xlsx)",  df_to_excel_bytes(df_rank),     "ekklesia_ranking_ir2.xlsx", XLSX),
        ])

# ════════════ TAB 4 — LINHA DO TEMPO ════════════
with tab_tempo:
    st.markdown("#### Evolução temporal de publicações e interações")
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
                    color_discrete_sequence=px.colors.qualitative.Set2)
    st.plotly_chart(fig_t1, use_container_width=True)

    fig_t2 = px.line(df_tempo.groupby("período")["interacoes"].sum().reset_index(),
                     x="período", y="interacoes",
                     title="Interações totais ao longo do tempo",
                     markers=True)
    fig_t2.update_traces(line_color="#0078D4")
    st.plotly_chart(fig_t2, use_container_width=True)

    # ── Downloads Linha do Tempo ──────────────────────────────────
    st.markdown("---")
    st.markdown("##### ⬇️ Downloads")

    XLSX = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    PNG  = "image/png"

    # Excel com duas abas: por rede e totais
    buf_tempo = io.BytesIO()
    df_totais = df_tempo.groupby("período")[["publicacoes","interacoes"]].sum().reset_index()
    with pd.ExcelWriter(buf_tempo, engine="openpyxl") as w:
        df_tempo.to_excel(w,   index=False, sheet_name="por_rede")
        df_totais.to_excel(w,  index=False, sheet_name="totais")

    png_t1 = fig_to_png(fig_t1, width=1400, height=600)
    png_t2 = fig_to_png(fig_t2, width=1400, height=500)

    dl_row([
        ("🖼️ Publicações por rede (.png)",   png_t1,               "ekklesia_tempo_publicacoes.png",  PNG),
        ("🖼️ Interações totais (.png)",       png_t2,               "ekklesia_tempo_interacoes.png",   PNG),
        ("📊 Dados linha do tempo (.xlsx)",   buf_tempo.getvalue(), "ekklesia_linha_do_tempo.xlsx",    XLSX),
    ])

# ════════════ TAB 5 — CORPUS IRAMUTEQ ════════════
with tab_corpus:
    st.markdown("#### Gerador de corpus para Iramuteq")
    st.info("O corpus será gerado com os dados do filtro ativo (rede e período selecionados).")
    col_c1, col_c2 = st.columns([1,3])
    with col_c1:
        gerar_corpus_btn = st.button("📝 Gerar corpus", type="primary", use_container_width=True)
    with col_c2:
        if gerar_corpus_btn:
            n = df["conteudo"].dropna().shape[0]
            if n == 0:
                st.warning("Nenhum conteúdo disponível.")
            else:
                with st.spinner(f"Processando {n} textos com spaCy…"):
                    corpus_txt = gerar_corpus(df)
                if corpus_txt:
                    st.session_state.corpus_txt = corpus_txt
                    st.success(f"Corpus gerado: {corpus_txt.count('****')} documentos")
                else:
                    st.warning("Corpus vazio — verifique o conteúdo das publicações.")

        if "corpus_txt" in st.session_state:
            st.download_button(
                "⬇️ Baixar corpus_iramuteq.txt",
                data=st.session_state.corpus_txt.encode("utf-8"),
                file_name="ekklesia_corpus_iramuteq.txt",
                mime="text/plain",
            )
            with st.expander("Prévia do corpus (primeiras 50 linhas)"):
                linhas = st.session_state.corpus_txt.split("\n")[:50]
                st.code("\n".join(linhas), language="text")
