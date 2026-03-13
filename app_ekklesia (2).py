"""
╔══════════════════════════════════════════════════════╗
║           EKKLESIA — Monitor de Narrativas           ║
║   Brandwatch · SuperMetrics · Stilingue · Apify      ║
║   v2.0 — Redesign escuro + IA + IR² corrigido        ║
╚══════════════════════════════════════════════════════╝
"""

import io, re, unicodedata, warnings, base64
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
# CONFIGURAÇÃO DA PÁGINA (deve vir primeiro)
# ════════════════════════════════════════════════════════════════
st.set_page_config(page_title="Ekklesia", page_icon="🏛️", layout="wide")

# ── Tema escuro com laranja ──────────────────────────────────────
st.markdown("""
<style>
/* Fundo geral escuro */
.stApp { background-color: #0f1117 !important; color: #e0e0e0 !important; }
[data-testid="stAppViewContainer"] { background-color: #0f1117 !important; }
[data-testid="stHeader"] { background-color: #0f1117 !important; }

/* Sidebar */
[data-testid="stSidebar"] { background: #16181f !important; border-right: 1px solid #2a2d3a; }
[data-testid="stSidebar"] * { color: #e0e0e0 !important; }
[data-testid="stSidebar"] .stFileUploader label { color: #e0e0e0 !important; }

/* Tabs */
[data-testid="stTabs"] [role="tab"] {
    background: #1e2030 !important; color: #aaa !important;
    border-radius: 8px 8px 0 0 !important; border: 1px solid #2a2d3a !important;
    font-weight: 500;
}
[data-testid="stTabs"] [role="tab"][aria-selected="true"] {
    background: #E8770A !important; color: #fff !important;
    border-color: #E8770A !important;
}

/* Botões primários — laranja */
.stButton > button[kind="primary"] {
    background: #E8770A !important; color: #fff !important;
    border: none !important; border-radius: 8px !important;
    font-weight: 600 !important;
}
.stButton > button[kind="primary"]:hover {
    background: #d4690a !important;
}

/* Botões secundários */
.stButton > button {
    background: #1e2030 !important; color: #e0e0e0 !important;
    border: 1px solid #3a3d4a !important; border-radius: 8px !important;
}

/* Métricas */
[data-testid="stMetric"] {
    background: #1e2030 !important; border: 1px solid #2a2d3a !important;
    border-radius: 12px !important; padding: 16px 20px !important;
}
[data-testid="stMetricValue"] { color: #E8770A !important; font-size: 2rem !important; font-weight: 700 !important; }
[data-testid="stMetricLabel"] { color: #aaa !important; }

/* Títulos */
h1, h2, h3 { color: #ffffff !important; }
h4 { color: #E8770A !important; }

/* DataFrames */
[data-testid="stDataFrame"] { border-radius: 8px !important; }

/* Inputs e selects */
[data-testid="stMultiSelect"] > div,
[data-testid="stDateInput"] > div > div {
    background: #1e2030 !important;
    border-color: #3a3d4a !important;
    color: #e0e0e0 !important;
}

/* Divider */
hr { border-color: #2a2d3a !important; }

/* Info / Warning boxes */
[data-testid="stAlert"] { background: #1e2030 !important; border-radius: 8px !important; }

/* Download buttons */
[data-testid="stDownloadButton"] > button {
    background: #1e2030 !important; color: #E8770A !important;
    border: 1px solid #E8770A !important; border-radius: 8px !important;
    font-weight: 500 !important;
}
[data-testid="stDownloadButton"] > button:hover {
    background: #E8770A !important; color: #fff !important;
}

/* Status/spinner */
[data-testid="stStatusWidget"] { background: #1e2030 !important; border-color: #E8770A !important; }

/* Scrollbar */
::-webkit-scrollbar { width: 6px; height: 6px; }
::-webkit-scrollbar-track { background: #1e2030; }
::-webkit-scrollbar-thumb { background: #E8770A; border-radius: 3px; }

/* Upload area */
[data-testid="stFileUploadDropzone"] {
    background: #1e2030 !important; border: 2px dashed #E8770A !important;
    border-radius: 10px !important;
}

/* Step indicator */
.step-badge {
    display: inline-block; background: #E8770A; color: #fff;
    border-radius: 50%; width: 32px; height: 32px; line-height: 32px;
    text-align: center; font-weight: 700; font-size: 1rem; margin-right: 10px;
}

/* Analysis box */
.analysis-box {
    background: linear-gradient(135deg, #1e2030 0%, #16181f 100%);
    border-left: 4px solid #E8770A; border-radius: 0 12px 12px 0;
    padding: 20px 24px; margin: 16px 0; color: #e0e0e0;
    line-height: 1.7;
}

/* Table links */
a { color: #E8770A !important; }
a:hover { color: #ffaa55 !important; }
</style>
""", unsafe_allow_html=True)

# ════════════════════════════════════════════════════════════════
# HELPERS GLOBAIS
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

def apply_dark_plotly(fig, title=None):
    """Aplica tema escuro padrão a qualquer figura Plotly."""
    fig.update_layout(
        plot_bgcolor="#1e2030",
        paper_bgcolor="#1e2030",
        font=dict(color="#e0e0e0"),
        title=dict(text=title, font=dict(color="#ffffff", size=15)) if title else {},
        legend=dict(bgcolor="#16181f", bordercolor="#3a3d4a", borderwidth=1),
        xaxis=dict(gridcolor="#2a2d3a", linecolor="#3a3d4a"),
        yaxis=dict(gridcolor="#2a2d3a", linecolor="#3a3d4a"),
    )
    return fig

ORANGE_SCALE = ["#3a1a00","#7a3300","#b84f00","#E8770A","#ff9933","#ffcc88","#fff0cc"]

# ════════════════════════════════════════════════════════════════
# NLP — substitui spaCy por NLTK (compatível Python 3.14)
# ════════════════════════════════════════════════════════════════
@st.cache_resource(show_spinner="Carregando recursos de linguagem…")
def load_nltk_resources():
    import nltk
    for resource in ["stopwords", "punkt", "rslp"]:
        try:
            nltk.data.find(f"corpora/{resource}")
        except LookupError:
            nltk.download(resource, quiet=True)
        except Exception:
            try:
                nltk.download(resource, quiet=True)
            except Exception:
                pass
    return True

def tokeniza_nltk(texto):
    """Tokenização simples com NLTK, sem spaCy."""
    import nltk
    try:
        stops = set(nltk.corpus.stopwords.words("portuguese"))
    except Exception:
        stops = set()
    words = re.findall(r'\b[a-záàâãéèêíïóôõöúüçñ]{3,}\b', str(texto).lower())
    return [w for w in words if w not in stops]

# ════════════════════════════════════════════════════════════════
# HELPERS DE NORMALIZAÇÃO
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
    """
    Lê APENAS a aba 'global' do arquivo IR².
    Chave: coluna 'autor_normalizado'
    Score: coluna 'ranking global'
    Retorna dict {autor_lower: score_global}
    """
    xl = pd.ExcelFile(io.BytesIO(file_bytes), engine="openpyxl")
    
    # Busca a aba 'global' (case-insensitive)
    aba_global = None
    for name in xl.sheet_names:
        if name.strip().lower() == "global":
            aba_global = name
            break
    
    if aba_global is None:
        st.warning("⚠️ IR²: aba 'global' não encontrada no arquivo.")
        return {}
    
    df = xl.parse(aba_global, dtype=str)
    df.columns = df.columns.str.strip()
    
    # Localiza coluna de autor (case-insensitive)
    col_autor = None
    for c in df.columns:
        if "autor" in c.lower():
            col_autor = c
            break
    
    # Localiza coluna de ranking global (case-insensitive)
    col_score = None
    for c in df.columns:
        if "ranking global" in c.lower() or c.lower() == "ranking global":
            col_score = c
            break
    
    if col_autor is None or col_score is None:
        st.warning(f"⚠️ IR²: colunas necessárias não encontradas. Colunas disponíveis: {list(df.columns)}")
        return {}
    
    scores = {}
    for _, row in df.iterrows():
        autor = row.get(col_autor)
        score = row.get(col_score)
        if pd.notna(autor) and str(autor).strip():
            val = pd.to_numeric(str(score).replace(",", "."), errors="coerce")
            if pd.notna(val):
                scores[str(autor).strip().lower()] = round(float(val), 2)
    
    return scores

def parse_ir2_full(file_bytes):
    """
    Retorna DataFrame completo da aba 'global' para exibição na tabela IR².
    """
    xl = pd.ExcelFile(io.BytesIO(file_bytes), engine="openpyxl")
    aba_global = None
    for name in xl.sheet_names:
        if name.strip().lower() == "global":
            aba_global = name
            break
    if aba_global is None:
        return pd.DataFrame()
    df = xl.parse(aba_global, dtype=str)
    df.columns = df.columns.str.strip()
    # Converte colunas numéricas
    for c in df.columns:
        if c.lower() != "autor_normalizado" and "autor" not in c.lower():
            df[c] = df[c].apply(lambda x: pd.to_numeric(str(x).replace(",","."), errors="coerce") if pd.notna(x) else np.nan)
    return df

# ════════════════════════════════════════════════════════════════
# UNIFICAÇÃO E DEDUPLICAÇÃO
# ════════════════════════════════════════════════════════════════
def unificar(frames, ir2_scores):
    df = pd.concat(frames, ignore_index=True)
    
    # ── Corrige timezone (bug linha 670 original) ──────────────
    df["data"] = pd.to_datetime(df["data"], errors="coerce", utc=False)
    try:
        # Remove timezone de qualquer coluna de data com tz
        if hasattr(df["data"], "dt") and df["data"].dt.tz is not None:
            df["data"] = df["data"].dt.tz_localize(None)
    except Exception:
        try:
            df["data"] = df["data"].dt.tz_convert(None)
        except Exception:
            pass
    # Força tz_localize(None) linha a linha para casos mistos
    def remove_tz(x):
        if pd.isna(x): return pd.NaT
        try:
            if hasattr(x, "tzinfo") and x.tzinfo is not None:
                return x.replace(tzinfo=None)
            return x
        except Exception:
            return x
    df["data"] = df["data"].apply(remove_tz)
    df["data"] = pd.to_datetime(df["data"], errors="coerce")

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
# CORPUS IRAMUTEQ — com NLTK (sem spaCy)
# ════════════════════════════════════════════════════════════════
def gerar_corpus(df_filtrado):
    load_nltk_resources()
    
    def limpa(t):
        t = str(t).replace("\n"," ").replace("\r"," ")
        t = re.sub(r'https?://\S+', ' ', t)
        t = re.sub(r'[^\w\s]', ' ', t)
        return re.sub(r'\s+', ' ', t).strip()

    textos = (df_filtrado["titulo"].fillna("") + " " + df_filtrado["conteudo"].fillna("")).tolist()
    textos = [limpa(t) for t in textos if str(t).strip() and str(t).strip() not in ("nan nan", "")]
    if not textos: return ""

    n = len(textos)
    n_termos = 150 if n <= 1000 else 300 if n <= 5000 else 400
    
    tokens_por_doc = [tokeniza_nltk(t) for t in textos]

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

    palette = ["#E8770A","#ff9933","#ffcc00","#ff5500","#cc4400","#ffaa55","#ffd480"]
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
                           line=dict(width=0.6, color="rgba(232,119,10,0.2)"), hoverinfo="none"),
                go.Scatter(
                    x=[pos[n][0] for n in G.nodes()],
                    y=[pos[n][1] for n in G.nodes()],
                    mode="text", text=labels,
                    textfont=dict(size=sizes, color=[color_map.get(n,"#E8770A") for n in G.nodes()]),
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
            plot_bgcolor="#1e2030", paper_bgcolor="#1e2030",
            font=dict(color="#e0e0e0"),
            xaxis=dict(visible=False, autorange=False),
            yaxis=dict(visible=False, autorange=False),
            margin=dict(l=20, r=20, t=40, b=100),
            height=560,
            updatemenus=[{"type":"buttons","x":0.05,"y":-0.08,
                          "buttons":[{"label":"▶ Play","method":"animate",
                                      "args":[None,{"frame":{"duration":1500,"redraw":True},
                                                    "transition":{"duration":600}}]},
                                     {"label":"⏸ Pausar","method":"animate",
                                      "args":[[None],{"frame":{"duration":0},"mode":"immediate"}]}],
                          "bgcolor":"#1e2030","bordercolor":"#E8770A","font":{"color":"#E8770A"}}],
            sliders=[{"active":0,"y":-0.04,"len":0.9,"x":0.05,
                      "currentvalue":{"prefix":"Período: ","font":{"size":14,"color":"#E8770A"}},
                      "steps":slider_steps,
                      "bgcolor":"#1e2030","bordercolor":"#3a3d4a",
                      "activebgcolor":"#E8770A"}],
        ),
        frames=frames,
    )
    return fig

# ════════════════════════════════════════════════════════════════
# ANÁLISE IA — Claude API
# ════════════════════════════════════════════════════════════════
def gerar_analise_ia(df, redes_sel, periodo):
    """Chama a API do Claude para gerar análise em 2 parágrafos."""
    import json
    
    api_key = st.secrets.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        return None, "Chave ANTHROPIC_API_KEY não configurada nos Secrets do Streamlit."

    # Resumo estatístico para enviar à API
    total = len(df)
    total_inter = int(df["interacoes"].sum(skipna=True))
    by_canal = df.groupby("canal", dropna=False).agg(
        pubs=("canal","count"),
        inter=("interacoes", lambda x: int(x.sum(skipna=True)))
    ).reset_index().sort_values("pubs", ascending=False)
    
    canal_resumo = "; ".join(
        f"{row['canal']}: {row['pubs']} publicações, {row['inter']} interações"
        for _, row in by_canal.head(5).iterrows()
    )
    
    top_autores = df.nlargest(5, "interacoes")[["nome_publicador","canal","interacoes"]].dropna(subset=["nome_publicador"])
    autores_txt = "; ".join(
        f"{r['nome_publicador']} ({r['canal']}): {int(r['interacoes'])} interações"
        for _, r in top_autores.iterrows()
    ) if not top_autores.empty else "não disponível"
    
    sent_dist = ""
    if df["sentimento"].notna().any():
        s = df["sentimento"].value_counts(normalize=True).mul(100).round(1)
        sent_dist = ", ".join(f"{k}: {v}%" for k, v in s.items())
    
    periodo_txt = f"{periodo[0]} a {periodo[1]}" if periodo and len(periodo)==2 else "período completo"
    redes_txt = ", ".join(redes_sel) if "Todas" not in redes_sel else "todas as redes"
    
    prompt = f"""Você é um analista de comunicação e redes sociais da Nexus, empresa de pesquisa e inteligência de dados. 
Analise os dados abaixo e escreva EXATAMENTE 2 parágrafos em português brasileiro:

- Parágrafo 1 (Contexto geral): Síntese do volume, distribuição por rede e padrões de engajamento
- Parágrafo 2 (Destaques e padrões): Insights mais relevantes, incluindo sentimento, top publicadores e tendências

Dados do monitoramento:
- Período: {periodo_txt}
- Redes monitoradas: {redes_txt}
- Total de publicações: {total:,}
- Total de interações: {total_inter:,}
- Distribuição por rede: {canal_resumo}
- Top publicadores por interações: {autores_txt}
- Distribuição de sentimento: {sent_dist if sent_dist else "não disponível"}

Escreva de forma objetiva, analítica e profissional. Não use bullet points. Apenas 2 parágrafos corridos."""

    import urllib.request
    headers = {
        "Content-Type": "application/json",
        "x-api-key": api_key,
        "anthropic-version": "2023-06-01"
    }
    body = json.dumps({
        "model": "claude-sonnet-4-20250514",
        "max_tokens": 600,
        "messages": [{"role": "user", "content": prompt}]
    }).encode("utf-8")

    try:
        req = urllib.request.Request(
            "https://api.anthropic.com/v1/messages",
            data=body, headers=headers, method="POST"
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        texto = data["content"][0]["text"]
        return texto, None
    except Exception as e:
        return None, f"Erro ao chamar a API: {str(e)}"

# ════════════════════════════════════════════════════════════════
# LOGO EM BASE64
# ════════════════════════════════════════════════════════════════
def get_logo_base64():
    """Tenta carregar o logo do arquivo local."""
    logo_paths = [
        "ekklesia_logo_4k.png",
        "ekklesia_logo.png",
        "logo.png",
    ]
    for p in logo_paths:
        if Path(p).exists():
            with open(p, "rb") as f:
                return base64.b64encode(f.read()).decode()
    return None

# ════════════════════════════════════════════════════════════════
# ETAPA 1 — TELA DE UPLOAD
# ════════════════════════════════════════════════════════════════
def tela_upload():
    # Header com logo
    logo_b64 = get_logo_base64()
    
    col_logo, col_title = st.columns([1, 3])
    with col_logo:
        if logo_b64:
            st.markdown(
                f'<img src="data:image/png;base64,{logo_b64}" style="max-width:180px; margin-top:8px;">',
                unsafe_allow_html=True
            )
        else:
            st.markdown("### 🏛️")
    with col_title:
        st.markdown("""
        <h1 style="color:#ffffff; margin-bottom:4px;">Ekklesia</h1>
        <p style="color:#E8770A; font-size:1.1rem; margin:0;">Monitor de Narrativas · Nexus</p>
        """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    st.markdown("""
    <div style="background:#1e2030; border-radius:12px; padding:20px 24px; margin-bottom:24px; border-left: 4px solid #E8770A;">
        <span class="step-badge">1</span>
        <strong style="color:#fff; font-size:1.1rem;">Upload das bases de dados</strong>
        <p style="color:#aaa; margin: 8px 0 0 42px;">Envie os arquivos exportados das ferramentas de monitoramento. Você pode enviar de 1 a 5 fontes simultaneamente.</p>
    </div>
    """, unsafe_allow_html=True)

    col1, col2 = st.columns(2)
    with col1:
        st.markdown("##### 📊 Brandwatch")
        f_bw = st.file_uploader("Arquivo `*Uso_Geral*.xlsx`", type=["xlsx","csv"], key="bw",
                                 help="Cabeçalho na linha 7 (6 linhas de metadata)")
        
        st.markdown("##### 📱 SuperMetrics")
        f_sm = st.file_uploader("Arquivo `*Energisa*.xlsx`", type=["xlsx","csv"], key="sm",
                                 help="Abas: facebook, instagram, facebook seguidores, instagram seguidores")
        
        st.markdown("##### 🎵 Apify / TikTok")
        f_ap = st.file_uploader("Arquivo `dataset_tiktok*.xlsx`", type=["xlsx","csv"], key="ap",
                                 help="879 colunas JSON achatado")

    with col2:
        st.markdown("##### 📈 Stilingue")
        f_st = st.file_uploader("Arquivo `RelatorioExpress*.xlsx`", type=["xlsx","csv"], key="st",
                                 help="Mapeamento direto de colunas")
        
        st.markdown("##### 🏆 IR² (Ranking de relevância)")
        f_ir2 = st.file_uploader("Arquivo `ranking-compilado*.xlsx`", type=["xlsx","csv"], key="ir2",
                                  help="Aba 'global' — ranking de relevância por autor (0-100)")

    st.markdown("---")
    
    arquivos_enviados = [f for f in [f_bw, f_sm, f_st, f_ap] if f is not None]
    n_arquivos = len(arquivos_enviados)
    
    if n_arquivos > 0:
        st.markdown(f"**{n_arquivos} base(s) pronta(s) para processar** {' + IR²' if f_ir2 else ''}")
    
    col_btn, col_info = st.columns([1, 3])
    with col_btn:
        processar = st.button(
            "⚙️ Processar bases →",
            type="primary",
            use_container_width=True,
            disabled=(n_arquivos == 0),
        )
    with col_info:
        if n_arquivos == 0:
            st.info("👆 Envie ao menos uma base para continuar.")
        else:
            st.success(f"✅ {n_arquivos} arquivo(s) carregado(s). Clique em **Processar** para continuar.")

    return processar, f_bw, f_sm, f_st, f_ap, f_ir2

# ════════════════════════════════════════════════════════════════
# ETAPA 2 — DASHBOARD
# ════════════════════════════════════════════════════════════════
def tela_dashboard(df_full, ir2_bytes):
    # ── Header ───────────────────────────────────────────────────
    logo_b64 = get_logo_base64()
    
    col_logo, col_title, col_btn = st.columns([1, 4, 1])
    with col_logo:
        if logo_b64:
            st.markdown(
                f'<img src="data:image/png;base64,{logo_b64}" style="max-width:120px; margin-top:8px;">',
                unsafe_allow_html=True
            )
    with col_title:
        st.markdown("""
        <h2 style="color:#ffffff; margin:0;">Ekklesia <span style="color:#E8770A; font-size:0.8em;">Monitor de Narrativas</span></h2>
        <p style="color:#888; margin:0; font-size:0.9rem;">Nexus — Pesquisa e Inteligência de Dados</p>
        """, unsafe_allow_html=True)
    with col_btn:
        if st.button("← Nova análise", use_container_width=True):
            st.session_state.df = None
            st.session_state.ir2_scores = {}
            st.session_state.ir2_df = pd.DataFrame()
            st.session_state.pop("fig_grafo", None)
            st.session_state.pop("corpus_txt", None)
            st.session_state.pop("analise_ia", None)
            st.session_state.etapa = 1
            st.rerun()

    st.markdown("---")

    # ── Filtros globais ──────────────────────────────────────────
    redes_disponiveis = sorted(df_full["canal"].dropna().unique().tolist())
    col_f1, col_f2, col_f3 = st.columns([2, 2, 1])
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

    # Aplica filtros — com correção do bug de timezone
    df = df_full.copy()
    if "Todas" not in redes_sel and redes_sel:
        df = df[df["canal"].isin(redes_sel)]
    if periodo and len(periodo) == 2:
        # Corrige TypeError linha 670: garante que data é naive (sem timezone)
        try:
            df_data = df["data"].dt.tz_localize(None) if df["data"].dt.tz is not None else df["data"]
        except Exception:
            df_data = pd.to_datetime(df["data"], errors="coerce").apply(
                lambda x: x.replace(tzinfo=None) if pd.notna(x) and hasattr(x, 'tzinfo') and x.tzinfo else x
            )
        df = df[(df_data.dt.date >= periodo[0]) & (df_data.dt.date <= periodo[1])]

    st.markdown("---")

    # ── Tabs ─────────────────────────────────────────────────────
    tab_vis, tab_grafo, tab_ir2_tab, tab_tempo, tab_corpus, tab_pubs = st.tabs([
        "📊 Visão geral",
        "🔗 Grafo de narrativas",
        "🏆 Ranking IR²",
        "📅 Linha do tempo",
        "📝 Corpus Iramuteq",
        "🔗 Publicações",
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

        # ── Análise de IA ────────────────────────────────────────
        st.markdown("#### 🤖 Análise automática (IA)")
        
        col_ia1, col_ia2 = st.columns([1, 4])
        with col_ia1:
            gerar_ia = st.button("✨ Gerar análise", type="primary", use_container_width=True)
        with col_ia2:
            if gerar_ia:
                with st.spinner("Analisando dados com IA…"):
                    texto_ia, erro_ia = gerar_analise_ia(df, redes_sel, periodo)
                if texto_ia:
                    st.session_state.analise_ia = texto_ia
                elif erro_ia:
                    st.error(f"❌ {erro_ia}")
        
        if "analise_ia" in st.session_state and st.session_state.analise_ia:
            st.markdown(
                f'<div class="analysis-box">{st.session_state.analise_ia}</div>',
                unsafe_allow_html=True
            )
            st.download_button(
                "⬇️ Baixar análise (.txt)",
                data=st.session_state.analise_ia.encode("utf-8"),
                file_name="ekklesia_analise_ia.txt",
                mime="text/plain",
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

        CORES_REDES = ["#E8770A","#ff9933","#ffcc00","#cc4400","#ff5500","#ffd480","#ff6b35"]

        col_a, col_b = st.columns(2)
        with col_a:
            fig_posts = px.pie(by_canal, names="canal", values="publicacoes",
                               title="Distribuição de publicações por rede",
                               hole=0.45, color_discrete_sequence=CORES_REDES)
            fig_posts.update_traces(textinfo="percent+label", textfont_color="#fff")
            apply_dark_plotly(fig_posts)
            st.plotly_chart(fig_posts, use_container_width=True)
        with col_b:
            fig_inter = px.pie(by_canal, names="canal", values="interacoes",
                               title="Distribuição de interações por rede",
                               hole=0.45, color_discrete_sequence=CORES_REDES)
            fig_inter.update_traces(textinfo="percent+label", textfont_color="#fff")
            apply_dark_plotly(fig_inter)
            st.plotly_chart(fig_inter, use_container_width=True)

        st.markdown("#### Tabela por rede")
        st.dataframe(by_canal[["canal","publicacoes","% publicações","interacoes","% interações","publicadores","% publicadores"]],
                     use_container_width=True, hide_index=True)

        if df["sentimento"].notna().any():
            st.markdown("#### Distribuição de sentimento")
            sent = df["sentimento"].value_counts(dropna=True).reset_index()
            sent.columns = ["sentimento","contagem"]
            SENT_CORES = {"positivo":"#E8770A","neutro":"#888","negativo":"#cc3300",
                          "muito positivo":"#ffcc00","muito negativo":"#880000"}
            fig_sent = px.bar(sent, x="sentimento", y="contagem",
                              color="sentimento", text="contagem",
                              color_discrete_map=SENT_CORES)
            fig_sent.update_layout(showlegend=False, xaxis_title="", yaxis_title="publicações")
            apply_dark_plotly(fig_sent)
            st.plotly_chart(fig_sent, use_container_width=True)

        st.markdown("---")
        st.markdown("##### ⬇️ Downloads")

        XLSX = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        PNG  = "image/png"

        dl_row([
            ("📄 Base filtrada (.xlsx)",  df_to_excel_bytes(df),      "ekklesia_base_filtrada.xlsx",  XLSX),
            ("📄 Base completa (.xlsx)",  df_to_excel_bytes(df_full), "ekklesia_base_completa.xlsx",  XLSX),
            ("📊 Resumo por rede (.xlsx)", df_to_excel_bytes(
                by_canal[["canal","publicacoes","% publicações","interacoes",
                           "% interações","publicadores","% publicadores"]]
            ), "ekklesia_resumo_redes.xlsx", XLSX),
        ])

        png_posts = fig_to_png(fig_posts, height=500)
        png_inter = fig_to_png(fig_inter, height=500)
        png_sent  = fig_to_png(fig_sent,  height=400) if df["sentimento"].notna().any() else None
        dl_row([
            ("🖼️ Gráfico publicações (.png)", png_posts, "ekklesia_publicacoes_por_rede.png", PNG),
            ("🖼️ Gráfico interações (.png)",  png_inter, "ekklesia_interacoes_por_rede.png",  PNG),
            ("🖼️ Gráfico sentimento (.png)",  png_sent,  "ekklesia_sentimento.png",            PNG),
        ])

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

                st.markdown("---")
                st.markdown("##### ⬇️ Downloads do grafo")

                XLSX = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                PNG  = "image/png"

                fig_g = st.session_state.fig_grafo
                png_grafo = fig_to_png(fig_g, width=1600, height=800)

                grafo_rows = []
                for frame in fig_g.frames:
                    periodo_nome = frame.name
                    traces = frame.data
                    if len(traces) >= 2:
                        node_trace = traces[1]
                        labels = node_trace.text if node_trace.text else []
                        sizes  = node_trace.textfont.size if node_trace.textfont and node_trace.textfont.size else []
                        for i, lbl in enumerate(labels):
                            grafo_rows.append({
                                "periodo":     periodo_nome,
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
    with tab_ir2_tab:
        st.markdown("#### Ranking IR² — Relevância dos publicadores")
        
        ir2_df = st.session_state.get("ir2_df", pd.DataFrame())
        ir2_scores = st.session_state.ir2_scores
        
        if ir2_df.empty and not ir2_scores:
            st.info("Envie o arquivo IR² (ranking-compilado) na etapa de upload.")
        else:
            # Exibe tabela completa da aba global se disponível
            if not ir2_df.empty:
                st.markdown("##### Tabela completa de rankings")
                
                # Cruzamento com base
                perfis_base = df_full["nome_publicador"].dropna().str.lower().str.strip().unique()
                col_autor_nome = None
                for c in ir2_df.columns:
                    if "autor" in c.lower():
                        col_autor_nome = c
                        break
                
                df_ir2_display = ir2_df.copy()
                if col_autor_nome:
                    def match_autor(k):
                        if pd.isna(k): return "❓"
                        k_lower = str(k).lower().strip()
                        if k_lower in perfis_base: return "✅ na base"
                        if any(k_lower in p or p in k_lower for p in perfis_base): return "⚠️ parcial"
                        return "❌ ausente"
                    df_ir2_display["na base?"] = df_ir2_display[col_autor_nome].map(match_autor)
                
                st.dataframe(df_ir2_display, use_container_width=True, hide_index=True)
            
            # Gráfico com scores globais
            if ir2_scores:
                df_rank = pd.DataFrame([
                    {"perfil": k, "score_global": v}
                    for k, v in sorted(ir2_scores.items(), key=lambda x: -x[1])
                ])
                df_rank.insert(0, "posição", range(1, len(df_rank)+1))
                
                fig_rank = px.bar(
                    df_rank.head(15), x="score_global", y="perfil", orientation="h",
                    color="score_global", color_continuous_scale=["#3a1a00","#E8770A","#ffcc88"],
                    title="Top 15 perfis por score IR² (ranking global)"
                )
                fig_rank.update_layout(yaxis=dict(autorange="reversed"), coloraxis_showscale=False)
                apply_dark_plotly(fig_rank)
                st.plotly_chart(fig_rank, use_container_width=True)

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

        CORES_REDES = ["#E8770A","#ff9933","#ffcc00","#cc4400","#ff5500","#ffd480","#ff6b35"]
        
        fig_t1 = px.bar(df_tempo, x="período", y="publicacoes", color="rede",
                        title="Publicações ao longo do tempo",
                        labels={"publicacoes":"publicações"},
                        color_discrete_sequence=CORES_REDES)
        apply_dark_plotly(fig_t1)
        st.plotly_chart(fig_t1, use_container_width=True)

        fig_t2 = px.line(df_tempo.groupby("período")["interacoes"].sum().reset_index(),
                         x="período", y="interacoes",
                         title="Interações totais ao longo do tempo",
                         markers=True)
        fig_t2.update_traces(line_color="#E8770A", marker_color="#E8770A")
        apply_dark_plotly(fig_t2)
        st.plotly_chart(fig_t2, use_container_width=True)

        st.markdown("---")
        st.markdown("##### ⬇️ Downloads")

        XLSX = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        PNG  = "image/png"

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
        st.info("O corpus será gerado com os dados do filtro ativo (rede e período selecionados). Utiliza NLTK (compatível com Python 3.14).")
        col_c1, col_c2 = st.columns([1,3])
        with col_c1:
            gerar_corpus_btn = st.button("📝 Gerar corpus", type="primary", use_container_width=True)
        with col_c2:
            if gerar_corpus_btn:
                n = df["conteudo"].dropna().shape[0]
                if n == 0:
                    st.warning("Nenhum conteúdo disponível.")
                else:
                    with st.spinner(f"Processando {n} textos com NLTK…"):
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

    # ════════════ TAB 6 — PUBLICAÇÕES COM LINKS ════════════
    with tab_pubs:
        st.markdown("#### Tabela de publicações com links clicáveis")
        
        # Prepara colunas de exibição
        cols_show = ["data","canal","nome_publicador","conteudo","interacoes","link_publicacao"]
        df_pubs = df[cols_show].copy()
        df_pubs["data"] = df_pubs["data"].dt.strftime("%d/%m/%Y").fillna("—")
        df_pubs["conteudo"] = df_pubs["conteudo"].fillna("").str[:120] + df_pubs["conteudo"].fillna("").apply(lambda x: "…" if len(str(x)) > 120 else "")
        df_pubs["interacoes"] = df_pubs["interacoes"].fillna(0).astype(int)
        df_pubs["canal"] = df_pubs["canal"].fillna("—")
        df_pubs["nome_publicador"] = df_pubs["nome_publicador"].fillna("—")
        
        # Converte links em HTML clicável
        def make_link(url):
            if pd.isna(url) or not str(url).strip() or str(url) == "nan":
                return "—"
            return f'<a href="{url}" target="_blank">🔗 abrir</a>'
        
        df_pubs["link"] = df_pubs["link_publicacao"].apply(make_link)
        df_pubs = df_pubs.drop(columns=["link_publicacao"])
        df_pubs = df_pubs.rename(columns={
            "data":"Data","canal":"Rede","nome_publicador":"Autor",
            "conteudo":"Conteúdo","interacoes":"Interações","link":"Link"
        })
        
        # Ordenação
        col_ord1, col_ord2, _ = st.columns([1, 1, 2])
        with col_ord1:
            ordenar_por = st.selectbox("Ordenar por", ["Interações","Data","Rede","Autor"])
        with col_ord2:
            ordem = st.radio("Ordem", ["↓ Maior primeiro","↑ Menor primeiro"], horizontal=True)
        
        asc = "↑" in ordem
        ord_map = {"Interações":"Interações","Data":"Data","Rede":"Rede","Autor":"Autor"}
        df_pubs = df_pubs.sort_values(ord_map[ordenar_por], ascending=asc)
        
        # Renderiza tabela com HTML para links clicáveis
        st.markdown(
            df_pubs.to_html(escape=False, index=False, classes="pub-table"),
            unsafe_allow_html=True
        )
        st.markdown("""
        <style>
        .pub-table { width:100%; border-collapse:collapse; color:#e0e0e0; font-size:0.85rem; }
        .pub-table th { background:#E8770A; color:#fff; padding:8px 12px; text-align:left; }
        .pub-table td { background:#1e2030; padding:7px 12px; border-bottom:1px solid #2a2d3a; vertical-align:top; }
        .pub-table tr:hover td { background:#252840; }
        </style>
        """, unsafe_allow_html=True)
        
        st.markdown(f"**{len(df_pubs):,} publicações exibidas**")
        
        XLSX = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        st.download_button(
            "📊 Exportar publicações (.xlsx)",
            data=df_to_excel_bytes(df_pubs.drop(columns=["Link"])),
            file_name="ekklesia_publicacoes.xlsx",
            mime=XLSX,
        )

# ════════════════════════════════════════════════════════════════
# CONTROLE DE ETAPAS — MAIN
# ════════════════════════════════════════════════════════════════
if "etapa" not in st.session_state:
    st.session_state.etapa = 1
if "df" not in st.session_state:
    st.session_state.df = None
if "ir2_scores" not in st.session_state:
    st.session_state.ir2_scores = {}
if "ir2_df" not in st.session_state:
    st.session_state.ir2_df = pd.DataFrame()

if st.session_state.etapa == 1 or st.session_state.df is None:
    processar, f_bw, f_sm, f_st, f_ap, f_ir2 = tela_upload()
    
    if processar:
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
                ir2_bytes = f_ir2.read()
                st.session_state.ir2_scores = parse_ir2(io.BytesIO(ir2_bytes).read())
                st.session_state.ir2_df = parse_ir2_full(ir2_bytes)
            st.write("Unificando e deduplicando…")
            st.session_state.df = unificar(frames, st.session_state.ir2_scores)
            n = len(st.session_state.df)
            status.update(label=f"✅ {n:,} publicações processadas", state="complete")
        
        st.session_state.etapa = 2
        st.rerun()
else:
    tela_dashboard(st.session_state.df, None)
