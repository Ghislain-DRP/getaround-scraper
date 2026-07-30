#!/usr/bin/env python3
"""
analyse.py — Getaround Scraper v3.2
Module d'analyse et de synthèse des snapshots Getaround.

Règles non négociables :
- commune_annonce est utilisée pour tout calcul géographique (jamais commune_recherche)
- Les snapshots sont comparés uniquement au sein du même pipeline (A1 vs A1, A2 vs A2, B vs B)
- note et nb_avis vides sont traités comme valeurs manquantes, jamais comme 0
- Le pool de comparaison est segment × type_connexion (repli sur segment seul si < 8 véhicules)
- pool_fiable = False → la synthèse signale explicitement l'insuffisance du pool
"""

import json
import math
import re
import unicodedata
import glob
from datetime import datetime
from pathlib import Path
from statistics import median, mean
from typing import Optional


# ─────────────────────────────────────────────
# 1. SEGMENTATION
# ─────────────────────────────────────────────

def _strip_accents(s: str) -> str:
    return "".join(
        c for c in unicodedata.normalize("NFD", str(s))
        if unicodedata.category(c) != "Mn"
    )


# Ordre = priorité décroissante. La première règle qui matche gagne.
SEGMENT_RULES = [
    ("CABRIOLET",      r"cabriolet|500 c\b|\bmega\b.*cabriolet|evoque cabriolet|\b2cv\b"),
    ("COLLECTION",     r"coccinelle|cox|beetle|mustang|\b911\b|cayman|boxster|lotus|"
                       r"citroen ds\b|ds20|ds19|ds21|ds23|\bds 19\b|\bds 20\b|\bds 21\b|\bds 23\b|"
                       r"alfa romeo spider|mx 5|mx-5|miata|triumph|morgan"),
    ("UTILITAIRE",     r"fourgon|cargo|jumper|jumpy(?! multispace)|ducato|doblo|fiorino|dokker|"
                       r"transit(?! connect)|boxer|master|movano|nemo|berlingo(?! multispace)|"
                       r"partner(?! tepee)|kangoo(?! combi)|nv200|expert(?! tepee)(?! double)|"
                       r"proace fourgon|trafic(?! passenger)|vivaro(?! combi)|sprinter(?! combi)|"
                       r"citan fourgon|combo cargo|rifter cargo|"
                       r"\bnv400\b|man tge|proace city electric|evito|\bexpress\b"),
    ("MONOSPACE",      r"5008|c4 picasso|c3 picasso|grand scenic|\bscenic\b|espace|jogger|lodgy|"
                       r"c max|c-max|traveller|vito tourer|trafic passenger|expert tepee|"
                       r"expert double|proace combi|jumpy multispace|vivaro combi|sprinter combi|"
                       r"classe b\b|modus|kangoo combi|berlingo multispace|rifter(?! cargo)|"
                       r"proace city verso|citan combi|combo life|"
                       r"voyager|\bverso\b|touran|\bix20\b|"
                       r"classe v\b|symbioz"),
    ("BREAK",          r"\bsw\b|estate|touring sports|\bbreak\b|rxh|cross tourer|"
                       r"\bvariant\b|avant\b|touring\b|sportbrake|alltrack|cross country|"
                       r"clubman|logan mcv|b-max|b max|"
                       r"\bv40\b"),
    ("SUV_FAMILIAL",   r"3008|tiguan|kodiaq|\bx5\b|cr v|cr-v|model y|austral|kadjar|"
                       r"countryman|\bx3\b|\bq5\b|sorento|santa fe|tucson|grandland|"
                       r"\bq7\b|\bq8\b|\bx6\b|\bx7\b|discovery sport|range rover evoque|"
                       r"range rover velar|range rover sport|range rover|evoque|velar|"
                       r"stelvio|levante|urus|cayenne|macan|touareg|edge|kuga|"
                       r"\bx1\b|\bix1\b|\bx2\b|renegade|lynk|\bq4\b|hs phev|xc40|"
                       r"rav 4|rav4|\bex30\b|\bds 7\b|\bds7\b|"
                       r"\bsuv\b.*familial|cx 5|cx-5|cx 60|cx-60|eclipse cross|outlander|"
                       r"\bcr-v\b|\bpassat\b.*alltrack|"
                       r"\bix35\b|\bxc60\b"),
    ("SUV_COMPACT",    r"2008|captur|mokka|t cross|t-cross|\bpuma\b|kamiq|c hr|c-hr|kona|niro|"
                       r"formentor|classe gla|\bgla\b|ds3 crossback|ds 3 crossback|yaris cross|"
                       r"zs ev|hr v|hr-v|\bzs\b|juke|arona|\bq2\b|\bq3\b|"
                       r"\bglc\b|\bgle\b|\bgla\b|\bglb\b|"
                       r"cx 3|cx-3|cx 30|cx-30|asx|crossland|grandland x(?! suv)|"
                       r"\btroc\b|t-roc|taigo|\bkaroq\b|\bkamiq\b|"
                       r"\bpulsar\b|\bterrano\b|\bqashqai\b|\bx trail\b|x-trail|"
                       r"\bc5 aircross\b|\bpace\b|\bf-pace\b|\be-pace\b|"
                       r"\bexplorer\b|\bpuma\b|\bkuga\b|\bfocus\b.*active|"
                       r"lexus ux|lexus ct|"
                       r"\bkia soul\b|\bkia venga\b|\bmg ehs\b"),
    ("BERLINE",        r"passat|\b508\b|classe c|classe e|classe s|laguna|\bds5\b|\bds4\b|"
                       r"model 3|serie 3|serie 5|\ba4\b|\ba6\b|insignia|mondeo|superb|talisman|"
                       r"\ba7\b|\ba8\b|serie 7|\bls\b|\bes\b.*berline|"
                       r"\bm3\b|\bm5\b|\brs3\b|\brs4\b|\brs6\b|"
                       r"\b407\b|\b607\b|\b307\b.*berline|"
                       r"\blaguna\b|\bvel satis\b|\blatitude\b|\bfluence\b|"
                       r"\bbravo\b|\blinea\b|\bstilo\b|\balbea\b|"
                       r"\baccord\b|\blegend\b|\binsight\b|"
                       r"a5 sportback|\bcla\b|cruze|giulietta|byd seal|"
                       r"\bds 4 e-tense\b|\bds4 e-tense\b"),
    ("COMPACTE",       r"\bgolf\b|\b308\b|megane|\bc4\b|c4 cactus|\ba3\b|serie 1|astra|auris|"
                       r"corolla|focus(?! active)|classe a|prius|ioniq|leon|octavia|i30|ceed|civic|"
                       r"punto|\btipo\b|\bserie 2\b|serie 2 gran coupe|"
                       r"\b207\b|\b206\b|\b301\b|\b408\b|"
                       r"\bvera\b|\brapid\b|\bscala\b|\bkaroq\b.*compact|"
                       r"\bpulse\b|\bspring\b|\btwingo\b.*gt|"
                       r"\bnote\b|\bpulsar\b|\btiida\b|\balmera\b|"
                       r"\bspark\b|\btrax\b|\bverano\b|"
                       r"\bimpreza\b|\blegacy\b|\bxv\b|"
                       r"arkana|baleno|inster|\bmg4\b|\bmg 4\b|mazda 2|\b500l\b"),
    ("CITADINE",       r"clio|\b208\b|\bpolo\b|corsa|yaris|fiesta|micra|\bi20\b|\brio\b|\bc3\b|"
                       r"\ba1\b|ds 3|\bds3\b|\bzoe\b|\b500\b|swift|\bibiza\b|fabia|"
                       r"mini cooper(?! clubman)(?! countryman)|renault 5|\b5 e tech\b|"
                       r"\bi10\b|\bsandero\b|\blogon\b|\bduster\b(?! suv)|"
                       r"\bseat mii\b|\bvw up\b|\bskoda citigo\b|"
                       r"\bsplash\b|\baltoz\b|\bpicanto\b|\bmorning\b|"
                       r"\btwingo\b(?! gt)|\bwind\b|\bclio\b|\bkangoo\b.*city|"
                       r"\bleaf\b|\be 208\b|\be-208\b|\bid.3\b|\bspring\b.*electr|"
                       r"\bka\b|\badam\b|\bagila\b|\b500e\b|\b600e\b|aveo|space star|"
                       r"\blogan\b(?! mcv)|"
                       r"\bmito\b|\bkarl\b|\bmeriva\b|c-elysee|c elysee|\bmini one\b"),
    ("MICRO_CITADINE", r"\bc1\b|\b107\b|\b108\b|aygo|twingo(?! gt)|\bup\b|e up|fortwo|forfour|"
                       r"pixo|ignis|panda|smart|\bi1\b"),
]#!/usr/bin/env python3
"""
analyse.py — Getaround Scraper v3.2
Module d'analyse et de synthèse des snapshots Getaround.

Règles non négociables :
- commune_annonce est utilisée pour tout calcul géographique (jamais commune_recherche)
- Les snapshots sont comparés uniquement au sein du même pipeline (A1 vs A1, A2 vs A2, B vs B)
- note et nb_avis vides sont traités comme valeurs manquantes, jamais comme 0
- Le pool de comparaison est segment × type_connexion (repli sur segment seul si < 8 véhicules)
- pool_fiable = False → la synthèse signale explicitement l'insuffisance du pool
"""

import json
import math
import re
import unicodedata
import glob
from datetime import datetime
from pathlib import Path
from statistics import median, mean
from typing import Optional


# ─────────────────────────────────────────────
# 1. SEGMENTATION
# ─────────────────────────────────────────────

def _strip_accents(s: str) -> str:
    return "".join(
        c for c in unicodedata.normalize("NFD", str(s))
        if unicodedata.category(c) != "Mn"
    )


# Ordre = priorité décroissante. La première règle qui matche gagne.
SEGMENT_RULES = [
    ("CABRIOLET",      r"cabriolet|500 c\b|\bmega\b.*cabriolet|evoque cabriolet|\b2cv\b"),
    ("COLLECTION",     r"coccinelle|cox|beetle|mustang|\b911\b|cayman|boxster|lotus|"
                       r"citroen ds\b|ds20|ds19|ds21|ds23|\bds 19\b|\bds 20\b|\bds 21\b|\bds 23\b|"
                       r"alfa romeo spider|mx 5|mx-5|miata|triumph|morgan"),
    ("UTILITAIRE",     r"fourgon|cargo|jumper|jumpy(?! multispace)|ducato|doblo|fiorino|dokker|"
                       r"transit(?! connect)|boxer|master|movano|nemo|berlingo(?! multispace)|"
                       r"partner(?! tepee)|kangoo(?! combi)|nv200|expert(?! tepee)(?! double)|"
                       r"proace fourgon|trafic(?! passenger)|vivaro(?! combi)|sprinter(?! combi)|"
                       r"citan fourgon|combo cargo|rifter cargo|"
                       r"\bnv400\b|man tge|proace city electric|evito|\bexpress\b"),
    ("MONOSPACE",      r"5008|c4 picasso|c3 picasso|grand scenic|\bscenic\b|espace|jogger|lodgy|"
                       r"c max|c-max|traveller|vito tourer|trafic passenger|expert tepee|"
                       r"expert double|proace combi|jumpy multispace|vivaro combi|sprinter combi|"
                       r"classe b\b|modus|kangoo combi|berlingo multispace|rifter(?! cargo)|"
                       r"proace city verso|citan combi|combo life|"
                       r"voyager|\bverso\b|touran|\bix20\b|"
                       r"classe v\b|symbioz"),
    ("BREAK",          r"\bsw\b|estate|touring sports|\bbreak\b|rxh|cross tourer|"
                       r"\bvariant\b|avant\b|touring\b|sportbrake|alltrack|cross country|"
                       r"clubman|logan mcv|b-max|b max|"
                       r"\bv40\b"),
    ("SUV_FAMILIAL",   r"3008|tiguan|kodiaq|\bx5\b|cr v|cr-v|model y|austral|kadjar|"
                       r"countryman|\bx3\b|\bq5\b|sorento|santa fe|tucson|grandland|"
                       r"\bq7\b|\bq8\b|\bx6\b|\bx7\b|discovery sport|range rover evoque|"
                       r"range rover velar|range rover sport|range rover|evoque|velar|"
                       r"stelvio|levante|urus|cayenne|macan|touareg|edge|kuga|"
                       r"\bx1\b|\bix1\b|\bx2\b|renegade|lynk|\bq4\b|hs phev|xc40|"
                       r"rav 4|rav4|\bex30\b|\bds 7\b|\bds7\b|"
                       r"\bsuv\b.*familial|cx 5|cx-5|cx 60|cx-60|eclipse cross|outlander|"
                       r"\bcr-v\b|\bpassat\b.*alltrack|"
                       r"\bix35\b|\bxc60\b"),
    ("SUV_COMPACT",    r"2008|captur|mokka|t cross|t-cross|\bpuma\b|kamiq|c hr|c-hr|kona|niro|"
                       r"formentor|classe gla|\bgla\b|ds3 crossback|ds 3 crossback|yaris cross|"
                       r"zs ev|hr v|hr-v|\bzs\b|juke|arona|\bq2\b|\bq3\b|"
                       r"\bglc\b|\bgle\b|\bgla\b|\bglb\b|"
                       r"cx 3|cx-3|cx 30|cx-30|asx|crossland|grandland x(?! suv)|"
                       r"\btroc\b|t-roc|taigo|\bkaroq\b|\bkamiq\b|"
                       r"\bpulsar\b|\bterrano\b|\bqashqai\b|\bx trail\b|x-trail|"
                       r"\bc5 aircross\b|\bpace\b|\bf-pace\b|\be-pace\b|"
                       r"\bexplorer\b|\bpuma\b|\bkuga\b|\bfocus\b.*active|"
                       r"lexus ux|lexus ct|"
                       r"\bkia soul\b|\bkia venga\b|\bmg ehs\b"),
    ("BERLINE",        r"passat|\b508\b|classe c|classe e|classe s|laguna|\bds5\b|\bds4\b|"
                       r"model 3|serie 3|serie 5|\ba4\b|\ba6\b|insignia|mondeo|superb|talisman|"
                       r"\ba7\b|\ba8\b|serie 7|\bls\b|\bes\b.*berline|"
                       r"\bm3\b|\bm5\b|\brs3\b|\brs4\b|\brs6\b|"
                       r"\b407\b|\b607\b|\b307\b.*berline|"
                       r"\blaguna\b|\bvel satis\b|\blatitude\b|\bfluence\b|"
                       r"\bbravo\b|\blinea\b|\bstilo\b|\balbea\b|"
                       r"\baccord\b|\blegend\b|\binsight\b|"
                       r"a5 sportback|\bcla\b|cruze|giulietta|byd seal|"
                       r"\bds 4 e-tense\b|\bds4 e-tense\b"),
    ("COMPACTE",       r"\bgolf\b|\b308\b|megane|\bc4\b|c4 cactus|\ba3\b|serie 1|astra|auris|"
                       r"corolla|focus(?! active)|classe a|prius|ioniq|leon|octavia|i30|ceed|civic|"
                       r"punto|\btipo\b|\bserie 2\b|serie 2 gran coupe|"
                       r"\b207\b|\b206\b|\b301\b|\b408\b|"
                       r"\bvera\b|\brapid\b|\bscala\b|\bkaroq\b.*compact|"
                       r"\bpulse\b|\bspring\b|\btwingo\b.*gt|"
                       r"\bnote\b|\bpulsar\b|\btiida\b|\balmera\b|"
                       r"\bspark\b|\btrax\b|\bverano\b|"
                       r"\bimpreza\b|\blegacy\b|\bxv\b|"
                       r"arkana|baleno|inster|\bmg4\b|\bmg 4\b|mazda 2|\b500l\b"),
    ("CITADINE",       r"clio|\b208\b|\bpolo\b|corsa|yaris|fiesta|micra|\bi20\b|\brio\b|\bc3\b|"
                       r"\ba1\b|ds 3|\bds3\b|\bzoe\b|\b500\b|swift|\bibiza\b|fabia|"
                       r"mini cooper(?! clubman)(?! countryman)|renault 5|\b5 e tech\b|"
                       r"\bi10\b|\bsandero\b|\blogon\b|\bduster\b(?! suv)|"
                       r"\bseat mii\b|\bvw up\b|\bskoda citigo\b|"
                       r"\bsplash\b|\baltoz\b|\bpicanto\b|\bmorning\b|"
                       r"\btwingo\b(?! gt)|\bwind\b|\bclio\b|\bkangoo\b.*city|"
                       r"\bleaf\b|\be 208\b|\be-208\b|\bid.3\b|\bspring\b.*electr|"
                       r"\bka\b|\badam\b|\bagila\b|\b500e\b|\b600e\b|aveo|space star|"
                       r"\blogan\b(?! mcv)|"
                       r"\bmito\b|\bkarl\b|\bmeriva\b|c-elysee|c elysee|\bmini one\b"),
    ("MICRO_CITADINE", r"\bc1\b|\b107\b|\b108\b|aygo|twingo(?! gt)|\bup\b|e up|fortwo|forfour|"
                       r"pixo|ignis|panda|smart|\bi1\b"),
]

ELECTRIQUE_RE = (r"electri|e tech|e-tech|\bzoe\b|\bev\b|e up|e 2008|ioniq|"
                 r"tesla|model 3|model y|\be nv200\b")
HYBRIDE_RE = r"hybrid|iperformance|prius|niro|austral|\brxh\b"


def infer_segment(modele: str) -> str:
    """Dérive le segment à partir du libellé du modèle."""
    m = _strip_accents(str(modele)).lower()
    for seg, pattern in SEGMENT_RULES:
        if re.search(pattern, m):
            return seg
    return "NON_CLASSE"


def infer_energie(modele: str, type_connexion: str = "") -> str:
    """Dérive le type d'énergie à partir du libellé du modèle et du type de connexion."""
    m = _strip_accents(str(modele)).lower()
    if re.search(ELECTRIQUE_RE, m) or str(type_connexion).strip().lower() in ("électrique", "electrique"):
        return "ELECTRIQUE"
    if re.search(HYBRIDE_RE, m):
        return "HYBRIDE"
    return "THERMIQUE"


# ─────────────────────────────────────────────
# 2. SCORE RÉPUTATION
# ─────────────────────────────────────────────

def score_reputation(note, nb_avis) -> float:
    """
    Combine la note et le volume d'avis en un score 0-1.
    note ou nb_avis manquants → 0.0 (absence d'historique, jamais note neutre).
    """
    if nb_avis in (None, "", 0):
        return 0.0
    if note in (None, ""):
        return 0.0
    try:
        n = float(note)
        a = float(nb_avis)
        if a <= 0:
            return 0.0
        return round(min(1.0, (n / 5.0) * (math.log1p(a) / math.log1p(200))), 3)
    except (ValueError, TypeError):
        return 0.0


# ─────────────────────────────────────────────
# 3. POSITIONNEMENT CONCURRENTIEL (pool segment × connexion)
# ─────────────────────────────────────────────

def add_positioning(cars: list[dict], taille_min: int = 8) -> list[dict]:
    """
    Enrichit chaque véhicule avec :
      - segment, energie (si absents)
      - pool (segment × type_connexion, repli sur segment si < taille_min)
      - pool_n, pool_p25, pool_median, pool_p75
      - percentile_pool (0-100)
      - ecart_median_pool_pct
      - pool_fiable (bool)
      - score_reputation
    """
    # Inférer segment et energie
    for c in cars:
        if not c.get("segment"):
            c["segment"] = infer_segment(c.get("modele", ""))
        if not c.get("energie"):
            c["energie"] = infer_energie(c.get("modele", ""), c.get("type_connexion", ""))
        if "score_reputation" not in c:
            c["score_reputation"] = score_reputation(c.get("note"), c.get("nb_avis"))

    # Construire les pools
    def _pool_key(c, use_connexion=True):
        seg = c.get("segment", "NON_CLASSE")
        if use_connexion:
            conn = (c.get("type_connexion") or "INCONNU").strip()
            return f"{seg}|{conn}"
        return seg

    # Compter les pools avec connexion
    from collections import Counter
    pool_counts = Counter(_pool_key(c, True) for c in cars)

    for c in cars:
        pk = _pool_key(c, True)
        if pool_counts[pk] < taille_min:
            pk = _pool_key(c, False)  # repli sur segment seul
        c["_pool"] = pk

    # Calculer les stats par pool
    pool_prices = {}
    for c in cars:
        pk = c["_pool"]
        try:
            p = float(c.get("prix_jour", 0) or 0)
        except (ValueError, TypeError):
            p = 0
        if p > 0:
            pool_prices.setdefault(pk, []).append(p)

    pool_stats = {}
    for pk, prices in pool_prices.items():
        sp = sorted(prices)
        n = len(sp)
        pool_stats[pk] = {
            "n": n,
            "p25": sp[max(0, int(n * 0.25) - 1)],
            "median": sp[int(n * 0.50)],
            "p75": sp[min(n - 1, int(n * 0.75))],
        }

    # Enrichir chaque véhicule
    for c in cars:
        pk = c["_pool"]
        try:
            prix = float(c.get("prix_jour", 0) or 0)
        except (ValueError, TypeError):
            prix = 0

        stats = pool_stats.get(pk, {})
        n = stats.get("n", 0)
        med = stats.get("median")

        c["pool"] = pk
        c["pool_n"] = n
        c["pool_p25"] = stats.get("p25")
        c["pool_median"] = med
        c["pool_p75"] = stats.get("p75")
        c["pool_fiable"] = n >= taille_min

        if prix > 0 and n > 0:
            prices_in_pool = pool_prices.get(pk, [])
            below = sum(1 for p in prices_in_pool if p < prix)
            c["percentile_pool"] = round(below / len(prices_in_pool) * 100, 0)
            c["ecart_median_pool_pct"] = round((prix / med - 1) * 100, 1) if med else None
        else:
            c["percentile_pool"] = None
            c["ecart_median_pool_pct"] = None

    return cars


# ─────────────────────────────────────────────
# 4. CHARGEMENT
# ─────────────────────────────────────────────

def load_json(path) -> list[dict]:
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return []


def load_populations(pop_path: str) -> dict:
    with open(pop_path, encoding="utf-8") as f:
        data = json.load(f)
    return {k: v for k, v in data.items() if k != "_meta"}


def _normalize_city_key(name: str) -> str:
    s = _strip_accents(str(name)).lower().strip()
    s = re.sub(r"[^a-z0-9]", "-", s)
    return re.sub(r"-+", "-", s).strip("-")


def find_previous_json(output_dir: Path, current_file: Path, pipeline: str) -> list[dict]:
    """Trouve le JSON précédent du MÊME pipeline."""
    pattern = str(output_dir / f"getaround_*_{pipeline}_*.json")
    files = sorted(glob.glob(pattern))
    others = [f for f in files if f != str(current_file)]
    return load_json(Path(others[-1])) if others else []


# ─────────────────────────────────────────────
# 5. DELTA (même pipeline uniquement)
# ─────────────────────────────────────────────

def compute_delta(current: list[dict], previous: list[dict]) -> dict:
    """
    Compare deux snapshots et retourne les mouvements d'annonces.

    GARDE-FOU : lève ValueError si les deux snapshots ont des version_collecte
    différentes. Comparer un snapshot v3.1 (40 ann.) à un v3.2 (1 156 ann.)
    produirait 1 116 fausses apparitions puis autant de fausses disparitions.
    """
    # Extraire les versions
    v_curr = next((c.get("version_collecte", "") for c in current if c.get("version_collecte")), "")
    v_prev = next((c.get("version_collecte", "") for c in previous if c.get("version_collecte")), "")

    if previous:  # Ne vérifier que si un snapshot précédent est fourni
        if not v_curr or not v_prev:
            raise ValueError(
                f"compute_delta : version_collecte absente ou ambiguë — "
                f"current={v_curr!r}, previous={v_prev!r}. "
                f"Impossible de comparer des snapshots sans version identifiée."
            )
        if v_curr != v_prev:
            raise ValueError(
                f"compute_delta : version_collecte incompatible — "
                f"current={v_curr!r} vs previous={v_prev!r}. "
                f"Purger l'historique des snapshots {v_prev!r} avant de comparer."
            )

    curr_ids = {str(c.get("annonce_id", c.get("id", ""))) for c in current if c.get("annonce_id") or c.get("id")}
    prev_ids = {str(c.get("annonce_id", c.get("id", ""))) for c in previous if c.get("annonce_id") or c.get("id")}
    return {
        "version_collecte": v_curr,
        "apparus": len(curr_ids - prev_ids),
        "disparus": len(prev_ids - curr_ids),
        "stables": len(curr_ids & prev_ids),
        "total_current": len(curr_ids),
        "total_previous": len(prev_ids),
    }


# ─────────────────────────────────────────────
# 6. SYNTHÈSE PRINCIPALE
# ─────────────────────────────────────────────

def generate_synthesis(
    current_cars: list[dict],
    previous_cars: list[dict],
    owner_vehicles: list[dict],
    populations: Optional[dict] = None,
    pipeline: str = "A2",
    scrape_time: Optional[str] = None,
) -> str:
    """
    Génère la synthèse complète en Markdown.

    Règles :
    - commune_annonce est utilisée pour tout calcul géographique
    - Les snapshots sont comparés uniquement au sein du même pipeline
    - note et nb_avis vides → valeurs manquantes, jamais 0
    - pool_fiable = False → recommandation signalée comme non exploitable
    """
    # Charger populations si non fourni
    if populations is None:
        pop_path = Path(__file__).parent.parent / "references" / "populations.json"
        populations = load_populations(str(pop_path)) if pop_path.exists() else {}

    # Enrichir avec positionnement
    current_cars = add_positioning(current_cars)

    now_str = scrape_time or datetime.now().strftime("%d/%m/%Y à %H:%M")
    pipeline_labels = {
        "A1": "Recensement quotidien (A1)",
        "A2": "Détection réservation (A2)",
        "B":  "Pricing week-end (B)",
    }
    pipeline_label = pipeline_labels.get(pipeline, f"Pipeline {pipeline}")

    lines = []
    lines.append(f"# Synthèse Getaround — {pipeline_label}")
    lines.append(f"*Généré le {now_str}*")
    lines.append("")

    # ── 1. INVENTAIRE PAR VILLE ──────────────────────────────────────────
    lines.append("## 1. Inventaire du marché")

    # commune_annonce prioritaire, repli sur ville/commune_recherche
    def _get_commune(c):
        """
        commune_annonce est la SEULE source géographique valide.
        Aucun repli sur commune_recherche : celle-ci désigne la requête,
        pas la localisation du véhicule. Un échec d'extraction doit rester
        visible, pas être masqué par une valeur plausible.
        """
        return c.get("commune_annonce") or c.get("ville_annonce") or "INCONNU"

    nb_total = len(current_cars)
    ville_stats = {}
    for c in current_cars:
        v = _get_commune(c)
        try:
            p = float(c.get("prix_jour", 0) or 0)
        except (ValueError, TypeError):
            p = 0
        if v not in ville_stats:
            ville_stats[v] = {"count": 0, "prices": []}
        ville_stats[v]["count"] += 1
        if p > 0:
            ville_stats[v]["prices"].append(p)

    nb_communes = len(ville_stats)
    lines.append(
        f"**{nb_total} véhicules disponibles** sur **{nb_communes} communes** "
        f"des Hauts-de-Seine."
    )
    lines.append("")
    lines.append("| Commune | Habitants | Véhicules | Saturation (véh/10k hab) | Prix min | Prix moy | Prix max |")
    lines.append("|---|---|---|---|---|---|---|")

    for v, stats in sorted(ville_stats.items(), key=lambda x: -x[1]["count"]):
        n = stats["count"]
        prices = stats["prices"]
        key = _normalize_city_key(v)
        pop_data = populations.get(key, {})
        # Essai avec correspondance partielle si clé exacte absente
        if not pop_data:
            for pk, pv in populations.items():
                if pk in key or key in pk:
                    pop_data = pv
                    break
        pop = pop_data.get("population", 0) if pop_data else 0

        if pop and pop > 0:
            pop_str = f"{pop:,}".replace(",", "\u202f")
            saturation = round(n / pop * 10000, 1)
            sat_str = f"**{saturation}**" if saturation > 5 else str(saturation)
        else:
            pop_str = "—"
            sat_str = "—"

        if prices:
            p_min = f"{int(min(prices))}€"
            p_moy = f"{int(round(mean(prices)))}€"
            p_max = f"{int(max(prices))}€"
        else:
            p_min = p_moy = p_max = "—"

        lines.append(f"| {v} | {pop_str} | {n} | {sat_str} | {p_min} | {p_moy} | {p_max} |")

    lines.append("")

    # ── 2. DELTA (même pipeline) ─────────────────────────────────────────
    lines.append("## 2. Évolution depuis le dernier scraping")

    if not previous_cars:
        lines.append("*Aucun snapshot précédent disponible pour ce pipeline — delta non calculable.*")
    else:
        delta = compute_delta(current_cars, previous_cars)
        nb_prev = delta["total_previous"]
        diff = nb_total - nb_prev
        diff_str = f"+{diff}" if diff > 0 else str(diff)
        lines.append(
            f"Dernier snapshot ({pipeline_label}) : **{nb_prev} véhicules**. "
            f"Variation : **{diff_str}** ({nb_total} aujourd'hui)."
        )
        lines.append("")
        if delta["disparus"] > 0:
            lines.append(
                f"- **{delta['disparus']} véhicule(s) disparu(s)** "
                f"(potentiellement loués ou retirés de la plateforme)."
            )
        if delta["apparus"] > 0:
            lines.append(
                f"- **{delta['apparus']} nouveau(x) véhicule(s)** apparu(s) sur la plateforme."
            )
        if delta["disparus"] == 0 and delta["apparus"] == 0:
            lines.append("- L'inventaire est stable depuis le dernier snapshot.")

    lines.append("")

    # ── 3. POSITIONNEMENT DES VÉHICULES PROPRIÉTAIRES ───────────────────
    lines.append("## 3. Positionnement de votre flotte")

    owner_ids = {str(v.get("id_getaround") or v.get("annonce_id") or "").strip()
                 for v in owner_vehicles}
    owner_ids.discard("")

    owner_cars_in_scrape = [
        c for c in current_cars
        if str(c.get("annonce_id") or c.get("id") or "").strip() in owner_ids
    ]

    if not owner_vehicles:
        lines.append(
            "> ℹ Aucun véhicule propriétaire configuré. "
            "Renseignez `mes_vehicules.json` pour obtenir l'analyse de positionnement."
        )
    elif not owner_cars_in_scrape:
        lines.append(
            "> ⚠️ Aucun de vos véhicules n'a été trouvé dans ce snapshot. "
            "Vérifiez les `id_getaround` dans `mes_vehicules.json`."
        )
    else:
        for car in owner_cars_in_scrape:
            car_id = str(car.get("annonce_id") or car.get("id") or "")
            owner_info = next(
                (v for v in owner_vehicles
                 if str(v.get("id_getaround") or v.get("annonce_id") or "") == car_id),
                {}
            )
            modele = owner_info.get("modele") or car.get("modele") or f"Véhicule #{car_id}"
            immat = owner_info.get("immatriculation", "")
            label = f"{modele}" + (f" ({immat})" if immat else "")

            try:
                prix = float(car.get("prix_jour", 0) or 0)
            except (ValueError, TypeError):
                prix = 0

            segment = car.get("segment", "—")
            energie = car.get("energie", "—")
            pool = car.get("pool", "—")
            pool_n = car.get("pool_n", 0)
            pool_median = car.get("pool_median")
            pool_p25 = car.get("pool_p25")
            pool_p75 = car.get("pool_p75")
            pool_fiable = car.get("pool_fiable", False)
            percentile = car.get("percentile_pool")
            ecart = car.get("ecart_median_pool_pct")
            note = car.get("note")
            nb_avis = car.get("nb_avis")
            score_rep = car.get("score_reputation", score_reputation(note, nb_avis))

            lines.append(f"### {label}")
            lines.append(
                f"- **Prix actuel** : {int(prix)}€/jour | **Segment** : {segment} | **Énergie** : {energie}"
            )

            if note not in (None, "") and nb_avis not in (None, "", 0):
                lines.append(
                    f"- **Note** : {note}/5 ({int(float(nb_avis))} avis) | "
                    f"Score réputation : {score_rep:.3f}"
                )
            else:
                lines.append("- **Note** : données manquantes (pas de moyenne calculée)")

            if not pool_fiable:
                lines.append(
                    f"- ⚠️ **Pool insuffisant** ({pool_n} véhicules dans `{pool}`) — "
                    f"recommandation non exploitable (minimum 8 requis)."
                )
            else:
                lines.append(f"- **Pool de comparaison** : `{pool}` ({pool_n} véhicules)")
                lines.append(f"- **Médiane du pool** : {int(pool_median)}€/jour")
                if percentile is not None:
                    lines.append(f"- **Percentile** : {int(percentile)}e centile")

                if ecart is not None:
                    if ecart < -15:
                        icon, verdict = "📉", "EN DESSOUS DU MARCHÉ"
                    elif ecart > 15:
                        icon, verdict = "📈", "AU-DESSUS DU MARCHÉ"
                    else:
                        icon, verdict = "✅", "DANS LA MÉDIANE"
                    lines.append(
                        f"- **Positionnement** : {icon} {verdict} ({ecart:+.1f}% vs médiane)"
                    )

            lines.append("")

    # ── 4. RECOMMANDATIONS TARIFAIRES ───────────────────────────────────
    lines.append("## 4. Recommandations tarifaires")

    if not owner_cars_in_scrape:
        lines.append("*Aucun véhicule identifié — recommandations non disponibles.*")
    else:
        for car in owner_cars_in_scrape:
            car_id = str(car.get("annonce_id") or car.get("id") or "")
            owner_info = next(
                (v for v in owner_vehicles
                 if str(v.get("id_getaround") or v.get("annonce_id") or "") == car_id),
                {}
            )
            modele = owner_info.get("modele") or car.get("modele") or f"Véhicule #{car_id}"
            immat = owner_info.get("immatriculation", "")
            label = f"{modele}" + (f" ({immat})" if immat else "")

            try:
                prix = float(car.get("prix_jour", 0) or 0)
            except (ValueError, TypeError):
                prix = 0

            pool_fiable = car.get("pool_fiable", False)
            pool_median = car.get("pool_median")
            pool_p25 = car.get("pool_p25")
            pool_p75 = car.get("pool_p75")
            ecart = car.get("ecart_median_pool_pct")

            if not pool_fiable:
                lines.append(
                    f"- **{label}** : pool insuffisant — pas de recommandation exploitable."
                )
                continue

            if ecart is not None and prix > 0 and pool_median:
                if ecart < -15:
                    prix_conseille = round(pool_median)
                    gain_jour = prix_conseille - prix
                    gain_mois = gain_jour * 20
                    lines.append(
                        f"- **{label}** → **📈 Augmenter à {prix_conseille}€/jour** "
                        f"(+{gain_jour:.0f}€/jour, soit +{gain_mois:.0f}€/mois estimés sur 20 jours). "
                        f"Votre prix actuel ({int(prix)}€) est dans les 20% les moins chers du segment."
                    )
                elif ecart > 15:
                    prix_conseille = round(pool_p75) if pool_p75 else round(pool_median)
                    lines.append(
                        f"- **{label}** → **⚠️ Surveiller à {prix_conseille}€/jour** "
                        f"(3e quartile du segment). Votre prix actuel ({int(prix)}€) dépasse "
                        f"la médiane de {ecart:+.1f}%. Vérifiez que votre note et équipements "
                        f"justifient ce positionnement premium."
                    )
                else:
                    lines.append(
                        f"- **{label}** → **✅ Maintenir à {int(prix)}€/jour.** "
                        f"Prix aligné avec le marché. "
                        f"Fourchette du segment : {int(pool_p25)}€ – {int(pool_p75)}€/jour."
                    )
            else:
                lines.append(f"- **{label}** : données insuffisantes pour une recommandation.")

    lines.append("")

    # ── 5. CONTRÔLE QUALITÉ ──────────────────────────────────────────────
    lines.append("## 5. Contrôle qualité")
    lines.append("| Indicateur | Valeur | Seuil | Statut |")
    lines.append("|---|---|---|---|")

    nb_sans_prix = sum(
        1 for c in current_cars
        if not c.get("prix_jour") or float(c.get("prix_jour") or 0) == 0
    )
    nb_sans_id = sum(
        1 for c in current_cars
        if not c.get("annonce_id") and not c.get("id")
    )
    nb_non_classe = sum(1 for c in current_cars if c.get("segment") == "NON_CLASSE")
    pct_nc = round(nb_non_classe / nb_total * 100, 1) if nb_total > 0 else 0
    pct_sans_prix = round(nb_sans_prix / nb_total * 100, 1) if nb_total > 0 else 0

    lines.append(
        f"| Annonces sans prix | {nb_sans_prix} ({pct_sans_prix}%) | < 5% | "
        f"{'⚠️ ALERTE' if pct_sans_prix > 5 else '✅ OK'} |"
    )
    lines.append(
        f"| Annonces sans ID | {nb_sans_id} | 0 | "
        f"{'⚠️ ALERTE' if nb_sans_id > 0 else '✅ OK'} |"
    )
    lines.append(
        f"| Segments NON_CLASSE | {nb_non_classe} ({pct_nc}%) | < 2% | "
        f"{'⚠️ ALERTE — enrichir SEGMENT_RULES' if pct_nc > 2 else '✅ OK'} |"
    )

    lines.append("")
    lines.append("---")
    lines.append("*Rapport généré automatiquement par le skill getaround-scraper v2.1*")

    return "\n".join(lines)


# ─────────────────────────────────────────────
# 7. POINT D'ENTRÉE STANDALONE
# ─────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Analyser les résultats Getaround")
    parser.add_argument("--current", required=True, help="Fichier JSON du snapshot courant")
    parser.add_argument("--previous", default=None, help="Fichier JSON du snapshot précédent (même pipeline)")
    parser.add_argument("--vehicles", default=None, help="Fichier JSON des véhicules propriétaire")
    parser.add_argument("--populations", default=None, help="Fichier JSON des populations")
    parser.add_argument("--pipeline", default="A2", choices=["A1", "A2", "B"])
    args = parser.parse_args()

    current = load_json(args.current)
    previous = load_json(args.previous) if args.previous else []

    owner_vehicles = []
    if args.vehicles and Path(args.vehicles).exists():
        with open(args.vehicles, encoding="utf-8") as f:
            owner_vehicles = json.load(f)

    populations = None
    if args.populations and Path(args.populations).exists():
        populations = load_populations(args.populations)

    report = generate_synthesis(
        current_cars=current,
        previous_cars=previous,
        owner_vehicles=owner_vehicles,
        populations=populations,
        pipeline=args.pipeline,
        scrape_time=datetime.now().strftime("%d/%m/%Y à %H:%M"),
    )
    print(report)
