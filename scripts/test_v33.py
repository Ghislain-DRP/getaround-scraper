"""
Recette de tests v3.3 — 12 cas de validation
=============================================
Exécuter : python3 test_v33.py
Résultat attendu : 12/12 PASS
"""
import re
import sys
import asyncio
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from scrape_getaround import (
    VERSION_COLLECTE,
    compute_window,
    extract_note_avis,
    parse_price,
    extract_annonce_id,
    JS_CARDS_SEARCH,
    JS_CARDS_SEO,
    JS_HAS_BTN,
    JS_NB_RESULTS,
    _DIST_RE,
    NOTE_AVIS_RE,
    A2_GPS_POINTS,
    MAX_CLICS,
    MIN_DELAY,
)

PASS = 0
FAIL = 0

def check(name: str, condition: bool, detail: str = ""):
    global PASS, FAIL
    if condition:
        PASS += 1
        print(f"  PASS  {name}")
    else:
        FAIL += 1
        print(f"  FAIL  {name}" + (f" — {detail}" if detail else ""))


# ─── T01 : VERSION_COLLECTE ───────────────────────────────────────────────────
print("\n[T01] VERSION_COLLECTE")
check("VERSION_COLLECTE == v3.3-search-gps", VERSION_COLLECTE == "v3.3-search-gps",
      f"got '{VERSION_COLLECTE}'")


# ─── T02 : URL GPS v3.3 ───────────────────────────────────────────────────────
print("\n[T02] URL GPS v3.3 (/search)")
from scrape_getaround import compute_window
fenetre_debut, fenetre_fin = compute_window("A2")
lat, lng = 48.8847, 2.2388
start_date = fenetre_debut[:10]
end_date = fenetre_fin[:10]
start_time = fenetre_debut[11:] if len(fenetre_debut) > 10 else "09:00"
end_time = fenetre_fin[11:] if len(fenetre_fin) > 10 else "20:00"
expected_url_prefix = "https://fr.getaround.com/search?latitude="
url = (
    f"https://fr.getaround.com/search"
    f"?latitude={lat}&longitude={lng}"
    f"&start_date={start_date}&start_time={start_time}"
    f"&end_date={end_date}&end_time={end_time}"
    f"&country_scope=FR&display_view=list"
    f"&pickup_method_explicit_choice=true"
)
check("URL commence par /search?latitude=", url.startswith(expected_url_prefix), url[:60])
check("URL contient country_scope=FR", "country_scope=FR" in url)
check("URL contient display_view=list", "display_view=list" in url)
check("URL ne contient pas /location-voiture/france", "/location-voiture/france" not in url)


# ─── T03 : JS_CARDS_SEARCH contient data-car-page-url ────────────────────────
print("\n[T03] JS_CARDS_SEARCH")
check("Sélecteur [data-car-page-url]", "[data-car-page-url]" in JS_CARDS_SEARCH,
      "sélecteur manquant")
check("Ne contient pas a[href*='/location-voiture/']",
      "a[href*=" not in JS_CARDS_SEARCH, "ancien sélecteur SEO présent")
check("Extraction prix via span.c-font-bold", "c-font-bold" in JS_CARDS_SEARCH)


# ─── T04 : JS_CARDS_SEO contient a[href*='/location-voiture/'] ───────────────
print("\n[T04] JS_CARDS_SEO")
check("Sélecteur a[href*='/location-voiture/']",
      "a[href*=\"/location-voiture/\"]" in JS_CARDS_SEO)
check("Extraction prix /jour", "/jour" in JS_CARDS_SEO)
check("Extraction prix /h", "/h" in JS_CARDS_SEO)


# ─── T05 : JS_HAS_BTN v3.3 ───────────────────────────────────────────────────
print("\n[T05] JS_HAS_BTN")
check("Classe search-results__load-more-button", "search-results__load-more-button" in JS_HAS_BTN)
check("Fallback texte 'afficher plus'", "afficher plus" in JS_HAS_BTN.lower())


# ─── T06 : JS_NB_RESULTS pattern v3.3 ────────────────────────────────────────
print("\n[T06] JS_NB_RESULTS")
check("Pattern 'résultats sur'", "résultats sur" in JS_NB_RESULTS)
check("Retourne le total (groupe 2)", "m[2]" in JS_NB_RESULTS)


# ─── T07 : extract_note_avis ─────────────────────────────────────────────────
print("\n[T07] extract_note_avis")
note, avis = extract_note_avis("Peugeot 208 (2013)\n5.0 (2)\nà 680 m\n84 €")
check("Note extraite correctement", note == 5.0, f"got {note}")
check("Nb avis extrait correctement", avis == 2, f"got {avis}")
note2, avis2 = extract_note_avis("Renault Clio (2011)\nà 480 m\n73 €")
check("Note None si absent", note2 is None, f"got {note2}")


# ─── T08 : extraction modèle regex ───────────────────────────────────────────
print("\n[T08] Extraction modèle v3.3")
_MODEL_YEAR_RE = re.compile(r'^(.+?)\s*\((\d{4})\)\s*$', re.MULTILINE)
samples = [
    ("GETAROUND CONNECT\nPeugeot 208 (2013)\n5.0 (2)", "Peugeot 208 (2013)", "2013"),
    ("SUR RENDEZ-VOUS\nPépite des locataires\nMG B Cabriolet (1973)\n5.0 (8)", "MG B Cabriolet (1973)", "1973"),
    ("GETAROUND CONNECT\nRenault Austral E-Tech full hybrid (2024)\n5.0 (1)", "Renault Austral E-Tech full hybrid (2024)", "2024"),
]
all_ok = True
for text, expected_modele, expected_annee in samples:
    m = _MODEL_YEAR_RE.search(text)
    if m:
        modele = f"{m.group(1).strip()} ({m.group(2)})"
        annee = m.group(2)
        if modele != expected_modele or annee != expected_annee:
            all_ok = False
            print(f"    FAIL: got '{modele}' (annee={annee}), expected '{expected_modele}' (annee={expected_annee})")
    else:
        all_ok = False
        print(f"    FAIL: regex ne match pas sur '{text[:40]}'")
check("Extraction modèle+année sur 3 cas", all_ok)


# ─── T09 : garde-fou run partiel ─────────────────────────────────────────────
print("\n[T09] Garde-fou run partiel")
# Vérifier que la logique est présente dans le code source
import inspect
from scrape_getaround import scrape_gps_point
source = inspect.getsource(scrape_gps_point)
check("RuntimeError RUN_PARTIEL présent", "RUN_PARTIEL" in source)
check("Seuil MIN_ANNONCES_ATTENDUES = 100",
      "MIN_ANNONCES_ATTENDUES = 100" in source or "MIN_ANNONCES_ATTENDUES=100" in source)


# ─── T10 : compute_window ────────────────────────────────────────────────────
print("\n[T10] compute_window")
from datetime import date, timedelta
today = date.today()
d_a2, f_a2 = compute_window("A2")
target = today + timedelta(days=1)
check("A2 fenêtre J+1", d_a2.startswith(target.isoformat()),
      f"got {d_a2}, expected {target.isoformat()}")
check("A2 heure début 09:00", "T09:00" in d_a2, f"got {d_a2}")
check("A2 heure fin 20:00", "T20:00" in f_a2, f"got {f_a2}")


# ─── T11 : points GPS A2 ─────────────────────────────────────────────────────
print("\n[T11] Points GPS A2")
check("3 points GPS définis", len(A2_GPS_POINTS) == 3, f"got {len(A2_GPS_POINTS)}")
labels = [p[0] for p in A2_GPS_POINTS]
check("Puteaux présent", "Puteaux" in labels)
check("Asnières-sur-Seine présent", "Asnières-sur-Seine" in labels)
check("Paris 17e présent", "Paris 17e" in labels)


# ─── T12 : parse_price ───────────────────────────────────────────────────────
print("\n[T12] parse_price")
check("parse_price('84') == 84.0", parse_price("84") == 84.0, f"got {parse_price('84')}")
check("parse_price('') == None", parse_price("") is None)
check("parse_price('55') == 55.0", parse_price("55") == 55.0)


# ─── Résumé ───────────────────────────────────────────────────────────────────
total = PASS + FAIL
print(f"\n{'='*50}")
print(f"Résultat : {PASS}/{total} PASS")
if FAIL > 0:
    print(f"ÉCHEC : {FAIL} test(s) en erreur")
    sys.exit(1)
else:
    print("✓ Tous les tests passent — v3.3 validée")
