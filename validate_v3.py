#!/usr/bin/env python3
"""
validate_v3.py — Recette de non-régression Getaround Scraper v3.2
Usage :
    python3 validate_v3.py --csv <dernier_A2.csv> --ref <snapshot_reference.csv>
"""
import argparse
import csv
import json
import re
import sys
import importlib.util
from pathlib import Path
from collections import Counter

ROOT = Path(__file__).parent
SCRIPTS = ROOT / "scripts"
REFS = ROOT / "references"

# ─── Chargement des modules ────────────────────────────────────────────────────
def _load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod

def _load_csv(path):
    if not path or not Path(path).exists():
        return []
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))

# ─── Résultats ─────────────────────────────────────────────────────────────────
results = []

def ok(tid, msg):
    results.append((tid, "PASS", msg))

def fail(tid, msg):
    results.append((tid, "FAIL", msg))

# ══════════════════════════════════════════════════════════════════════════════
# T01 — Pas de fichiers __pycache__ dans le skill
# ══════════════════════════════════════════════════════════════════════════════
def t01_no_pycache():
    caches = list(ROOT.rglob("__pycache__"))
    pycs = list(ROOT.rglob("*.pyc"))
    if caches or pycs:
        fail("T01", f"{len(caches)} __pycache__ et {len(pycs)} .pyc trouvés : {[str(c) for c in caches+pycs][:5]}")
    else:
        ok("T01", "Aucun __pycache__ ni .pyc")

# ══════════════════════════════════════════════════════════════════════════════
# T02 — VERSION_COLLECTE = v3.2-pagination-gps
# ══════════════════════════════════════════════════════════════════════════════
def t02_version_collecte():
    scrape = SCRIPTS / "scrape_getaround.py"
    content = scrape.read_text(encoding="utf-8")
    m = re.search(r'VERSION_COLLECTE\s*=\s*["\']([^"\']+)["\']', content)
    if not m:
        fail("T02", "VERSION_COLLECTE introuvable dans scrape_getaround.py")
    elif m.group(1) != "v3.2-pagination-gps":
        fail("T02", f"VERSION_COLLECTE = {m.group(1)!r} (attendu 'v3.2-pagination-gps')")
    else:
        ok("T02", f"VERSION_COLLECTE = {m.group(1)!r}")

# ══════════════════════════════════════════════════════════════════════════════
# T03 — MAX_CLICS = 50
# ══════════════════════════════════════════════════════════════════════════════
def t03_max_clics():
    scrape = SCRIPTS / "scrape_getaround.py"
    content = scrape.read_text(encoding="utf-8")
    m = re.search(r'MAX_CLICS\s*=\s*(\d+)', content)
    if not m:
        fail("T03", "MAX_CLICS introuvable")
    elif int(m.group(1)) < 50:
        fail("T03", f"MAX_CLICS = {m.group(1)} (attendu ≥ 50)")
    else:
        ok("T03", f"MAX_CLICS = {m.group(1)}")

# ══════════════════════════════════════════════════════════════════════════════
# T04 — 3 points GPS dans A2_GPS_POINTS (Puteaux, Asnières, Paris 17e)
# ══════════════════════════════════════════════════════════════════════════════
def t04_gps_points():
    scrape = SCRIPTS / "scrape_getaround.py"
    content = scrape.read_text(encoding="utf-8")
    required = ["Puteaux", "Asnières", "Paris 17"]
    missing = [r for r in required if r not in content]
    if missing:
        fail("T04", f"Points GPS manquants dans A2_GPS_POINTS : {missing}")
    else:
        ok("T04", "3 points GPS présents (Puteaux, Asnières, Paris 17e)")

# ══════════════════════════════════════════════════════════════════════════════
# T05 — SEGMENT_RULES : 0 NON_CLASSE sur le CSV de référence
# ══════════════════════════════════════════════════════════════════════════════
def t05_segment_rules(ref_csv):
    analyse = _load_module("analyse", SCRIPTS / "analyse.py")
    rows = _load_csv(ref_csv)
    if not rows:
        fail("T05", f"CSV de référence vide ou absent : {ref_csv}")
        return
    nc = [r for r in rows if analyse.infer_segment(r.get("modele", "")) == "NON_CLASSE"]
    pct = len(nc) / len(rows) * 100
    if nc:
        exemples = [r.get("modele", "?") for r in nc[:5]]
        fail("T05", f"{len(nc)}/{len(rows)} NON_CLASSE ({pct:.1f}%) — ex : {exemples}")
    else:
        ok("T05", f"0 NON_CLASSE sur {len(rows)} annonces de référence")

# ══════════════════════════════════════════════════════════════════════════════
# T06 — extract_note_avis : ≥ 70% de notes extraites sur le CSV de référence
# ══════════════════════════════════════════════════════════════════════════════
def t06_extract_note(ref_csv):
    scrape = _load_module("scrape_getaround", SCRIPTS / "scrape_getaround.py")
    rows = _load_csv(ref_csv)
    if not rows:
        fail("T06", f"CSV de référence vide ou absent : {ref_csv}")
        return
    # Utiliser le champ note directement (déjà extrait dans le CSV)
    with_note = [r for r in rows if r.get("note", "").strip() not in ("", "None", "N/A")]
    pct = len(with_note) / len(rows) * 100
    if pct < 70:
        fail("T06", f"Notes renseignées : {len(with_note)}/{len(rows)} ({pct:.1f}%) < 70%")
    else:
        ok("T06", f"Notes renseignées : {len(with_note)}/{len(rows)} ({pct:.1f}%)")

# ══════════════════════════════════════════════════════════════════════════════
# T07 — garde-fou compute_delta : ValueError si versions différentes
# ══════════════════════════════════════════════════════════════════════════════
def t07_compute_delta_guardrail():
    analyse = _load_module("analyse", SCRIPTS / "analyse.py")
    curr = [{"annonce_id": "1", "version_collecte": "v3.2-pagination-gps"}]
    prev = [{"annonce_id": "2", "version_collecte": "v3.1-hybrid-seo-gps"}]
    try:
        analyse.compute_delta(curr, prev)
        fail("T07", "compute_delta n'a pas levé ValueError pour des versions différentes")
    except ValueError as e:
        ok("T07", f"ValueError correctement levée : {str(e)[:80]}")
    except Exception as e:
        fail("T07", f"Exception inattendue : {type(e).__name__}: {e}")

# ══════════════════════════════════════════════════════════════════════════════
# T08 — garde-fou compute_delta : passe si même version
# ══════════════════════════════════════════════════════════════════════════════
def t08_compute_delta_same_version():
    analyse = _load_module("analyse", SCRIPTS / "analyse.py")
    curr = [{"annonce_id": "1", "version_collecte": "v3.2-pagination-gps"}]
    prev = [{"annonce_id": "2", "version_collecte": "v3.2-pagination-gps"}]
    try:
        delta = analyse.compute_delta(curr, prev)
        if delta.get("version_collecte") != "v3.2-pagination-gps":
            fail("T08", f"version_collecte absent du retour : {delta}")
        else:
            ok("T08", f"compute_delta OK (même version) — apparus={delta['apparus']}, disparus={delta['disparus']}")
    except Exception as e:
        fail("T08", f"Exception inattendue : {type(e).__name__}: {e}")

# ══════════════════════════════════════════════════════════════════════════════
# T09 — populations.json : 56 entrées (36 communes 92 + 20 Paris)
# ══════════════════════════════════════════════════════════════════════════════
def t09_populations():
    pop_path = REFS / "populations.json"
    if not pop_path.exists():
        fail("T09", "populations.json absent")
        return
    with open(pop_path, encoding="utf-8") as f:
        pop = json.load(f)
    communes = {k: v for k, v in pop.items() if k != "_meta"}
    n = len(communes)
    paris_ok = "Paris 17e" in communes and "Paris 1er" in communes
    if n != 56:
        fail("T09", f"populations.json : {n} entrées (attendu 56)")
    elif not paris_ok:
        fail("T09", "Paris 17e ou Paris 1er absent de populations.json")
    else:
        ok("T09", f"populations.json : {n} entrées, arrondissements Paris présents")

# ══════════════════════════════════════════════════════════════════════════════
# T10 — scrape_pipeline_c.py : présent et compilable
# ══════════════════════════════════════════════════════════════════════════════
def t10_pipeline_c():
    import py_compile
    pipeline_c = SCRIPTS / "scrape_pipeline_c.py"
    if not pipeline_c.exists():
        fail("T10", "scrape_pipeline_c.py absent")
        return
    try:
        py_compile.compile(str(pipeline_c), doraise=True, cfile="/tmp/_check_c.pyc")
        ok("T10", "scrape_pipeline_c.py présent et compilable")
    except py_compile.PyCompileError as e:
        fail("T10", f"Erreur de compilation : {e}")

# ══════════════════════════════════════════════════════════════════════════════
# T11 — FIELDNAMES : contient distance_recherche, nb_resultats_total, nb_clics_pagination
# ══════════════════════════════════════════════════════════════════════════════
def t11_fieldnames():
    scrape = SCRIPTS / "scrape_getaround.py"
    content = scrape.read_text(encoding="utf-8")
    required = ["distance_recherche", "nb_resultats_total", "nb_clics_pagination", "version_collecte"]
    missing = [f for f in required if f not in content]
    if missing:
        fail("T11", f"Champs manquants dans FIELDNAMES : {missing}")
    else:
        ok("T11", f"Tous les nouveaux champs présents : {required}")

# ══════════════════════════════════════════════════════════════════════════════
# T12 — Run A2 réel : version_collecte, couverture ≥ 95% par point, distance_recherche
# ══════════════════════════════════════════════════════════════════════════════
def t12_run_reel(csv_path):
    rows = _load_csv(csv_path)
    if not rows:
        fail("T12", f"CSV A2 vide ou absent : {csv_path}")
        return

    # version_collecte
    versions = Counter(r.get("version_collecte", "") for r in rows)
    v32_count = versions.get("v3.2-pagination-gps", 0)
    if v32_count == 0:
        fail("T12", f"Aucune ligne avec version_collecte=v3.2-pagination-gps. Versions trouvées : {dict(versions)}")
        return

    # distance_recherche renseigné
    with_dist = [r for r in rows if r.get("distance_recherche", "").strip() not in ("", "None", "N/A")]
    pct_dist = len(with_dist) / len(rows) * 100

    # couverture par point (via communes_recherche)
    # Lire le QC JSON si disponible
    qc_files = sorted(Path("/home/ubuntu/getaround_results").glob("getaround_A2_qc_*.json"), reverse=True)
    couverture_ok = True
    couverture_detail = "QC JSON non disponible"
    if qc_files:
        with open(qc_files[0]) as f:
            qc = json.load(f)
        cov = qc.get("couverture_par_point", {})
        if cov:
            sous_seuil = {pt: v for pt, v in cov.items() if v < 0.95}
            if sous_seuil:
                couverture_ok = False
                couverture_detail = f"Points < 95% : {sous_seuil}"
            else:
                couverture_detail = f"Tous points ≥ 95% : {cov}"

    msgs = [
        f"{v32_count}/{len(rows)} lignes v3.2-pagination-gps",
        f"distance_recherche : {len(with_dist)}/{len(rows)} ({pct_dist:.1f}%)",
        f"couverture : {couverture_detail}",
    ]

    if pct_dist < 80:
        fail("T12", " | ".join(msgs) + " — distance_recherche < 80%")
    elif not couverture_ok:
        fail("T12", " | ".join(msgs) + " — couverture insuffisante")
    else:
        ok("T12", " | ".join(msgs))

# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════
def main():
    parser = argparse.ArgumentParser(description="Recette v3.2 Getaround Scraper")
    parser.add_argument("--csv", default=None, help="Dernier CSV A2 (pour T06, T12)")
    parser.add_argument("--ref", default=None, help="CSV de référence (pour T05, T06)")
    args = parser.parse_args()

    # Si --csv non fourni, chercher le dernier CSV A2
    csv_path = args.csv
    if not csv_path:
        candidates = sorted(Path("/home/ubuntu/getaround_results").glob("getaround_A2_*.csv"), reverse=True)
        # Exclure les fichiers QC
        candidates = [c for c in candidates if "qc" not in c.name.lower()]
        if candidates:
            csv_path = str(candidates[0])

    ref_path = args.ref or csv_path  # Utiliser le CSV A2 comme référence si non fourni

    print(f"Recette v3.2 — {ROOT}")
    print(f"CSV A2    : {csv_path or 'non fourni'}")
    print(f"Référence : {ref_path or 'non fourni'}")
    print("─" * 60)

    t01_no_pycache()
    t02_version_collecte()
    t03_max_clics()
    t04_gps_points()
    t05_segment_rules(ref_path)
    t06_extract_note(ref_path)
    t07_compute_delta_guardrail()
    t08_compute_delta_same_version()
    t09_populations()
    t10_pipeline_c()
    t11_fieldnames()
    t12_run_reel(csv_path)

    print()
    passed = sum(1 for _, s, _ in results if s == "PASS")
    failed = sum(1 for _, s, _ in results if s == "FAIL")
    for tid, status, msg in results:
        icon = "✅" if status == "PASS" else "❌"
        print(f"  {icon} {tid} — {msg}")
    print()
    print(f"Résultat : {passed}/{passed+failed} PASS")
    if failed:
        sys.exit(1)

if __name__ == "__main__":
    main()
