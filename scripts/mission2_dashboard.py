"""
Getaround Scraper v2 — Mission 2 : Paramètres tarifaires (dashboard authentifié)
==================================================================================

Objectif : Collecter les paramètres tarifaires et conditions de chaque véhicule
depuis le dashboard Getaround, pour les 37 véhicules répartis sur 3 comptes.

RÈGLES NON NÉGOCIABLES :
  - Lecture seule : aucune modification d'annonce, de prix ou de statut
  - Ne jamais inventer une valeur (champ absent = vide)
  - Signaler les échecs dans le rapport QC
  - Pour les 3 annonces bloquées (FC665WH, FR187TS, BR504ZJ) :
    cliquer sur "Débloquer mon annonce" et relever les documents demandés
    sans rien soumettre ni téléverser

Livrable : parametres_flotte_<date>.csv — 37 lignes attendues

Usage :
    python mission2_dashboard.py
    python mission2_dashboard.py --output-dir /home/ubuntu/getaround_results
    python mission2_dashboard.py --dry-run  (affiche les URLs sans scraper)

Prérequis :
    - Les 3 comptes doivent être connectés dans le navigateur Chromium du sandbox
    - Ou passer les credentials via variables d'environnement (non recommandé)
"""

import asyncio
import json
import csv
import re
import os
import argparse
from datetime import datetime, date
from pathlib import Path
from playwright.async_api import async_playwright, Page

DEFAULT_OUTPUT_DIR = Path("/home/ubuntu/getaround_results")
DEFAULT_VEHICLES_FILE = Path(__file__).parent.parent / "references" / "mes_vehicules.json"
MIN_DELAY = 2.5  # secondes entre requêtes (respecter la plateforme)

# Véhicules bloqués — relever les documents requis sans rien soumettre
VEHICULES_BLOQUES = {"FC665WH", "FR187TS", "BR504ZJ"}

# Champs du CSV de sortie
FIELDNAMES_M2 = [
    "date_collecte",
    "annonce_id", "immatriculation", "modele", "annee", "statut",
    "nb_locations_cumulees", "note", "nb_avis",
    "adresse_stationnement",
    "prix_base_jour",
    "prix_intelligents_actif",
    "prix_minimum",
    "reduction_2j", "reduction_7j", "reduction_30j",
    "prix_weekend_specifique",
    "delai_reservation",
    "duree_min", "duree_max",
    "horaires_echange_cles",
    "km_inclus_par_jour",
    "livraison_activee", "prix_livraison",
    "proprietaire",
    "erreur",
    "documents_requis_si_bloque",
]


def empty_record(vehicule: dict, date_collecte: str) -> dict:
    """Retourne un enregistrement vide (champs absents = vide, pas de valeur par défaut)."""
    return {
        "date_collecte": date_collecte,
        "annonce_id": vehicule.get("id_getaround", ""),
        "immatriculation": vehicule.get("immatriculation", ""),
        "modele": vehicule.get("modele", ""),
        "annee": vehicule.get("annee", ""),
        "statut": "",
        "nb_locations_cumulees": "",
        "note": "",
        "nb_avis": "",
        "adresse_stationnement": "",
        "prix_base_jour": "",
        "prix_intelligents_actif": "",
        "prix_minimum": "",
        "reduction_2j": "",
        "reduction_7j": "",
        "reduction_30j": "",
        "prix_weekend_specifique": "",
        "delai_reservation": "",
        "duree_min": "",
        "duree_max": "",
        "horaires_echange_cles": "",
        "km_inclus_par_jour": "",
        "livraison_activee": "",
        "prix_livraison": "",
        "proprietaire": vehicule.get("proprietaire", ""),
        "erreur": "",
        "documents_requis_si_bloque": "",
    }


def parse_price(text: str):
    """Retourne un float ou vide string (jamais de valeur par défaut)."""
    if not text:
        return ""
    m = re.search(r"(\d+[.,]?\d*)", text.replace("\u202f", "").replace("\xa0", ""))
    return float(m.group(1).replace(",", ".")) if m else ""


def parse_int(text: str):
    if not text:
        return ""
    m = re.search(r"(\d+)", text)
    return int(m.group(1)) if m else ""


async def scrape_onglet_general(page: Page, annonce_id: str, record: dict):
    """Extrait les données de l'onglet Général du dashboard."""
    url = f"https://fr.getaround.com/dashboard/cars/{annonce_id}"
    await page.goto(url, wait_until="domcontentloaded", timeout=30000)
    await asyncio.sleep(MIN_DELAY)

    content = await page.content()

    # Statut
    for badge in ["Active", "Bloquée", "En pause", "Incomplète", "En attente", "Prête à être publiée"]:
        if badge in content:
            record["statut"] = badge
            break

    # Nb locations cumulées
    m = re.search(r"(\d+)\s*location", content, re.IGNORECASE)
    if m:
        record["nb_locations_cumulees"] = int(m.group(1))

    # Note
    m = re.search(r"(\d+[.,]\d+)\s*/\s*5", content)
    if m:
        record["note"] = float(m.group(1).replace(",", "."))

    # Nb avis
    m = re.search(r"(\d+)\s*avis", content, re.IGNORECASE)
    if m:
        record["nb_avis"] = int(m.group(1))

    # Adresse de stationnement
    m = re.search(r"(\d+\s+[^<\n]{5,60}(?:92\d{3})[^<\n]{0,40})", content)
    if m:
        record["adresse_stationnement"] = m.group(1).strip()

    # Vérifier si annonce bloquée et récupérer les documents requis
    if "Débloquer mon annonce" in content or "débloquer" in content.lower():
        # Chercher le bouton et cliquer pour voir les documents
        try:
            btn = await page.query_selector("text=Débloquer mon annonce")
            if btn:
                await btn.click()
                await asyncio.sleep(2)
                modal_content = await page.content()
                # Extraire la liste des documents demandés
                docs = re.findall(r"(?:document|pièce|justificatif)[^<\n]{0,200}", modal_content, re.IGNORECASE)
                if docs:
                    record["documents_requis_si_bloque"] = " | ".join(docs[:5])
                # Fermer la modal sans rien soumettre
                esc_btn = await page.query_selector("[aria-label='Close'], button[type='button']:has-text('Fermer')")
                if esc_btn:
                    await esc_btn.click()
                else:
                    await page.keyboard.press("Escape")
        except Exception as e:
            record["documents_requis_si_bloque"] = f"Erreur lecture modal : {e}"


async def scrape_onglet_prix(page: Page, annonce_id: str, record: dict):
    """Extrait les données de l'onglet Prix du dashboard."""
    url = f"https://fr.getaround.com/dashboard/cars/{annonce_id}/pricing"
    await page.goto(url, wait_until="domcontentloaded", timeout=30000)
    await asyncio.sleep(MIN_DELAY)

    content = await page.content()

    # Prix de base/jour
    m = re.search(r"prix[^<]{0,50}(\d+)\s*€\s*/\s*jour", content, re.IGNORECASE)
    if not m:
        m = re.search(r"(\d+)\s*€\s*/\s*jour", content)
    if m:
        record["prix_base_jour"] = float(m.group(1))

    # Prix intelligents
    record["prix_intelligents_actif"] = (
        "prix intelligents" in content.lower() and "activé" in content.lower()
    )

    # Prix minimum
    m = re.search(r"prix\s+minimum[^<]{0,50}(\d+)\s*€", content, re.IGNORECASE)
    if m:
        record["prix_minimum"] = float(m.group(1))

    # Réductions
    for jours, key in [(2, "reduction_2j"), (7, "reduction_7j"), (30, "reduction_30j")]:
        m = re.search(rf"{jours}\s*jour[^<]{{0,50}}(\d+)\s*%", content, re.IGNORECASE)
        if not m:
            m = re.search(rf"(\d+)\s*%[^<]{{0,50}}{jours}\s*jour", content, re.IGNORECASE)
        if m:
            record[key] = int(m.group(1))

    # Prix week-end spécifique
    m = re.search(r"week.end[^<]{0,50}(\d+)\s*€", content, re.IGNORECASE)
    if m:
        record["prix_weekend_specifique"] = float(m.group(1))

    # Km inclus par jour
    m = re.search(r"(\d+)\s*km[^<]{0,30}jour", content, re.IGNORECASE)
    if not m:
        m = re.search(r"km[^<]{0,30}(\d+)", content, re.IGNORECASE)
    if m:
        record["km_inclus_par_jour"] = int(m.group(1))


async def scrape_onglet_conditions(page: Page, annonce_id: str, record: dict):
    """Extrait les données de l'onglet Mes conditions du dashboard."""
    url = f"https://fr.getaround.com/dashboard/cars/{annonce_id}/conditions"
    await page.goto(url, wait_until="domcontentloaded", timeout=30000)
    await asyncio.sleep(MIN_DELAY)

    content = await page.content()

    # Délai de réservation
    for delai in ["dans l'heure", "1 heure", "2 heures", "3 heures", "6 heures",
                  "12 heures", "24 heures", "48 heures", "3 jours", "7 jours"]:
        if delai.lower() in content.lower():
            record["delai_reservation"] = delai
            break

    # Durée min/max
    m = re.search(r"durée\s+minimum[^<]{0,50}(\d+)\s*(heure|jour)", content, re.IGNORECASE)
    if m:
        record["duree_min"] = f"{m.group(1)} {m.group(2)}"

    m = re.search(r"durée\s+maximum[^<]{0,50}(\d+)\s*(heure|jour|semaine|mois)", content, re.IGNORECASE)
    if m:
        record["duree_max"] = f"{m.group(1)} {m.group(2)}"

    # Horaires échange de clés
    m = re.search(r"(\d{1,2}h\d{0,2})\s*[-–]\s*(\d{1,2}h\d{0,2})", content)
    if m:
        record["horaires_echange_cles"] = f"{m.group(1)}-{m.group(2)}"


async def scrape_onglet_livraison(page: Page, annonce_id: str, record: dict):
    """Extrait les données de l'onglet Adresse et livraison du dashboard."""
    url = f"https://fr.getaround.com/dashboard/cars/{annonce_id}/delivery"
    await page.goto(url, wait_until="domcontentloaded", timeout=30000)
    await asyncio.sleep(MIN_DELAY)

    content = await page.content()

    # Livraison activée
    record["livraison_activee"] = "livraison" in content.lower() and (
        "activée" in content.lower() or "disponible" in content.lower()
    )

    # Prix livraison
    m = re.search(r"livraison[^<]{0,50}(\d+)\s*€", content, re.IGNORECASE)
    if not m:
        m = re.search(r"(\d+)\s*€[^<]{0,30}livraison", content, re.IGNORECASE)
    if m:
        record["prix_livraison"] = float(m.group(1))

    # Adresse si non trouvée dans onglet général
    if not record.get("adresse_stationnement"):
        m = re.search(r"(\d+\s+[^<\n]{5,60}(?:92\d{3})[^<\n]{0,40})", content)
        if m:
            record["adresse_stationnement"] = m.group(1).strip()


async def scrape_vehicule(page: Page, vehicule: dict, date_collecte: str) -> dict:
    """Scrape les 4 onglets du dashboard pour un véhicule."""
    annonce_id = str(vehicule.get("id_getaround", ""))
    immat = vehicule.get("immatriculation", "")
    record = empty_record(vehicule, date_collecte)

    if not annonce_id:
        record["erreur"] = "ID Getaround manquant"
        return record

    print(f"  [{immat}] ID {annonce_id}...", end=" ", flush=True)

    try:
        await scrape_onglet_general(page, annonce_id, record)
        await scrape_onglet_prix(page, annonce_id, record)
        await scrape_onglet_conditions(page, annonce_id, record)
        await scrape_onglet_livraison(page, annonce_id, record)
        print(f"OK — {record.get('statut', '?')} — {record.get('prix_base_jour', '?')}€/j")
    except Exception as e:
        record["erreur"] = str(e)
        print(f"ERREUR : {e}")

    return record


async def run_mission2(output_dir: Path, vehicles_file: Path, dry_run: bool = False):
    """Exécute la Mission 2 sur les 3 comptes."""

    date_collecte = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    date_str = datetime.now().strftime("%Y%m%d")

    # Charger les véhicules
    if not vehicles_file.exists():
        print(f"ERREUR : Fichier véhicules introuvable : {vehicles_file}")
        return

    vehicules = json.load(open(vehicles_file, encoding="utf-8"))
    print(f"\n{'='*60}")
    print(f"Mission 2 — Paramètres tarifaires dashboard")
    print(f"Date : {date_collecte}")
    print(f"Véhicules : {len(vehicules)}")
    print(f"{'='*60}")

    if dry_run:
        print("\nMode DRY-RUN — URLs qui seraient visitées :")
        for v in vehicules:
            aid = v.get("id_getaround", "?")
            immat = v.get("immatriculation", "?")
            print(f"  {immat} (ID {aid})")
            for onglet in ["", "/pricing", "/conditions", "/delivery"]:
                print(f"    https://fr.getaround.com/dashboard/cars/{aid}{onglet}")
        return

    # Grouper par compte
    comptes: dict[str, list[dict]] = {}
    for v in vehicules:
        compte = v.get("proprietaire", "inconnu")
        comptes.setdefault(compte, []).append(v)

    all_records = []
    qc_errors = []

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        )
        page = await context.new_page()

        for compte_email, vehicules_compte in comptes.items():
            compte_nom = vehicules_compte[0].get("proprietaire", compte)
            print(f"\n--- Compte : {compte_nom} — {len(vehicules_compte)} véhicules ---")

            # Vérifier si connecté en naviguant vers le dashboard
            await page.goto("https://fr.getaround.com/dashboard/cars", wait_until="domcontentloaded", timeout=20000)
            await asyncio.sleep(2)

            current_url = page.url
            if "login" in current_url or "sign_in" in current_url:
                print(f"  [WARN] Non connecté sur ce compte. Les données seront vides.")
                for v in vehicules_compte:
                    rec = empty_record(v, date_collecte)
                    rec["erreur"] = "Non connecté — authentification requise"
                    all_records.append(rec)
                    qc_errors.append(f"{v.get('immatriculation')} : non connecté")
                continue

            for v in vehicules_compte:
                record = await scrape_vehicule(page, v, date_collecte)
                all_records.append(record)
                if record.get("erreur"):
                    qc_errors.append(f"{v.get('immatriculation')} : {record['erreur']}")
                await asyncio.sleep(MIN_DELAY)

        await browser.close()

    # Sauvegarde CSV
    output_dir.mkdir(parents=True, exist_ok=True)
    csv_path = output_dir / f"parametres_flotte_{date_str}.csv"

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES_M2, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(all_records)

    # Rapport QC
    qc = {
        "date_collecte": date_collecte,
        "nb_vehicules_attendus": len(vehicules),
        "nb_vehicules_collectes": len(all_records),
        "nb_erreurs": len(qc_errors),
        "erreurs": qc_errors,
        "vehicules_bloques_inspectes": [
            r.get("immatriculation") for r in all_records
            if r.get("immatriculation") in VEHICULES_BLOQUES
        ],
        "alertes": [],
    }

    if len(all_records) != 37:
        qc["alertes"].append(
            f"ALERTE : {len(all_records)} lignes collectées au lieu de 37 attendues."
        )
    if qc_errors:
        qc["alertes"].append(
            f"ALERTE : {len(qc_errors)} véhicule(s) en erreur : {qc_errors}"
        )

    qc_path = output_dir / f"qc_mission2_{date_str}.json"
    with open(qc_path, "w", encoding="utf-8") as f:
        json.dump(qc, f, ensure_ascii=False, indent=2)

    print(f"\n{'='*60}")
    print(f"Mission 2 terminée")
    print(f"  CSV  : {csv_path} ({len(all_records)} lignes)")
    print(f"  QC   : {qc_path}")
    if qc["alertes"]:
        for a in qc["alertes"]:
            print(f"  ⚠️  {a}")
    print(f"{'='*60}\n")

    return all_records, qc


def main():
    parser = argparse.ArgumentParser(description="Getaround Mission 2 — Dashboard authentifié")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--vehicles-file", default=str(DEFAULT_VEHICLES_FILE))
    parser.add_argument("--dry-run", action="store_true",
                        help="Affiche les URLs sans scraper")
    args = parser.parse_args()

    asyncio.run(run_mission2(
        Path(args.output_dir),
        Path(args.vehicles_file),
        dry_run=args.dry_run,
    ))


if __name__ == "__main__":
    main()
