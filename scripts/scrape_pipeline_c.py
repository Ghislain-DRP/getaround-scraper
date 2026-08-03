"""
Pipeline C — Cartographie de visibilité Getaround (hebdomadaire, lundi 04h00)
==============================================================================
Objectif : mesurer la visibilité de chaque véhicule de la flotte sur 56 points
GPS couvrant les 36 communes du 92 et les 20 arrondissements de Paris.
Pour chaque point GPS :
  - Collecte paginée complète via API search.json (v3.4, requests, sans Playwright)
  - Enregistrement du rang de chaque véhicule de la flotte
  - Enregistrement de la distance au point de recherche
Sorties :
  - getaround_C_YYYYMMDDTHHMMSS.csv       : toutes les annonces (dédoublonnées)
  - qc_C_YYYYMMDDTHHMMSS.json             : rapport QC avec couverture par point
  - visibilite_flotte_YYYYMMDDTHHMMSS.csv : rang et distance par véhicule × point
Durée nominale : ~1h43 (56 points × pagination JSON × 2s délai)
Historique des versions :
  v3.2 (initial) : Playwright + URL /location-voiture/france + JS_CARDS
  v3.4 (actuel)  : API search.json (requests, sans Playwright) — même migration que A2
Usage :
    python scrape_pipeline_c.py
    python scrape_pipeline_c.py --output-dir /home/ubuntu/getaround_results
"""
import json
import csv
import os
import argparse
import time
from datetime import datetime, timedelta, date
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).parent))

# Importer les utilitaires partagés depuis scrape_getaround v3.4
from scrape_getaround import (
    VERSION_COLLECTE, MIN_DELAY,
    FIELDNAMES,
    deduplicate, mark_owner_vehicles,
    scrape_gps_point_json,
)

DEFAULT_OUTPUT_DIR = Path("/home/ubuntu/getaround_results")
DEFAULT_VEHICLES_FILE = Path(__file__).parent.parent / "references" / "mes_vehicules.json"

# 56 points GPS : 36 communes 92 + 20 arrondissements Paris
C_GPS_POINTS = [
    # 36 communes du 92
    ("Antony",                48.7536, 2.2975),
    ("Asnieres-sur-Seine",    48.9175, 2.2861),
    ("Bagneux",               48.7950, 2.3100),
    ("Bois-Colombes",         48.9192, 2.2694),
    ("Boulogne-Billancourt",  48.8350, 2.2408),
    ("Bourg-la-Reine",        48.7792, 2.3147),
    ("Chatenay-Malabry",      48.7653, 2.2681),
    ("Chatillon",             48.8006, 2.2939),
    ("Chaville",              48.8097, 2.1894),
    ("Clamart",               48.7989, 2.2625),
    ("Clichy",                48.9042, 2.3050),
    ("Colombes",              48.9228, 2.2539),
    ("Courbevoie",            48.8972, 2.2567),
    ("Fontenay-aux-Roses",    48.7897, 2.2894),
    ("Garches",               48.8444, 2.1828),
    ("La Garenne-Colombes",   48.9078, 2.2447),
    ("Gennevilliers",         48.9328, 2.2983),
    ("Issy-les-Moulineaux",   48.8236, 2.2736),
    ("Levallois-Perret",      48.8950, 2.2878),
    ("Malakoff",              48.8186, 2.3031),
    ("Marnes-la-Coquette",    48.8317, 2.1697),
    ("Meudon",                48.8128, 2.2358),
    ("Montrouge",             48.8167, 2.3178),
    ("Nanterre",              48.8919, 2.2069),
    ("Neuilly-sur-Seine",     48.8847, 2.2686),
    ("Le Plessis-Robinson",   48.7814, 2.2647),
    ("Puteaux",               48.8847, 2.2388),
    ("Rueil-Malmaison",       48.8764, 2.1878),
    ("Saint-Cloud",           48.8461, 2.2131),
    ("Sceaux",                48.7769, 2.2958),
    ("Sevres",                48.8228, 2.2119),
    ("Suresnes",              48.8694, 2.2275),
    ("Vanves",                48.8211, 2.2883),
    ("Vaucresson",            48.8414, 2.1572),
    ("Ville-d-Avray",         48.8281, 2.1942),
    ("Villeneuve-la-Garenne", 48.9394, 2.3297),
    # 20 arrondissements de Paris
    ("Paris 1er",             48.8606, 2.3477),
    ("Paris 2e",              48.8669, 2.3472),
    ("Paris 3e",              48.8633, 2.3594),
    ("Paris 4e",              48.8533, 2.3522),
    ("Paris 5e",              48.8461, 2.3444),
    ("Paris 6e",              48.8503, 2.3328),
    ("Paris 7e",              48.8558, 2.3175),
    ("Paris 8e",              48.8747, 2.3078),
    ("Paris 9e",              48.8767, 2.3358),
    ("Paris 10e",             48.8764, 2.3597),
    ("Paris 11e",             48.8592, 2.3792),
    ("Paris 12e",             48.8400, 2.3878),
    ("Paris 13e",             48.8317, 2.3619),
    ("Paris 14e",             48.8283, 2.3261),
    ("Paris 15e",             48.8417, 2.2967),
    ("Paris 16e",             48.8636, 2.2669),
    ("Paris 17e",             48.8836, 2.3088),
    ("Paris 18e",             48.8925, 2.3444),
    ("Paris 19e",             48.8817, 2.3811),
    ("Paris 20e",             48.8636, 2.3983),
]


def run_pipeline_c(output_dir: Path, vehicles_file: Path):
    """Execute le pipeline C sur les 56 points GPS via API search.json (v3.4)."""
    snapshot_id = datetime.now().strftime("%Y%m%dT%H%M%S")
    target = date.today() + timedelta(days=3)
    fenetre_debut = f"{target.isoformat()} 08:00"
    fenetre_fin = f"{target.isoformat()} 20:00"

    print(f"\n{'='*60}")
    print(f"Pipeline C -- Cartographie visibilite -- Snapshot {snapshot_id}")
    print(f"Version : {VERSION_COLLECTE}")
    print(f"Fenetre : {fenetre_debut} -> {fenetre_fin}")
    print(f"Points GPS : {len(C_GPS_POINTS)}")
    print(f"{'='*60}\n")

    owner_vehicles = []
    if vehicles_file.exists():
        try:
            owner_vehicles = json.load(open(vehicles_file, encoding="utf-8"))
        except Exception as e:
            print(f"[WARN] Impossible de charger {vehicles_file} : {e}")
    owner_ids = {str(v["id_getaround"]) for v in owner_vehicles if v.get("id_getaround")}

    all_annonces = []
    points_ok = []
    points_ko = {}
    couverture_par_point = {}
    visibilite = {oid: {} for oid in owner_ids}

    for i, (label, lat, lng) in enumerate(C_GPS_POINTS):
        print(f"[{i+1:02d}/{len(C_GPS_POINTS)}] {label}...", end=" ", flush=True)
        annonces, erreur = scrape_gps_point_json(
            label, lat, lng,
            fenetre_debut, fenetre_fin,
            "C", snapshot_id,
        )
        if erreur:
            print(f"ERREUR : {erreur}")
            points_ko[label] = erreur
            couverture_par_point[label] = {
                "nb_captes": 0,
                "nb_total": None,
                "taux_pct": None,
            }
        else:
            nb_total = annonces[0].get("nb_resultats_total", "") if annonces else ""
            nb_pages = annonces[-1].get("nb_clics_pagination", 0) if annonces else 0
            print(f"{len(annonces)} ann. | {nb_pages} pages | total={nb_total}")
            points_ok.append(label)
            all_annonces.extend(annonces)
            if nb_total and int(str(nb_total)) > 0:
                taux = round(len(annonces) / int(str(nb_total)) * 100, 1)
            else:
                taux = 100.0
            couverture_par_point[label] = {
                "nb_captes": len(annonces),
                "nb_total": nb_total if nb_total else len(annonces),
                "taux_pct": taux,
            }
            for ann in annonces:
                aid = str(ann.get("annonce_id", ""))
                if aid in owner_ids:
                    visibilite[aid][label] = {
                        "rang": ann.get("rang_resultat", ""),
                        "distance_m": ann.get("distance_recherche", ""),
                    }
        if i < len(C_GPS_POINTS) - 1:
            time.sleep(MIN_DELAY)

    dedup = deduplicate(all_annonces)
    dedup = mark_owner_vehicles(dedup, owner_vehicles)
    print(f"\nTotal brut : {len(all_annonces)} | Dedoublonne : {len(dedup)}")
    print(f"Points OK : {len(points_ok)}/{len(C_GPS_POINTS)} | KO : {len(points_ko)}")

    # Sauvegarde CSV principal
    output_dir.mkdir(parents=True, exist_ok=True)
    csv_path = output_dir / f"getaround_C_{snapshot_id}.csv"
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(dedup)

    # Sauvegarde tableau de visibilite flotte
    vis_path = output_dir / f"visibilite_flotte_{snapshot_id}.csv"
    vis_rows = []
    for veh in owner_vehicles:
        aid = str(veh.get("id_getaround", ""))
        for pt_label, pt_lat, pt_lng in C_GPS_POINTS:
            data = visibilite.get(aid, {}).get(pt_label, {})
            vis_rows.append({
                "snapshot_id":         snapshot_id,
                "version_collecte":    VERSION_COLLECTE,
                "annonce_id":          aid,
                "modele":              veh.get("modele", ""),
                "immatriculation":     veh.get("immatriculation", ""),
                "compte_proprietaire": veh.get("proprietaire", ""),
                "point_gps":           pt_label,
                "lat":                 pt_lat,
                "lng":                 pt_lng,
                "rang":                data.get("rang", ""),
                "distance_m":          data.get("distance_m", ""),
                "visible":             bool(data),
            })
    vis_fieldnames = [
        "snapshot_id", "version_collecte", "annonce_id", "modele",
        "immatriculation", "compte_proprietaire",
        "point_gps", "lat", "lng", "rang", "distance_m", "visible",
    ]
    with open(vis_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=vis_fieldnames)
        writer.writeheader()
        writer.writerows(vis_rows)

    # QC
    nb_sous_seuil = sum(
        1 for v in couverture_par_point.values()
        if v.get("taux_pct") is not None and v["taux_pct"] < 95.0
    )
    alertes = []
    if nb_sous_seuil > 0:
        sous = [
            f"{pt}: {v['taux_pct']}%"
            for pt, v in couverture_par_point.items()
            if v.get("taux_pct") is not None and v["taux_pct"] < 95.0
        ]
        alertes.append(f"ALERTE COUVERTURE : {nb_sous_seuil} points sous 95% : {sous}")
    if len(points_ko) > 0:
        alertes.append(f"ALERTE POINTS KO : {list(points_ko.keys())}")

    qc = {
        "snapshot_id":              snapshot_id,
        "pipeline":                 "C",
        "version_collecte":         VERSION_COLLECTE,
        "fenetre_debut":            fenetre_debut,
        "fenetre_fin":              fenetre_fin,
        "nb_points":                len(C_GPS_POINTS),
        "nb_points_ok":             len(points_ok),
        "nb_points_ko":             len(points_ko),
        "points_ko":                points_ko,
        "nb_annonces_brutes":       len(all_annonces),
        "nb_annonce_ids_distincts": len({a["annonce_id"] for a in dedup if a["annonce_id"]}),
        "couverture_par_point":     couverture_par_point,
        "alertes":                  alertes,
    }
    qc_path = output_dir / f"qc_C_{snapshot_id}.json"
    with open(qc_path, "w", encoding="utf-8") as f:
        json.dump(qc, f, ensure_ascii=False, indent=2)

    print(f"\nFichiers sauvegardes :")
    print(f"  CSV principal    : {csv_path}")
    print(f"  Visibilite flotte: {vis_path}")
    print(f"  QC               : {qc_path}")

    # Resume visibilite flotte
    print(f"\n{'='*60}")
    print(f"VISIBILITE FLOTTE -- {len(owner_vehicles)} vehicules x {len(C_GPS_POINTS)} points")
    print(f"{'='*60}")
    for veh in owner_vehicles:
        aid = str(veh.get("id_getaround", ""))
        nb_visible = sum(1 for v in visibilite.get(aid, {}).values() if v)
        rangs = [v["rang"] for v in visibilite.get(aid, {}).values() if v.get("rang")]
        rang_min = min(rangs) if rangs else "--"
        print(
            f"  {veh.get('modele', aid)[:30]:<32} "
            f"visible sur {nb_visible:>2}/{len(C_GPS_POINTS)} points | "
            f"rang min = {rang_min}"
        )

    if alertes:
        print(f"\n{'='*60}")
        print("ALERTES QC :")
        for a in alertes:
            print(f"  ALERTE {a}")
        print(f"{'='*60}")

    return dedup, qc


def main():
    parser = argparse.ArgumentParser(
        description="Getaround Pipeline C -- Cartographie visibilite (56 points GPS) v3.4"
    )
    parser.add_argument(
        "--output-dir",
        default=os.environ.get("GETAROUND_OUTPUT_DIR", str(DEFAULT_OUTPUT_DIR)),
    )
    parser.add_argument(
        "--vehicles-file",
        default=os.environ.get("GETAROUND_VEHICLES_FILE", str(DEFAULT_VEHICLES_FILE)),
    )
    args = parser.parse_args()
    run_pipeline_c(Path(args.output_dir), Path(args.vehicles_file))


if __name__ == "__main__":
    main()
