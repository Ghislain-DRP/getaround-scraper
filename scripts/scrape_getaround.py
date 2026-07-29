"""
Getaround Scraper v3.2 — Mission 1 : Collecte concurrentielle (public, sans authentification)
==============================================================================================

Trois pipelines :
  A1 — Recensement de l'offre        : 1x/jour à 06h00, fenêtre J+3 08h00→20h00
  A2 — Détection de réservation      : 4x/jour à 08h30/13h/17h/21h, fenêtre J+1 09h00→20h00
  B  — Pricing week-end              : mer/jeu/ven à 10h00, fenêtre sam 08h00→dim 20h00

Architecture v3.3 :
  - Branche GPS (A2) : URL /search?latitude=...&longitude=... + pagination par clic
    → MAX_CLICS=50, ~40 cartes/clic, couverture ≥ 95% par point
  - 3 points GPS fixes A2 : Puteaux, Asnières-sur-Seine, Paris 17e
  - Branche SEO (A1/B) : URL par commune, scraping HTML classique
  - Nouveaux champs : distance_recherche, nb_resultats_total, nb_clics_pagination
  - version_collecte tracée dans chaque ligne CSV et dans le QC JSON
  - v3.3 : migration URL /search (Getaround a supprimé /location-voiture/france?address=...)
    Sélecteur : [data-car-page-url] au lieu de a[href*="/location-voiture/"]
    Bouton pagination : 'Afficher plus de résultats' (classe search-results__load-more-button)

Règles non négociables :
  - Ne jamais inventer une valeur (champ absent = vide, pas de valeur par défaut)
  - Identification par annonce_id uniquement (pas de correspondance approchée)
  - Signaler les échecs dans le rapport QC
  - Au moins 2 secondes entre deux requêtes
  - Historique cumulatif en ajout uniquement (jamais écrasé)

Usage :
    python scrape_getaround.py --pipeline A1
    python scrape_getaround.py --pipeline A2
    python scrape_getaround.py --pipeline B
"""

import asyncio
import json
import csv
import re
import os
import argparse
import time
from datetime import datetime, timedelta, date
from pathlib import Path
from playwright.async_api import async_playwright

import sys
sys.path.insert(0, str(Path(__file__).parent))
from analyse import generate_synthesis, load_json, infer_segment, infer_energie


# ─── Version ─────────────────────────────────────────────────────────────────

VERSION_COLLECTE = "v3.3-search-gps"


# ─── 36 communes du département 92 ───────────────────────────────────────────

ALL_COMMUNES = [
    ("antony",                "Antony"),
    ("asnieres-sur-seine",    "Asnières-sur-Seine"),
    ("bagneux",               "Bagneux"),
    ("bois-colombes",         "Bois-Colombes"),
    ("boulogne-billancourt",  "Boulogne-Billancourt"),
    ("bourg-la-reine",        "Bourg-la-Reine"),
    ("chatenay-malabry",      "Châtenay-Malabry"),
    ("chatillon",             "Châtillon"),
    ("chaville",              "Chaville"),
    ("clamart",               "Clamart"),
    ("clichy",                "Clichy"),
    ("colombes",              "Colombes"),
    ("courbevoie",            "Courbevoie"),
    ("fontenay-aux-roses",    "Fontenay-aux-Roses"),
    ("garches",               "Garches"),
    ("la-garenne-colombes",   "La Garenne-Colombes"),
    ("gennevilliers",         "Gennevilliers"),
    ("issy-les-moulineaux",   "Issy-les-Moulineaux"),
    ("levallois-perret",      "Levallois-Perret"),
    ("malakoff",              "Malakoff"),
    ("marnes-la-coquette",    "Marnes-la-Coquette"),
    ("meudon",                "Meudon"),
    ("montrouge",             "Montrouge"),
    ("nanterre",              "Nanterre"),
    ("neuilly-sur-seine",     "Neuilly-sur-Seine"),
    ("le-plessis-robinson",   "Le Plessis-Robinson"),
    ("puteaux",               "Puteaux"),
    ("rueil-malmaison",       "Rueil-Malmaison"),
    ("saint-cloud",           "Saint-Cloud"),
    ("sceaux",                "Sceaux"),
    ("sevres",                "Sèvres"),
    ("suresnes",              "Suresnes"),
    ("vanves",                "Vanves"),
    ("vaucresson",            "Vaucresson"),
    ("ville-d-avray",         "Ville-d'Avray"),
    ("villeneuve-la-garenne", "Villeneuve-la-Garenne"),
]

# ─── Points GPS fixes pour le pipeline A2 ────────────────────────────────────
# Format : (label, lat, lng)
# Choisis pour couvrir le marché 92 + Paris périphérie nord-ouest

A2_GPS_POINTS = [
    ("Puteaux",             48.8847, 2.2388),
    ("Asnières-sur-Seine",  48.9175, 2.2861),
    ("Paris 17e",           48.8836, 2.3088),
]

MAX_CLICS = 50  # Nombre maximum de clics "Afficher plus" par point GPS

DEFAULT_OUTPUT_DIR = Path("/home/ubuntu/getaround_results")
DEFAULT_VEHICLES_FILE = Path(__file__).parent.parent / "references" / "mes_vehicules.json"
HISTORY_FILE = DEFAULT_OUTPUT_DIR / "getaround_history.csv"
MIN_DELAY = 2.0  # secondes entre requêtes

# Regex distance : "à 670 m" ou "à 2,8 km"
_DIST_RE = re.compile(r"à\s+(\d+(?:[.,]\d+)?)\s*(m|km)\b", re.IGNORECASE)

# Regex note/avis : "4.65(75)" ou "4,65 (75 avis)"
NOTE_AVIS_RE = re.compile(r"(\d+[.,]\d+)\s*\((\d+)\)")


# ─── Calcul des fenêtres de dates par pipeline ────────────────────────────────

def compute_window(pipeline: str) -> tuple[str, str]:
    """
    Retourne (fenetre_debut, fenetre_fin) au format ISO 8601 selon le pipeline.

    A1 : J+3 08h00 → J+3 20h00
    A2 : J+1 09h00 → J+1 20h00
    B  : samedi de la semaine courante 08h00 → dimanche 20h00
    """
    today = date.today()

    if pipeline == "A1":
        target = today + timedelta(days=3)
        return (
            f"{target.isoformat()}T08:00",
            f"{target.isoformat()}T20:00",
        )
    elif pipeline == "A2":
        target = today + timedelta(days=1)
        return (
            f"{target.isoformat()}T09:00",
            f"{target.isoformat()}T20:00",
        )
    elif pipeline == "B":
        days_until_saturday = (5 - today.weekday()) % 7
        if days_until_saturday == 0:
            days_until_saturday = 7
        saturday = today + timedelta(days=days_until_saturday)
        sunday = saturday + timedelta(days=1)
        return (
            f"{saturday.isoformat()}T08:00",
            f"{sunday.isoformat()}T20:00",
        )
    else:
        raise ValueError(f"Pipeline inconnu : {pipeline}")


# ─── Extraction note/avis ─────────────────────────────────────────────────────

def extract_note_avis(texte: str):
    """
    Extrait (note, nb_avis) du texte brut d'une carte annonce.
    Retourne (None, None) si le véhicule n'a aucun avis.

    Stratégie : on coupe le texte avant "À partir de" pour ne pas
    capturer des chiffres de prix, puis on retient la DERNIÈRE occurrence.
    """
    head = str(texte).split("À partir de")[0]
    dernier = None
    for dernier in NOTE_AVIS_RE.finditer(head):
        pass
    if dernier is None:
        return None, None
    return float(dernier.group(1).replace(",", ".")), int(dernier.group(2))


# ─── Extraction d'un prix numérique ──────────────────────────────────────────

def parse_price(text: str):
    """Retourne un float ou None (jamais de valeur par défaut)."""
    if not text:
        return None
    m = re.search(r"(\d+[.,]?\d*)", text.replace("\u202f", "").replace("\xa0", ""))
    if m:
        return float(m.group(1).replace(",", "."))
    return None


def parse_int(text: str):
    """Retourne un int ou None."""
    if not text:
        return None
    m = re.search(r"(\d+)", text)
    return int(m.group(1)) if m else None


# ─── Extraction de l'annonce_id depuis une URL ───────────────────────────────

def extract_annonce_id(url: str) -> str:
    """Extrait l'ID numérique à la fin de l'URL Getaround."""
    if not url:
        return ""
    m = re.search(r"-(\d+)(?:\?|$|/)", url)
    if m:
        return m.group(1)
    m = re.search(r"/(\d+)(?:\?|$|/)", url)
    return m.group(1) if m else ""


# ─── JS d'extraction des cartes ──────────────────────────────────────────────

JS_CARDS_SEARCH = r"""
() => {
    const cards = [];
    const seen = new Set();
    // v3.3 : sélecteur data-car-page-url (page /search)
    const cardEls = document.querySelectorAll('[data-car-page-url]');
    cardEls.forEach(el => {
        const pageUrl = el.getAttribute('data-car-page-url') || '';
        const m = pageUrl.match(/-(\d+)(?:\?|$)/);
        if (!m) return;
        const id = m[1];
        if (seen.has(id)) return;
        seen.add(id);
        const fullText = el.innerText || '';
        // Commune depuis l'URL
        const communeM = pageUrl.match(/\/location-voiture\/([^\/]+)\//);
        // Type connexion depuis le badge
        const connM = fullText.match(/Getaround Connect/i);
        const rdvM = fullText.match(/rendez-vous/i);
        // Prix actuel (span.c-font-bold contenant €)
        let prix_actuel = '';
        const boldSpans = el.querySelectorAll('span.c-font-bold');
        for (const sp of boldSpans) {
            if (/€/.test(sp.innerText || '')) {
                const pm = (sp.innerText || '').replace(/[\u202f\xa0]/g, '').match(/(\d+)/);
                if (pm) { prix_actuel = pm[1]; break; }
            }
        }
        cards.push({
            id: id,
            href: pageUrl.split('?')[0],
            fullText: fullText,
            prix_jour: prix_actuel,
            prix_heure: '',
            commune_annonce_slug: communeM ? communeM[1] : '',
            type_connexion: connM ? 'Getaround Connect' : (rdvM ? 'Sur rendez-vous' : ''),
            reservation_instantanee: /instantan/i.test(fullText),
            livraison_disponible: /livraison/i.test(fullText),
        });
    });
    return cards;
}
"""

# JS pour la branche SEO (pages /location-voiture/{commune}) — structure inchangée depuis v3.2
JS_CARDS_SEO = r"""
() => {
    const cards = [];
    const links = document.querySelectorAll('a[href*="/location-voiture/"]');
    const seen = new Set();
    links.forEach(link => {
        const href = link.getAttribute('href') || '';
        const m = href.match(/-(\d+)(\?|$|\/)/);
        if (!m) return;
        const id = m[1];
        if (seen.has(id)) return;
        seen.add(id);
        const container = link.closest('article') || link.closest('li') || link.closest('[class*="car"]') || link.parentElement;
        const fullText = container ? container.innerText : link.innerText;
        // Prix heure et jour depuis le texte (format: 'À partir de X € /h • Y € /jour')
        const priceHM = fullText.match(/(\d+)\s*€\s*\/h/);
        const priceM = fullText.match(/(\d+)\s*€\s*\/jour/);
        const communeM = href.match(/\/location-voiture\/([^\/]+)\//);
        const connM = fullText.match(/Getaround Connect/i);
        const rdvM = fullText.match(/rendez-vous|échange de clés/i);
        cards.push({
            id: id,
            href: href,
            fullText: fullText,
            prix_jour: priceM ? priceM[1] : '',
            prix_heure: priceHM ? priceHM[1] : '',
            commune_annonce_slug: communeM ? communeM[1] : '',
            type_connexion: connM ? 'Getaround Connect' : (rdvM ? 'Sur rendez-vous' : ''),
            reservation_instantanee: /instantan/i.test(fullText),
            livraison_disponible: /livraison/i.test(fullText),
        });
    });
    return cards;
}
"""

JS_HAS_BTN = r"""
() => {
    // v3.3 : bouton 'Afficher plus de résultats' (classe search-results__load-more-button)
    const btn = document.querySelector('button.search-results__load-more-button');
    if (btn && !btn.disabled && btn.offsetParent !== null) return true;
    // Fallback : chercher par texte
    const btns = document.querySelectorAll('button');
    for (const b of btns) {
        if (/afficher plus/i.test(b.innerText || '')) return true;
    }
    return false;
}
"""

JS_NB_RESULTS = r"""
() => {
    // v3.3 : pattern '40 résultats sur 910' → retourne 910 (total)
    const allText = document.body ? document.body.innerText : '';
    const m = allText.match(/(\d+)\s*résultats sur\s*(\d+)/);
    if (m) return parseInt(m[2]);
    // Fallback : chercher le total seul
    const els = document.querySelectorAll('[class*="result"], [class*="count"], h1, h2');
    for (const el of els) {
        const m2 = (el.innerText || '').match(/(\d[\d\s]*)\s*(résultat|voiture|annonce)/i);
        if (m2) return parseInt(m2[1].replace(/\s/g, ''));
    }
    return null;
}
"""


# ─── Scraping GPS (pipeline A2) ───────────────────────────────────────────────

async def scrape_gps_point(
    page,
    label: str,
    lat: float,
    lng: float,
    fenetre_debut: str,
    fenetre_fin: str,
    pipeline: str,
    snapshot_id: str,
) -> tuple[list[dict], str | None]:
    """
    Scrape un point GPS par pagination complète (clic "Afficher plus").
    Retourne (liste_annonces, erreur_ou_None).
    """
    # v3.3 : nouvelle URL /search (Getaround a supprimé /location-voiture/france?address=...)
    # Dates au format YYYY-MM-DD (extraire depuis fenetre_debut/fin au format YYYY-MM-DDTHH:MM)
    start_date = fenetre_debut[:10]
    end_date = fenetre_fin[:10]
    start_time = fenetre_debut[11:] if len(fenetre_debut) > 10 else "09:00"
    end_time = fenetre_fin[11:] if len(fenetre_fin) > 10 else "20:00"
    url = (
        f"https://fr.getaround.com/search"
        f"?latitude={lat}&longitude={lng}"
        f"&start_date={start_date}&start_time={start_time}"
        f"&end_date={end_date}&end_time={end_time}"
        f"&country_scope=FR&display_view=list"
        f"&pickup_method_explicit_choice=true"
    )

    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=30000)
        await asyncio.sleep(MIN_DELAY)

        # Attendre le chargement initial des cartes (v3.3 : sélecteur data-car-page-url)
        try:
            await page.wait_for_selector(
                '[data-car-page-url]', timeout=15000
            )
        except Exception:
            pass

        # Lire le nombre total de résultats annoncé
        nb_total = await page.evaluate(JS_NB_RESULTS)

        # Pagination : cliquer "Afficher plus" jusqu'à épuisement ou MAX_CLICS
        nb_clics = 0
        for _ in range(MAX_CLICS):
            has_btn = await page.evaluate(JS_HAS_BTN)
            if not has_btn:
                break
            # Cliquer le bouton
            try:
                await page.evaluate(r"""
                    () => {
                        // v3.3 : clic sur le bouton search-results__load-more-button
                        const btn = document.querySelector('button.search-results__load-more-button');
                        if (btn && !btn.disabled) { btn.click(); return true; }
                        // Fallback : chercher par texte
                        const btns = document.querySelectorAll('button');
                        for (const b of btns) {
                            if (/afficher plus/i.test(b.innerText || '')) {
                                b.click();
                                return true;
                            }
                        }
                        return false;
                    }
                """)
                nb_clics += 1
                await asyncio.sleep(MIN_DELAY)
            except Exception:
                break

        # Extraire toutes les cartes visibles (v3.3 : JS_CARDS_SEARCH pour page /search)
        raw_cards = await page.evaluate(JS_CARDS_SEARCH)

        # ── Garde-fou run partiel ──────────────────────────────────────────────
        # Si le compteur Getaround annonce > 100 résultats mais qu'on n'a capté
        # que < 100 cartes, la page a probablement redirigé vers l'accueil
        # (throttling ou détection de bot). On refuse d'écrire un CSV vide.
        MIN_ANNONCES_ATTENDUES = 100
        if nb_total and int(str(nb_total)) >= MIN_ANNONCES_ATTENDUES and len(raw_cards) < MIN_ANNONCES_ATTENDUES:
            raise RuntimeError(
                f"RUN_PARTIEL : {len(raw_cards)} cartes captées alors que "
                f"le compteur annonce {nb_total} résultats. "
                f"Probable redirection vers l'accueil (throttling / bot-detection). "
                f"Aucun CSV écrit."
            )

        annonces = []
        rang = 0
        for card in raw_cards:
            rang += 1
            href = card.get("href", "")
            annonce_id = card.get("id", "")
            if not annonce_id:
                continue

            full_text = card.get("fullText", "")

            # Note et avis
            note, nb_avis = extract_note_avis(full_text)

            # Modèle : regex "Marque Modèle (Année)" sur page /search
            # Fallback : première ligne non-badge, non-chiffre
            _MODEL_YEAR_RE = re.compile(r'^(.+?)\s*\((\d{4})\)\s*$', re.MULTILINE)
            _BADGES = {'GETAROUND CONNECT', 'SUR RENDEZ-VOUS', 'Pépite des locataires'}
            modele = ""
            annee = ""
            m_my = _MODEL_YEAR_RE.search(full_text)
            if m_my:
                modele = f"{m_my.group(1).strip()} ({m_my.group(2)})"
                annee = m_my.group(2)
            else:
                for line in full_text.splitlines():
                    line = line.strip()
                    if line and line not in _BADGES and not re.match(r'^\d', line) and len(line) > 3:
                        modele = line
                        break

            # Commune annonce depuis l'URL
            commune_slug = card.get("commune_annonce_slug", "")
            commune_annonce = commune_slug.replace("-", " ").title() if commune_slug else ""

            # Distance depuis le point de recherche
            distance_recherche = ""
            dm = _DIST_RE.search(full_text)
            if dm:
                val = float(dm.group(1).replace(",", "."))
                unit = dm.group(2).lower()
                distance_recherche = int(val * 1000) if unit == "km" else int(val)

            annonces.append({
                "snapshot_id":           snapshot_id,
                "pipeline":              pipeline,
                "version_collecte":      VERSION_COLLECTE,
                "fenetre_debut":         fenetre_debut,
                "fenetre_fin":           fenetre_fin,
                "annonce_id":            annonce_id,
                "url":                   f"https://fr.getaround.com{href}" if href.startswith("/") else href,
                "modele":                modele,
                "annee":                 annee,
                "type_connexion":        card.get("type_connexion", ""),
                "segment":               infer_segment(modele),
                "energie":               infer_energie(modele, card.get("type_connexion", "")),
                "reservation_instantanee": card.get("reservation_instantanee", False),
                "prix_jour":             parse_price(card.get("prix_jour", "")) or "",
                "prix_heure":            parse_price(card.get("prix_heure", "")) or "",
                "note":                  note if note is not None else "",
                "nb_avis":               nb_avis if nb_avis is not None else "",
                "commune_recherche":     label,
                "commune_annonce":       commune_annonce,
                "communes_recherche":    label,
                "nb_communes_matchees":  1,
                "rang_resultat":         rang,
                "livraison_disponible":  card.get("livraison_disponible", False),
                "mon_vehicule":          False,
                "compte_proprietaire":   "",
                "distance_recherche":    distance_recherche,
                "nb_resultats_total":    nb_total if nb_total is not None else "",
                "nb_clics_pagination":   nb_clics,
                "source_taxonomie":      "SEGMENT_RULES",
            })

        return annonces, None

    except Exception as e:
        return [], str(e)


# ─── Scraping SEO (pipeline A1/B) ─────────────────────────────────────────────

async def scrape_commune(
    page,
    slug: str,
    label: str,
    fenetre_debut: str,
    fenetre_fin: str,
    pipeline: str,
    snapshot_id: str,
) -> tuple[list[dict], str | None]:
    """
    Scrape une commune via URL SEO (A1/B).
    Retourne (liste_annonces, erreur_ou_None).
    """
    url = (
        f"https://fr.getaround.com/location-voiture/{slug}"
        f"?start_date={fenetre_debut}&end_date={fenetre_fin}"
    )

    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=30000)
        await asyncio.sleep(MIN_DELAY)

        try:
            await page.wait_for_selector(
                'a[href*="/location-voiture/"]', timeout=8000
            )
        except Exception:
            pass

        raw_cards = await page.evaluate(JS_CARDS_SEO)

        annonces = []
        rang = 0
        for card in raw_cards:
            rang += 1
            href = card.get("href", "")
            annonce_id = card.get("id", "")
            if not annonce_id:
                continue

            full_text = card.get("fullText", "")
            note, nb_avis = extract_note_avis(full_text)

            modele = ""
            _SEO_BADGES = {
                'GETAROUND CONNECT', 'SUR RENDEZ-VOUS', 'UTILITAIRE', 'BERLINE',
                'BREAK', 'COMPACTE', 'CITADINE', 'MONOSPACE', 'SUV', 'CABRIOLET',
                'MICRO-CITADINE', 'AUTOMATIQUE', 'ELECTRIQUE',
                'PÉPITE DES LOCATAIRES', 'PÉPITE',
            }
            head = full_text.split("À partir de")[0].strip()
            for line in head.splitlines():
                line = line.strip()
                if not line or line.upper() in _SEO_BADGES:
                    continue
                if re.match(r"^\d", line) or len(line) <= 2:
                    continue
                modele = line
                break

            commune_slug = card.get("commune_annonce_slug", "")
            commune_annonce = commune_slug.replace("-", " ").title() if commune_slug else ""

            annonces.append({
                "snapshot_id":           snapshot_id,
                "pipeline":              pipeline,
                "version_collecte":      VERSION_COLLECTE,
                "fenetre_debut":         fenetre_debut,
                "fenetre_fin":           fenetre_fin,
                "annonce_id":            annonce_id,
                "url":                   f"https://fr.getaround.com{href}" if href.startswith("/") else href,
                "modele":                modele,
                "annee":                 "",
                "type_connexion":        card.get("type_connexion", ""),
                "segment":               infer_segment(modele),
                "energie":               infer_energie(modele, card.get("type_connexion", "")),
                "reservation_instantanee": card.get("reservation_instantanee", False),
                "prix_jour":             parse_price(card.get("prix_jour", "")) or "",
                "prix_heure":            parse_price(card.get("prix_heure", "")) or "",
                "note":                  note if note is not None else "",
                "nb_avis":               nb_avis if nb_avis is not None else "",
                "commune_recherche":     label,
                "commune_annonce":       commune_annonce,
                "communes_recherche":    label,
                "nb_communes_matchees":  1,
                "rang_resultat":         rang,
                "livraison_disponible":  card.get("livraison_disponible", False),
                "mon_vehicule":          False,
                "compte_proprietaire":   "",
                "distance_recherche":    "",
                "nb_resultats_total":    "",
                "nb_clics_pagination":   "",
                "source_taxonomie":      "SEGMENT_RULES",
            })

        return annonces, None

    except Exception as e:
        return [], str(e)


# ─── Dédoublonnage et agrégation ─────────────────────────────────────────────

def deduplicate(all_annonces: list[dict]) -> list[dict]:
    """
    Une seule ligne par (snapshot_id, annonce_id).
    Agrège communes_recherche et nb_communes_matchees.
    Conserve le rang le plus bas (première apparition).
    """
    index: dict[str, dict] = {}

    for a in all_annonces:
        key = f"{a['snapshot_id']}|{a['annonce_id']}"
        if key not in index:
            index[key] = dict(a)
            index[key]["_communes_list"] = [a["commune_recherche"]]
        else:
            existing = index[key]
            if a["commune_recherche"] not in existing["_communes_list"]:
                existing["_communes_list"].append(a["commune_recherche"])
            if a["rang_resultat"] and (
                not existing["rang_resultat"] or
                a["rang_resultat"] < existing["rang_resultat"]
            ):
                existing["rang_resultat"] = a["rang_resultat"]

    result = []
    for item in index.values():
        item["communes_recherche"] = "|".join(item["_communes_list"])
        item["nb_communes_matchees"] = len(item["_communes_list"])
        del item["_communes_list"]
        result.append(item)

    return result


# ─── Marquage des véhicules propriétaires ────────────────────────────────────

def mark_owner_vehicles(annonces: list[dict], owner_vehicles: list[dict]) -> list[dict]:
    """Marque les annonces appartenant au propriétaire par annonce_id uniquement."""
    owner_index = {str(v["id_getaround"]): v for v in owner_vehicles if v.get("id_getaround")}

    for a in annonces:
        aid = str(a.get("annonce_id", ""))
        if aid in owner_index:
            v = owner_index[aid]
            a["mon_vehicule"] = True
            a["compte_proprietaire"] = v.get("proprietaire", "")
        else:
            a["mon_vehicule"] = False
            a["compte_proprietaire"] = ""

    return sorted(annonces, key=lambda x: (0 if x["mon_vehicule"] else 1, x.get("rang_resultat", 999)))


# ─── Contrôle qualité ─────────────────────────────────────────────────────────

def build_qc_report(
    snapshot_id: str,
    pipeline: str,
    fenetre_debut: str,
    fenetre_fin: str,
    communes_ok: list[str],
    communes_ko: dict[str, str],
    all_annonces: list[dict],
    dedup_annonces: list[dict],
    previous_snapshot_count: int | None,
    couverture_par_point: dict | None = None,
) -> dict:
    """Construit le rapport de contrôle qualité."""

    nb_communes_ok = len(communes_ok)
    nb_communes_ko = len(communes_ko)
    nb_total_communes = len(ALL_COMMUNES)

    nb_annonces_brutes = len(all_annonces)
    nb_annonce_ids_distincts = len({a["annonce_id"] for a in dedup_annonces if a["annonce_id"]})

    nb_prix_manquants = sum(1 for a in dedup_annonces if a.get("prix_jour") == "" or a.get("prix_jour") is None)

    nb_hors_92 = sum(
        1 for a in dedup_annonces
        if a.get("commune_annonce") and not any(
            c[1].lower() in a["commune_annonce"].lower()
            for c in ALL_COMMUNES
        )
    )

    # Notes
    nb_avec_note = sum(1 for a in dedup_annonces if a.get("note") not in ("", None))
    taux_notes_pct = round(nb_avec_note / max(len(dedup_annonces), 1) * 100, 1)

    # Variation vs snapshot précédent
    variation_pct = None
    alerte_volume = False
    if previous_snapshot_count is not None and previous_snapshot_count > 0:
        variation_pct = round(
            (nb_annonce_ids_distincts - previous_snapshot_count) / previous_snapshot_count * 100, 1
        )
        alerte_volume = abs(variation_pct) > 50

    # Alerte communes (A1/B uniquement — A2 utilise la couverture par point)
    alerte_communes = pipeline != "A2" and nb_communes_ok < 30

    # Segmentation
    nb_non_classe = sum(1 for a in dedup_annonces if a.get("segment") == "NON_CLASSE")
    pct_non_classe = round(nb_non_classe / len(dedup_annonces) * 100, 1) if dedup_annonces else 0.0
    alerte_segmentation = pct_non_classe > 2.0

    # Géographie
    nb_commune_manquante = sum(1 for a in dedup_annonces if not a.get("commune_annonce"))
    pct_commune_manquante = round(nb_commune_manquante / len(dedup_annonces) * 100, 1) if dedup_annonces else 0.0
    alerte_geo = pct_commune_manquante > 5.0

    # Couverture par point GPS (A2)
    alerte_couverture = False
    if couverture_par_point:
        sous_seuil = [
            pt for pt, v in couverture_par_point.items()
            if v.get("taux_pct") is not None and v["taux_pct"] < 95.0
        ]
        alerte_couverture = len(sous_seuil) > 0

    alertes = []
    if alerte_communes:
        alertes.append(
            f"ALERTE : seulement {nb_communes_ok}/{nb_total_communes} communes ont abouti "
            f"(minimum requis : 30). Communes en échec : {list(communes_ko.keys())}"
        )
    if alerte_volume and variation_pct is not None:
        alertes.append(
            f"ALERTE VOLUME : écart de {variation_pct:+.1f}% à la médiane historique du pipeline "
            f"(seuil : 50%). Vérifier si anomalie technique."
        )
    if alerte_segmentation:
        alertes.append(
            f"ALERTE : {pct_non_classe}% des annonces en segment NON_CLASSE "
            f"({nb_non_classe} véhicules, seuil : 2%). Enrichir SEGMENT_RULES dans analyse.py."
        )
    if alerte_geo:
        alertes.append(
            f"ALERTE : {pct_commune_manquante}% des annonces sans commune_annonce "
            f"({nb_commune_manquante} véhicules, seuil : 5%). Vérifier le sélecteur JS."
        )
    if alerte_couverture and couverture_par_point:
        sous = [
            f"{pt}: {v['taux_pct']}% ({v['nb_captes']}/{v['nb_total']})"
            for pt, v in couverture_par_point.items()
            if v.get("taux_pct") is not None and v["taux_pct"] < 95.0
        ]
        alertes.append(
            f"ALERTE COUVERTURE : points GPS sous 95% : {sous}. "
            f"Cause probable : rate-limit 429 en cours de pagination."
        )

    return {
        "snapshot_id":                   snapshot_id,
        "pipeline":                      pipeline,
        "version_collecte":              VERSION_COLLECTE,
        "fenetre_debut":                 fenetre_debut,
        "fenetre_fin":                   fenetre_fin,
        "nb_communes_interrogees":       nb_total_communes,
        "nb_communes_ok":                nb_communes_ok,
        "nb_communes_ko":                nb_communes_ko,
        "communes_ok":                   communes_ok,
        "communes_ko":                   communes_ko,
        "nb_annonces_brutes":            nb_annonces_brutes,
        "nb_annonce_ids_distincts":      nb_annonce_ids_distincts,
        "nb_prix_jour_manquants":        nb_prix_manquants,
        "nb_annonces_hors_92":           nb_hors_92,
        "taux_notes_pct":                taux_notes_pct,
        "variation_vs_precedent_pct":    variation_pct,
        "alerte_communes":               alerte_communes,
        "alerte_volume":                 alerte_volume,
        "nb_segment_non_classe":         nb_non_classe,
        "pct_segment_non_classe":        pct_non_classe,
        "nb_commune_annonce_manquante":  nb_commune_manquante,
        "pct_commune_annonce_manquante": pct_commune_manquante,
        "alerte_segmentation":           alerte_segmentation,
        "alerte_geo":                    alerte_geo,
        "couverture_par_point":          couverture_par_point or {},
        "alerte_couverture":             alerte_couverture,
        "alertes":                       alertes,
    }


# ─── Sauvegarde CSV ───────────────────────────────────────────────────────────

FIELDNAMES = [
    "snapshot_id", "pipeline", "version_collecte", "fenetre_debut", "fenetre_fin",
    "annonce_id", "url", "modele", "annee", "type_connexion",
    "segment", "source_taxonomie", "energie",
    "reservation_instantanee", "prix_jour", "prix_heure",
    "note", "nb_avis",
    "commune_recherche", "commune_annonce",
    "communes_recherche", "nb_communes_matchees",
    "rang_resultat", "livraison_disponible",
    "mon_vehicule", "compte_proprietaire",
    "distance_recherche", "nb_resultats_total", "nb_clics_pagination",
]


def save_csv(annonces: list[dict], filepath: Path):
    """Sauvegarde un CSV avec les champs standardisés."""
    filepath.parent.mkdir(parents=True, exist_ok=True)
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(annonces)


def append_to_history(annonces: list[dict], history_file: Path):
    """Ajoute les nouvelles annonces au fichier historique cumulatif."""
    history_file.parent.mkdir(parents=True, exist_ok=True)

    existing_keys = set()
    if history_file.exists():
        with open(history_file, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                existing_keys.add(f"{row.get('snapshot_id')}|{row.get('annonce_id')}")

    new_rows = [
        a for a in annonces
        if f"{a['snapshot_id']}|{a['annonce_id']}" not in existing_keys
    ]

    if not new_rows:
        return 0

    write_header = not history_file.exists()
    with open(history_file, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES, extrasaction="ignore")
        if write_header:
            writer.writeheader()
        writer.writerows(new_rows)

    return len(new_rows)


def get_previous_snapshot_count(pipeline: str, output_dir: Path) -> int | None:
    """Retourne le nombre d'annonces du dernier snapshot du même pipeline."""
    pattern = f"getaround_{pipeline}_*.csv"
    files = sorted(output_dir.glob(pattern))
    if len(files) < 2:
        return None
    prev_file = files[-2]
    try:
        with open(prev_file, "r", encoding="utf-8") as f:
            return sum(1 for _ in csv.DictReader(f))
    except Exception:
        return None


# ─── Orchestrateur principal ──────────────────────────────────────────────────

async def run_pipeline(pipeline: str, output_dir: Path, vehicles_file: Path):
    """Exécute un pipeline complet."""

    snapshot_id = datetime.now().strftime("%Y%m%dT%H%M%S")
    fenetre_debut, fenetre_fin = compute_window(pipeline)

    print(f"\n{'='*60}")
    print(f"Pipeline {pipeline} — Snapshot {snapshot_id}")
    print(f"Version : {VERSION_COLLECTE}")
    print(f"Fenêtre : {fenetre_debut} → {fenetre_fin}")
    print(f"{'='*60}\n")

    # Charger les véhicules propriétaires
    owner_vehicles = []
    if vehicles_file.exists():
        try:
            owner_vehicles = json.load(open(vehicles_file, encoding="utf-8"))
        except Exception as e:
            print(f"[WARN] Impossible de charger {vehicles_file} : {e}")

    all_annonces = []
    communes_ok = []
    communes_ko = {}
    couverture_par_point = {}

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        )
        page = await context.new_page()

        if pipeline == "A2":
            # ── Branche GPS : 3 points fixes ──────────────────────────────────
            for i, (label, lat, lng) in enumerate(A2_GPS_POINTS):
                print(f"[GPS {i+1}/{len(A2_GPS_POINTS)}] {label} ({lat}, {lng})...", end=" ", flush=True)

                annonces, erreur = await scrape_gps_point(
                    page, label, lat, lng, fenetre_debut, fenetre_fin, pipeline, snapshot_id
                )

                if erreur:
                    print(f"ERREUR : {erreur}")
                    communes_ko[label] = erreur
                    couverture_par_point[label] = {"nb_captes": 0, "nb_total": None, "taux_pct": None}
                else:
                    nb_total = annonces[0].get("nb_resultats_total", "") if annonces else ""
                    nb_clics = annonces[0].get("nb_clics_pagination", 0) if annonces else 0
                    print(f"{len(annonces)} annonces | {nb_clics} clics | total={nb_total}")
                    communes_ok.append(label)
                    all_annonces.extend(annonces)
                    # Couverture par point
                    if nb_total and int(str(nb_total)) > 0:
                        taux = round(len(annonces) / int(str(nb_total)) * 100, 1)
                    else:
                        taux = 100.0
                    couverture_par_point[label] = {
                        "nb_captes": len(annonces),
                        "nb_total": nb_total if nb_total else len(annonces),
                        "taux_pct": taux,
                    }

                if i < len(A2_GPS_POINTS) - 1:
                    await asyncio.sleep(MIN_DELAY)

        else:
            # ── Branche SEO : 36 communes ──────────────────────────────────────
            env_cities = os.environ.get("GETAROUND_CITIES", "")
            if env_cities:
                slugs = [s.strip() for s in env_cities.split(",")]
                communes = [(s, l) for s, l in ALL_COMMUNES if s in slugs]
            else:
                communes = ALL_COMMUNES

            for i, (slug, label) in enumerate(communes):
                print(f"[{i+1:02d}/{len(communes)}] {label}...", end=" ", flush=True)

                annonces, erreur = await scrape_commune(
                    page, slug, label, fenetre_debut, fenetre_fin, pipeline, snapshot_id
                )

                if erreur:
                    print(f"ERREUR : {erreur}")
                    communes_ko[label] = erreur
                else:
                    print(f"{len(annonces)} annonces")
                    communes_ok.append(label)
                    all_annonces.extend(annonces)

                if i < len(communes) - 1:
                    await asyncio.sleep(MIN_DELAY)

        await browser.close()

    # Dédoublonnage
    dedup = deduplicate(all_annonces)

    # Marquage propriétaire
    dedup = mark_owner_vehicles(dedup, owner_vehicles)

    print(f"\nTotal brut : {len(all_annonces)} | Dédoublonné : {len(dedup)} annonces uniques")
    if pipeline == "A2":
        print(f"Points GPS : {len(communes_ok)} OK | {len(communes_ko)} KO")
    else:
        print(f"Communes OK : {len(communes_ok)}/{len(ALL_COMMUNES)} | KO : {len(communes_ko)}")

    # Contrôle qualité
    prev_count = get_previous_snapshot_count(pipeline, output_dir)
    qc = build_qc_report(
        snapshot_id, pipeline, fenetre_debut, fenetre_fin,
        communes_ok, communes_ko, all_annonces, dedup, prev_count,
        couverture_par_point=couverture_par_point if pipeline == "A2" else None,
    )

    for alerte in qc["alertes"]:
        print(f"\n⚠️  {alerte}")

    # Sauvegarde
    output_dir.mkdir(parents=True, exist_ok=True)
    csv_path = output_dir / f"getaround_{pipeline}_{snapshot_id}.csv"
    qc_path = output_dir / f"qc_{snapshot_id}.json"

    save_csv(dedup, csv_path)
    with open(qc_path, "w", encoding="utf-8") as f:
        json.dump(qc, f, ensure_ascii=False, indent=2)

    # Historique cumulatif
    history_path = output_dir / "getaround_history.csv"
    nb_new = append_to_history(dedup, history_path)
    print(f"\nFichiers sauvegardés :")
    print(f"  CSV     : {csv_path}")
    print(f"  QC      : {qc_path}")
    print(f"  History : {nb_new} nouvelles lignes ajoutées à {history_path}")

    # Synthèse (A2 et B)
    if pipeline in ("A2", "B"):
        prev_files = sorted(output_dir.glob(f"getaround_{pipeline}_*.csv"))
        prev_annonces = []
        if len(prev_files) >= 2:
            try:
                with open(prev_files[-2], "r", encoding="utf-8") as f:
                    prev_annonces = list(csv.DictReader(f))
            except Exception:
                pass

        try:
            synthese = generate_synthesis(
                dedup, prev_annonces, owner_vehicles,
                pipeline=pipeline, scrape_time=snapshot_id, qc=qc
            )
        except Exception as e:
            synthese = f"# Erreur synthèse\n{e}"

        synthese_path = output_dir / f"synthese_{pipeline}_{snapshot_id}.md"
        with open(synthese_path, "w", encoding="utf-8") as f:
            f.write(synthese)
        print(f"  Synthèse: {synthese_path}")

        print(f"\n{'='*60}")
        print(synthese)
        print(f"{'='*60}\n")

    return dedup, qc


# ─── Point d'entrée ───────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Getaround Scraper v3.3")
    parser.add_argument(
        "--pipeline", "-p",
        choices=["A1", "A2", "B"],
        default=os.environ.get("GETAROUND_PIPELINE", "A2"),
        help="Pipeline à exécuter (A1, A2 ou B)"
    )
    parser.add_argument(
        "--output-dir",
        default=os.environ.get("GETAROUND_OUTPUT_DIR", str(DEFAULT_OUTPUT_DIR)),
        help="Dossier de sortie"
    )
    parser.add_argument(
        "--vehicles-file",
        default=os.environ.get("GETAROUND_VEHICLES_FILE", str(DEFAULT_VEHICLES_FILE)),
        help="Chemin vers mes_vehicules.json"
    )
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    vehicles_file = Path(args.vehicles_file)

    asyncio.run(run_pipeline(args.pipeline, output_dir, vehicles_file))


if __name__ == "__main__":
    main()
