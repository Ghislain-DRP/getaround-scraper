---
name: getaround-scraper
description: Collecte et analyse les annonces Getaround sur les 36 communes des Hauts-de-Seine (92) via trois pipelines — A1 recensement quotidien de l'offre, A2 détection de réservation quatre fois par jour, B pricing week-end le mercredi, jeudi et vendredi. Inclut une mission dashboard authentifiée en lecture seule pour relever les paramètres tarifaires d'une flotte de 37 véhicules répartis sur 3 comptes propriétaires. Utiliser pour tout scraping Getaround, analyse de positionnement concurrentiel, suivi de disponibilité, ou relevé des paramètres de la flotte.
license: Complete terms in LICENSE.txt
---

# Getaround Scraper v2

Scraping automatisé des annonces de location de voitures sur Getaround France pour les **36 communes du département 92 (Hauts-de-Seine)**, avec analyse concurrentielle et suivi des paramètres tarifaires d'une flotte de 37 véhicules répartis sur 3 comptes propriétaires.

---

## Quand utiliser ce skill

- L'utilisateur demande un scraping Getaround sur la zone Puteaux / Hauts-de-Seine
- L'utilisateur veut analyser les prix concurrents ou la disponibilité des véhicules
- L'utilisateur veut récupérer les paramètres tarifaires de sa flotte depuis le dashboard
- Une tâche planifiée (A1, A2 ou B) se déclenche automatiquement

---

## Architecture — Deux missions

### Mission 1 — Collecte concurrentielle (public, sans authentification)

Trois pipelines distincts, non interchangeables :

| Pipeline | Objectif | Fréquence | Fenêtre |
|---|---|---|---|
| **A1** | Recensement exhaustif de l'offre | 1x/jour à 06h00 (7j/7) | J+3 08h00 → J+3 20h00 |
| **A2** | Détection de réservation | 4x/jour à 08h30/13h/17h/21h (7j/7) | J+1 09h00 → J+1 20h00 (fixe) |
| **B** | Pricing week-end | mer/jeu/ven à 10h00 | Sam 08h00 → Dim 20h00 |

**Règle A2 impérative** : la fenêtre J+1 ne varie jamais selon l'heure du déclenchement. Les 4 relevés quotidiens posent rigoureusement la même question : *qui est disponible demain pour une journée ?*

### Mission 2 — Paramètres tarifaires (dashboard authentifié)

Collecte des paramètres de chaque véhicule depuis `https://fr.getaround.com/dashboard/cars/<ID>` (onglets Général, Prix, Mes conditions, Adresse et livraison).

**Lecture seule — aucune modification d'annonce, de prix ou de statut.**

---

## Règles non négociables

1. **Ne jamais inventer une valeur.** Champ absent = vide. Aucune valeur par défaut, aucune estimation.
2. **Identification par `annonce_id` uniquement.** Pas de correspondance approchée sur modèle/année/connexion.
3. **Signaler les échecs** dans le rapport QC (commune KO, page en erreur, champ introuvable).
4. **Respecter la plateforme.** Au moins 2 secondes entre deux requêtes, une seule session à la fois.
5. **Ne jamais écraser l'historique.** `getaround_history.csv` en ajout uniquement.
6. **Ne modifier aucune annonce** (Mission 2 = lecture seule).
7. **Le dépôt GitHub est la source de vérité.** Tout code validé doit être commité et poussé immédiatement. Ce qui n'est pas dans le dépôt n'existe pas — les fichiers locaux d'un sandbox sont perdus à la compaction ou à la création d'une nouvelle conversation.
8. **Dépôt public : aucune donnée d'identification.** Le dépôt étant public, aucune adresse email, adresse physique, immatriculation nominative, ou coordonnée personnelle ne doit y figurer. Les identifiants de flotte (IDs Getaround, immatriculations) sont versionnés dans `references/mes_vehicules.json` sans champ `compte` ni `adresse`. Les credentials de session (cookies, tokens) sont transmis en session uniquement, jamais versionnés.
9. **L'API Getaround est fragile par construction.** `search.json?car_types[]=X` retournait les catégories officielles il y a deux jours ; il renvoie HTTP 500 depuis le 28/07/2026. Getaround modifie son API sans préavis. Tout appel qui en dépend doit être encapsulé dans un try/except et son absence documentée dans le QC. Ne jamais bloquer un run sur un appel API optionnel.
10. **Garde-fou run partiel.** Si un point GPS collecte < 100 annonces alors que le compteur Getaround en annonce ≥ 100, le run s'interrompt avec `RuntimeError("RUN_PARTIEL : ...")` sans écrire de CSV. Cause probable : redirection vers la page d'accueil (throttling ou détection de bot). Getaround applique un throttling agressif en soirée après une série de requêtes depuis la même IP — il se lève en 30 à 60 minutes sans aucune requête.
11. **`source_taxonomie` documente l'origine de la segmentation.** Valeur actuelle : `"SEGMENT_RULES"` (inférence par regex sur le modèle). Valeur future : `"GETAROUND_OFFICIEL"` si la catégorie officielle devient accessible via le filtre UI ou un endpoint stable. Ne jamais mélanger les deux valeurs dans un même historique sans purge préalable.

---

## Périmètre géographique — 36 communes du 92

Antony · Asnières-sur-Seine · Bagneux · Bois-Colombes · Boulogne-Billancourt · Bourg-la-Reine · Châtenay-Malabry · Châtillon · Chaville · Clamart · Clichy · Colombes · Courbevoie · Fontenay-aux-Roses · Garches · La Garenne-Colombes · Gennevilliers · Issy-les-Moulineaux · Levallois-Perret · Malakoff · Marnes-la-Coquette · Meudon · Montrouge · Nanterre · Neuilly-sur-Seine · Le Plessis-Robinson · **Puteaux** · Rueil-Malmaison · Saint-Cloud · Sceaux · Sèvres · Suresnes · Vanves · Vaucresson · Ville-d'Avray · Villeneuve-la-Garenne

---

## Modèle de données Mission 1

| Champ | Description |
|---|---|
| `snapshot_id` | Horodatage ISO 8601 du lancement |
| `pipeline` | `A1`, `A2` ou `B` |
| `fenetre_debut` / `fenetre_fin` | Dates et heures exactes interrogées |
| `annonce_id` | **ID Getaround extrait de la fin de l'URL — champ pivot, obligatoire** |
| `url` | URL complète de l'annonce |
| `modele` | Libellé complet affiché |
| `annee` | Année (vide si absente) |
| `type_connexion` | `Getaround Connect`, `Sur rendez-vous`, ou libellé exact |
| `reservation_instantanee` | Booléen |
| `prix_jour` | Numérique en euros (vide si absent) |
| `prix_heure` | Numérique en euros (vide si absent) |
| `note` | Sur 5 (**vide si absente** — distinct de 0) |
| `nb_avis` | Nombre d'avis (**vide si absent** — distinct de 0) |
| `commune_recherche` | Commune interrogée |
| `commune_annonce` | Commune affichée sur l'annonce |
| `communes_recherche` | Liste des communes ayant retourné cette annonce (séparées par `\|`) |
| `nb_communes_matchees` | Portée algorithmique de l'annonce |
| `rang_resultat` | Position dans la page de résultats, par commune |
| `livraison_disponible` | Booléen |
| `mon_vehicule` | `True` si appartient à la flotte (par `annonce_id` uniquement) |
| `compte_proprietaire` | Nom du compte propriétaire si `mon_vehicule = True` |

**Note critique** : `nb_avis` vide ≠ `nb_avis = 0`. Ne jamais substituer l'un à l'autre.

---

## Modèle de données Mission 2

| Champ | Onglet dashboard |
|---|---|
| `annonce_id`, `immatriculation`, `modele`, `annee`, `statut` | Général |
| `nb_locations_cumulees`, `note`, `nb_avis` | Général |
| `adresse_stationnement` | Général / Adresse |
| `prix_base_jour`, `prix_intelligents_actif`, `prix_minimum` | Prix |
| `reduction_2j`, `reduction_7j`, `reduction_30j` | Prix |
| `prix_weekend_specifique` | Prix |
| `delai_reservation`, `duree_min`, `duree_max` | Mes conditions |
| `horaires_echange_cles`, `km_inclus_par_jour` | Prix / Mes conditions |
| `livraison_activee`, `prix_livraison` | Adresse et livraison |
| `documents_requis_si_bloque` | Modal "Débloquer" (lecture seule) |

---

## Flotte — 37 véhicules sur 3 comptes

### Compte 1 — gftechfrance@gmail.com (Tchendjou) — 22 véhicules

| ID | Immat | Modèle | Statut |
|---|---|---|---|
| 1701291 | CC647AX | Volkswagen Golf 2012 | Active |
| 1718469 | DJ709ND | Peugeot 5008 2014 | Active |
| 1719415 | DD246YC | Volkswagen Polo 2014 | Active |
| 1723359 | DY872FX | Volkswagen Polo 2017 | Incomplète |
| 1743579 | DH842CZ | Renault Clio 2014 | Active |
| 1743643 | DS608SZ | Renault Clio 2015 | Active |
| 1825239 | EK932TR | Renault Twingo III 2017 | Active (Asnières) |
| 1832153 | FM263KE | Volkswagen Tiguan 2015 | Active |
| 1832155 | DB706NK | Volkswagen Polo 2013 | Active |
| 1832157 | FF953CA | Renault Clio 2019 | Active |
| 1832158 | DS487GS | Renault Clio 2015 | Active |
| 1832159 | FY585AL | Toyota Aygo II 2021 | Active |
| 1842730 | EQ220LG | Peugeot 5008 2017 | Active |
| 1850231 | FF281DA | Peugeot 5008 2019 | En attente |
| 1859070 | BQ202KP | Audi A1 2011 | Active |
| 1913338 | FJ476HA | Ford C-Max 2019 | Active |
| 1931774 | GE608GA | Citroën C4 2022 | Prête à publier |
| 1935525 | EA453JZ | Peugeot 208 2016 | Incomplète |
| 1935533 | EV484XR | Peugeot 2008 2018 | Active |
| 1935539 | DS558PQ | Volkswagen Up! 2015 | Incomplète |
| 1935546 | FC195PA | Renault Mégane IV 2018 | Incomplète |
| 1935557 | DQ348JL | Peugeot 208 2015 | Incomplète |

### Compte 2 — garagepapafrance@gmail.com (Bleriot) — 10 véhicules

| ID | Immat | Modèle | Statut |
|---|---|---|---|
| 1878543 | FC665WH | Renault Clio Société 2018 | **Bloquée** |
| 1881118 | FJ400WH | Peugeot 208 2019 | Active |
| 1881415 | EK012DW | Renault Twingo 2017 | Active |
| 1881481 | EH976BN | Citroën C1 2016 | Active |
| 1881606 | FR187TS | Volkswagen Up! 2020 | **Bloquée** |
| 1881624 | DV322GT | Renault Twingo III 2015 | Active |
| 1881636 | FW551HD | Peugeot 108 2019 | Active |
| 1881645 | DQ253YB | Renault Clio 2015 | Active |
| 1881691 | GF921SK | Opel Corsa 2017 | Active |
| 1894418 | FX634RS | Renault Twingo 2012 | Active |

### Compte 3 — fotsing_cm@yahoo.fr (Ghislain) — 5 véhicules

| ID | Immat | Modèle | Statut |
|---|---|---|---|
| 716035 | ED964CK | Citroën C3 2016 | Active |
| 770586 | FT350HL | Fiat 500 2016 | En pause |
| 996091 | FH763KA | Renault Twingo 2019 | En pause |
| 1078506 | BT463XA | Renault Clio 2011 | Active |
| 1267184 | BR504ZJ | Renault Clio 2011 | **Bloquée** |

**Localisation** : 36 véhicules au 14 Rue des Pavillons, 92800 Puteaux. 1 véhicule (EK932TR, ID 1825239) au 256 Avenue Des Grésillons, 92600 Asnières-sur-Seine.

---

## Contrôle qualité Mission 1 — à chaque exécution

Fichier `qc_<snapshot_id>.json` contenant :
- Nb communes OK / KO sur 36, avec liste nominative des échecs
- Nb total d'annonces brutes, nb `annonce_id` distincts
- Nb lignes avec `prix_jour` manquant ou non numérique
- Nb lignes avec `commune_annonce` hors 92 (attendu non nul, pas une erreur)
- Variation % vs snapshot précédent du même pipeline

**Alertes** : < 30 communes abouties OU variation > 40% → signaler explicitement dans le rapport.

---

## Synthèse (pipelines A2 et B)

### 1. Inventaire du marché par ville

| Commune | Habitants | Véhicules | Saturation (véh/10k hab) | Prix min | Prix moy | Prix max |
|---|---|---|---|---|---|---|

### 2. Évolution depuis le dernier snapshot du même pipeline

- Véhicules disparus (potentiellement loués ou retirés)
- Nouveaux véhicules apparus

### 3. Positionnement de chaque véhicule actif de la flotte

Pour chaque véhicule identifié par `annonce_id` :
- Prix actuel vs marché (même catégorie → même ville → même tranche de prix → tout le marché)
- Indicateur : 🟢 COMPÉTITIF / 🟡 DANS LA MOYENNE / 🔴 CHER

### 4. Recommandation tarifaire

- Prix conseillé avec justification
- Impact estimé en €/mois

---

## Livrables

### Mission 1
- `getaround_<pipeline>_<snapshot_id>.csv` — UTF-8, séparateur virgule
- `qc_<snapshot_id>.json` — rapport qualité
- `getaround_history.csv` — cumulatif en ajout uniquement, dédoublonné sur (`snapshot_id`, `annonce_id`)
- `synthese_<pipeline>_<snapshot_id>.md` — synthèse (pipelines A2 et B uniquement)

### Mission 2
- `parametres_flotte_<date>.csv` — 37 lignes attendues
- `qc_mission2_<date>.json` — rapport qualité

---

## Fichiers du skill

```
skills/getaround-scraper/
├── SKILL.md                          ← Ce fichier
├── scripts/
│   ├── scrape_getaround.py           ← Mission 1 (3 pipelines)
│   ├── mission2_dashboard.py         ← Mission 2 (dashboard authentifié)
│   └── analyse.py                    ← Module d'analyse et synthèse
└── references/
    ├── mes_vehicules.json            ← 37 véhicules avec IDs exacts et 3 comptes
    └── populations.json              ← Population des 36 communes
```

---

## Usage

```bash
# Mission 1 — Pipeline A1 (recensement)
python3 skills/getaround-scraper/scripts/scrape_getaround.py --pipeline A1

# Mission 1 — Pipeline A2 (détection réservation)
python3 skills/getaround-scraper/scripts/scrape_getaround.py --pipeline A2

# Mission 1 — Pipeline B (pricing week-end)
python3 skills/getaround-scraper/scripts/scrape_getaround.py --pipeline B

# Mission 2 — Dashboard authentifié (lecture seule)
python3 skills/getaround-scraper/scripts/mission2_dashboard.py

# Test dry-run Mission 2 (sans scraper)
python3 skills/getaround-scraper/scripts/mission2_dashboard.py --dry-run

# Override villes (test rapide)
GETAROUND_CITIES=puteaux,courbevoie python3 skills/getaround-scraper/scripts/scrape_getaround.py --pipeline A2
```

---

## Tâches planifiées

| Pipeline | Cron | Description |
|---|---|---|
| A1 | `0 0 6 * * *` | Tous les jours à 06h00 |
| A2 (08h30) | `0 30 8 * * *` | Tous les jours à 08h30 — **Actif** |
| A2 (13h00) | `0 0 13 * * *` | Tous les jours à 13h00 |
| A2 (17h00) | `0 0 17 * * *` | Tous les jours à 17h00 |
| A2 (21h00) | `0 0 21 * * *` | Tous les jours à 21h00 |
| B | `0 0 10 * * 3,4,5` | Mer/Jeu/Ven à 10h00 |
