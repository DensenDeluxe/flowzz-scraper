#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import time
import requests
import mysql.connector

# -----------------------------------------------------------
# KONFIGURATION
# -----------------------------------------------------------
DB_CONFIG = {
    'host': '###',
    'database': '###',
    'user': '###',
    'password': '###'
}

FLOWZZ_BASE = "https://flowzz.com/api"
PAGE_SIZE = 25

# Pause zwischen Apotheken-Inserts (zur Schonung)
PER_APOTHEKE_DELAY = 1

# Anzahl 429-Fehler in Folge, die wir akzeptieren, bevor Abbruch
MAX_CONSECUTIVE_429 = 5

# --------------------------------------------
# Hier die Cookies aus deinem Browser kopieren:
# ACHTUNG: Dies ist nur ein Beispiel aus dem geposteten Netzwerk-Log!
# Vermutlich fehlt noch ein Session-Token, z.B. '__Secure-next-auth.session-token'
# --------------------------------------------
SESSION_COOKIES = {
    "__Host-next-auth.csrf-token": "###",
    "__Secure-next-auth.callback-url": "https%3A%2F%2Fflowzz.com",
    # Falls du noch andere Cookies brauchst, hier ergänzen, z.B.:
    # "__Secure-next-auth.session-token": "abcdefg..."
    # ...
}

# -----------------------------------------------------------
# SQL-DDL (Tabellen definitions)
# -----------------------------------------------------------
TABLES = {}

TABLES['apotheken'] = """
CREATE TABLE IF NOT EXISTS apotheken (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) UNIQUE,
    adresse TEXT NULL,
    email VARCHAR(255) NULL,
    telefon VARCHAR(100) NULL,
    homepage VARCHAR(255) NULL,
    anzahl_produkte INT NULL,
    durchschnittspreis VARCHAR(50) NULL,
    profil_url TEXT NULL
) ENGINE=InnoDB;
"""

TABLES['stammdaten'] = """
CREATE TABLE IF NOT EXISTS stammdaten (
    id INT AUTO_INCREMENT PRIMARY KEY,
    flowzz_id INT NOT NULL UNIQUE,
    product_url VARCHAR(255) NOT NULL,
    product_img_url VARCHAR(255) NOT NULL,
    product_name VARCHAR(255) NULL,
    product_category VARCHAR(50) NULL,
    product_genetic VARCHAR(50) NULL,
    product_cultivar VARCHAR(255) NULL,
    product_irradiation ENUM('Bestrahlt','Unbestrahlt') NULL,
    product_grower VARCHAR(255) NULL,
    product_origin VARCHAR(255) NULL,
    product_importeur VARCHAR(255) NULL,
    product_rating DECIMAL(3,1) NULL,
    product_rating_count INT NULL,
    product_thc DECIMAL(5,2) NULL,
    product_thc_unit VARCHAR(10) NULL,
    product_cbd DECIMAL(5,2) NULL,
    product_cbd_unit VARCHAR(10) NULL,
    product_delivery_status VARCHAR(50) NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB;
"""

def create_tables(cnx):
    cursor = cnx.cursor()
    for name, ddl in TABLES.items():
        print(f"Erstelle/prüfe Tabelle {name} ...")
        cursor.execute(ddl)
    cursor.close()

# -----------------------------------------------------------
# INSERT-FUNKTIONEN
# -----------------------------------------------------------
def insert_stammdaten(cnx, product_data):
    sql = """
    INSERT INTO stammdaten (
        flowzz_id,
        product_url, product_img_url, product_name,
        product_category, product_genetic, product_cultivar, product_irradiation,
        product_grower, product_origin, product_importeur,
        product_rating, product_rating_count,
        product_thc, product_thc_unit,
        product_cbd, product_cbd_unit,
        product_delivery_status
    )
    VALUES (
        %(flowzz_id)s,
        %(product_url)s, %(product_img_url)s, %(product_name)s,
        %(product_category)s, %(product_genetic)s, %(product_cultivar)s, %(product_irradiation)s,
        %(product_grower)s, %(product_origin)s, %(product_importeur)s,
        %(product_rating)s, %(product_rating_count)s,
        %(product_thc)s, %(product_thc_unit)s,
        %(product_cbd)s, %(product_cbd_unit)s,
        %(product_delivery_status)s
    )
    ON DUPLICATE KEY UPDATE
        product_name = VALUES(product_name),
        product_category = VALUES(product_category),
        product_genetic = VALUES(product_genetic),
        product_cultivar = VALUES(product_cultivar),
        product_irradiation = VALUES(product_irradiation),
        product_grower = VALUES(product_grower),
        product_origin = VALUES(product_origin),
        product_importeur = VALUES(product_importeur),
        product_rating = VALUES(product_rating),
        product_rating_count = VALUES(product_rating_count),
        product_thc = VALUES(product_thc),
        product_thc_unit = VALUES(product_thc_unit),
        product_cbd = VALUES(product_cbd),
        product_cbd_unit = VALUES(product_cbd_unit),
        product_delivery_status = VALUES(product_delivery_status),
        updated_at = CURRENT_TIMESTAMP
    """
    cursor = cnx.cursor()
    cursor.execute(sql, product_data)
    cnx.commit()
    cursor.close()

def insert_stammdaten_list(cnx, products):
    for p in products:
        insert_stammdaten(cnx, p)

def insert_apotheke(cnx, apo_data):
    sql = """
    INSERT INTO apotheken (
        name, adresse, email, telefon, homepage, 
        anzahl_produkte, durchschnittspreis, profil_url
    )
    VALUES (
        %(name)s, %(adresse)s, %(email)s, %(telefon)s, %(homepage)s,
        %(anzahl_produkte)s, %(durchschnittspreis)s, %(profil_url)s
    )
    ON DUPLICATE KEY UPDATE
        adresse = VALUES(adresse),
        email = VALUES(email),
        telefon = VALUES(telefon),
        homepage = VALUES(homepage),
        anzahl_produkte = VALUES(anzahl_produkte),
        durchschnittspreis = VALUES(durchschnittspreis),
        profil_url = VALUES(profil_url)
    """
    cursor = cnx.cursor()
    cursor.execute(sql, apo_data)
    cnx.commit()
    cursor.close()

# -----------------------------------------------------------
# API-FUNKTIONEN
# -----------------------------------------------------------
def get_flowzz_products(page=1, page_size=25, category="flowers"):
    """
    Ruft /api/v1/views/{flowers|extracts} ab => Liste
    """
    url = f"{FLOWZZ_BASE}/v1/views/{category}"
    params = {
        "pagination[page]": page,
        "pagination[pageSize]": page_size,
        "avail": 0
    }
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; FlowzzScraper/1.0)"
    }
    try:
        r = requests.get(url, params=params, headers=headers, timeout=10)
    except requests.RequestException as e:
        print("[!] Verbindungsfehler:", e)
        return None

    if r.status_code != 200:
        print(f"[!] {url} => HTTP {r.status_code}, {r.text[:200]}")
        return None

    try:
        return r.json()
    except ValueError:
        print("[!] Fehler: Antwort kein JSON")
        return None

def get_flowzz_vendors_new(flowzz_id):
    """
    NEUER Endpunkt: /api/v1/views/vendors/price/2/<flowzz_id>
    laut deinem Fetch-Log

    Man braucht einen gültigen Session-Cookie.
    """
    url = f"{FLOWZZ_BASE}/v1/views/vendors/price/2/{flowzz_id}"
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; FlowzzScraper/1.0)",
        "Accept": "application/json"
    }
    try:
        r = requests.get(url, headers=headers, cookies=SESSION_COOKIES, timeout=10)
    except requests.RequestException as e:
        print("[!] /api/v1/views/vendors/price Verbindungsfehler:", e)
        return None

    if r.status_code == 200:
        try:
            return r.json()
        except ValueError:
            print("[!] Ungültige JSON-Antwort bei /vendors/price")
            return None
    elif r.status_code == 429:
        return "RATE_LIMIT"
    else:
        print(f"[!] /vendors/price => {r.status_code}, {r.text[:200]}")
        return None

# -----------------------------------------------------------
# MAPPING-FUNKTIONEN
# -----------------------------------------------------------
def map_flowzz_to_stammdaten(item, category):
    if not isinstance(item, dict):
        return None

    flowzz_id = item.get("id")
    if flowzz_id is None:
        return None

    product_url = f"https://flowzz.com/{category}/{flowzz_id}"
    product_img_url = ""

    irr = item.get("irradiated")
    if irr is True:
        irradiation = "Bestrahlt"
    elif irr is False:
        irradiation = "Unbestrahlt"
    else:
        irradiation = None

    rating = item.get("ratings_score")
    if rating is not None:
        try:
            rating = float(rating)
        except:
            rating = None

    rev_count = item.get("ratings_count")
    if rev_count is not None:
        try:
            rev_count = int(rev_count)
        except:
            rev_count = None

    return {
        "flowzz_id": flowzz_id,
        "product_url": product_url,
        "product_img_url": product_img_url,
        "product_name": item.get("name"),
        "product_category": category,
        "product_genetic": item.get("genetic"),
        "product_cultivar": item.get("strain_name"),
        "product_irradiation": irradiation,
        "product_grower": item.get("producer_name"),
        "product_origin": item.get("origin"),
        "product_importeur": None,
        "product_rating": rating,
        "product_rating_count": rev_count,
        "product_thc": item.get("thc"),
        "product_thc_unit": "%",
        "product_cbd": item.get("cbd"),
        "product_cbd_unit": "%",
        "product_delivery_status": None,
    }

def map_flowzz_vendor_to_apotheke(vendor_item):
    if not isinstance(vendor_item, dict):
        return None
    return {
        "name": vendor_item.get("vendor_name"),
        "adresse": vendor_item.get("address"),
        "email": None,
        "telefon": vendor_item.get("phone"),
        "homepage": vendor_item.get("homepage"),
        "anzahl_produkte": None,
        "durchschnittspreis": None,
        "profil_url": None
    }

# -----------------------------------------------------------
# SCRAPING-FUNKTIONEN
# -----------------------------------------------------------
def scrape_flowzz_stammdaten(cnx):
    """
    Lädt alle flowers/extracts seitenweise und speichert in stammdaten
    """
    for category in ["flowers", "extracts"]:
        page = 1
        while True:
            print(f"==> Hole {category} - Seite {page} ...")
            data_json = get_flowzz_products(page=page, page_size=PAGE_SIZE, category=category)
            if not data_json:
                print(f"[!] Keine oder fehlerhafte Antwort für {category}, Seite={page}. Abbruch.")
                break

            inner_data = data_json.get("data", {})
            products = inner_data.get("data", [])
            if not products:
                print(f"[!] Keine Produkte mehr in {category}, Seite={page}.")
                break

            mapped_list = []
            for item in products:
                row = map_flowzz_to_stammdaten(item, category)
                if row:
                    mapped_list.append(row)

            insert_stammdaten_list(cnx, mapped_list)

            meta = inner_data.get("meta", {})
            pagination = meta.get("pagination", {})
            page_count = pagination.get("pageCount", page)
            if page >= page_count:
                break
            page += 1


def scrape_vendor_data_newendpoint(cnx):
    """
    Verwendet den NEUEN Endpunkt /api/v1/views/vendors/price/2/<id>
    für die Apotheken-/Preisdaten.
    Braucht gültige Cookies => SESSION_COOKIES.
    """
    cursor = cnx.cursor(dictionary=True)
    cursor.execute("SELECT id, flowzz_id FROM stammdaten")
    rows = cursor.fetchall()
    cursor.close()

    consecutive_429 = 0

    for row in rows:
        product_id = row["flowzz_id"]

        max_retries = 3
        attempt = 0
        while attempt < max_retries:
            attempt += 1
            result = get_flowzz_vendors_new(product_id)

            if result == "RATE_LIMIT":
                consecutive_429 += 1
                print(f"[!] 429 Rate-Limit => warte 10s. Versuch {attempt}/{max_retries}")
                time.sleep(10)
                if consecutive_429 >= MAX_CONSECUTIVE_429:
                    print("[!] Zu viele 429 in Folge => Abbruch")
                    return
            elif result is None:
                print(f"[!] Keine Vendor-Daten für ID={product_id}")
                break
            else:
                # Erfolg => 429-Zähler reset
                consecutive_429 = 0
                # Vermutlich result = {"vendors": [...]} oder "data": [...]
                # Musst schauen, wie das JSON genau aufgebaut ist
                vendors_list = result.get("vendors") or result.get("data") or []
                print(f"   => {len(vendors_list)} Apotheken bei ID={product_id}")

                for v in vendors_list:
                    apo_data = map_flowzz_vendor_to_apotheke(v)
                    if apo_data and apo_data.get("name"):
                        print(f"       - Speichere: {apo_data['name']}")
                        insert_apotheke(cnx, apo_data)
                        time.sleep(PER_APOTHEKE_DELAY)  # Pause
                break


def main():
    try:
        cnx = mysql.connector.connect(**DB_CONFIG)
        print("Mit der Datenbank verbunden.")
    except mysql.connector.Error as err:
        print("[!] DB-Verbindungsfehler:", err)
        return

    create_tables(cnx)

    print("[+] Schritt 1: Stammdaten laden (flowers + extracts)")
    scrape_flowzz_stammdaten(cnx)

    print("[+] Schritt 2: Vendor-Daten => Apotheken (Neuer Endpunkt /api/v1/views/vendors/price/2/...)")
    scrape_vendor_data_newendpoint(cnx)

    print("Fertig. Verbindung wird geschlossen.")
    cnx.close()


if __name__ == "__main__":
    main()
