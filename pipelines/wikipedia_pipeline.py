from bs4 import BeautifulSoup
import requests
import json
import pandas as pd
import os
import googlemaps
from dotenv import load_dotenv

dotenv_path = os.path.join(os.path.dirname(__file__), "../.env")
load_dotenv(dotenv_path)

account_key = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
google_maps_key = os.getenv("GOOGLE_MAPS_GEOCODER_KEY")

NO_IMAGE = "https://upload.wikimedia.org/wikipedia/commons/thumb/0/0a/" + \
"No-image-available.png/480px-No-image-available.png"

def get_wikipedia_page(url):
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return get_wikipedia_data(response.text)

    except requests.exceptions.RequestException as e:
        print(e)
        return None

def get_wikipedia_data(response):
    soup = BeautifulSoup(response, "html.parser")
    table = soup.select('table[class*="wikitable sortable"]')[0].find("tbody")
    rows = table.findAll("tr")[1:]
    return rows

def extract_data_from_wikipedia(**kwargs):
    url = kwargs["url"]
    rows = get_wikipedia_page(url) if url else []

    data = []

    for i in range(0, len(rows)):
        r = rows[i]
        tds = r.findAll("td")

        r_rank = clean_text(r.find("th").text) if r.find("th") else "N/A"
        r_stadium = clean_text(tds[0].text)
        r_seating_capacity = clean_text(tds[1].text)
        r_region = clean_text(tds[2].text)
        r_country = clean_text(tds[3].text)
        r_city = clean_text(tds[4].text)
        r_image = (
            tds[5].find("img").get("src").split("//")[1]
            if tds[5].find("img")
            else "NO_IMAGE"
        )
        r_home_team = clean_text(tds[6].text)

        values = {
            "rank": r_rank,
            "stadium": r_stadium,
            "seating_capacity": r_seating_capacity,
            "region": r_region,
            "country": r_country,
            "city": r_city,
            "image": r_image,
            "home_team": r_home_team,
        }

        data.append(values)

    json_rows = json.dumps(data)
    kwargs["ti"].xcom_push(key="rows", value=json_rows)

    return "OK"

def transform_wikipedia_data(**kwargs):
    data = kwargs["ti"].xcom_pull(key="rows", task_ids="extract_data_from_wikipedia")

    data = json.loads(data)

    stadiums_df = pd.DataFrame(data)

    stadiums_df["rank"] = stadiums_df["rank"].astype(int)
    stadiums_df["seating_capacity"] = (
        stadiums_df["seating_capacity"].str.replace(",", "").astype(int)
    )
    stadiums_df["image"] = stadiums_df["image"].apply(
        lambda x: f"https://{x}" if x not in ["NO_IMAGE", "", None] else NO_IMAGE
    )

    stadiums_df["home_team"] = (
        stadiums_df["home_team"].replace("", None).replace("N/A", None)
    )

    stadiums_df["latitude"], stadiums_df["longitude"] = zip(
        *stadiums_df.apply(
            lambda x: get_geolocation(x["stadium"], x["city"], x["country"]), axis=1
        )
    )

    stadiums_df["latitude"] = stadiums_df["latitude"].astype(float)
    stadiums_df["longitude"] = stadiums_df["longitude"].astype(float)

    # Handle duplicates
    duplicates = stadiums_df[stadiums_df.duplicated(subset=["stadium"])]
    duplicates["latitude"], duplicates["longitude"] = zip(
        *duplicates.apply(
            lambda x: get_geolocation(x["stadium"], x["city"], x["country"]), axis=1
        )
    )
    stadiums_df.update(duplicates)

    # Push the transformed data to XCom
    kwargs["ti"].xcom_push(key="rows", value=stadiums_df.to_json(orient="records"))

    return "OK"

def write_wikipedia_data(**kwargs):
    from datetime import datetime
    
    data = kwargs["ti"].xcom_pull(key="rows", task_ids="transform_wikipedia_data")

    data = json.loads(data)
    data = pd.DataFrame(data)

    file_name = (
        "stadium_cleaned_"
        + str(datetime.now().date())
        + "_"
        + str(datetime.now().time()).replace(":", "_")
        + ".csv"
    )

    # data.to_csv("data/" + file_name, index=False)

    data.to_csv(
        "abfs://soccer-stadium-analytics-container@soccerstadiumanalyticssa.dfs.core.windows.net/data/"
        + file_name,
        storage_options={
            "account_key": account_key,
        },
        index=False,
    )


def clean_text(text):
    text = str(text).strip()
    text = text.replace("&nbsp", "")
    if " ♦" in text:
        text = text.split(" ♦")[0]
    if "[" in text:
        text = text.split("[")[0]
    if " (formerly)" in text:
        text = text.split(" (formerly)")[0]
    return text.replace("\n", "").strip()


# def parse_home_teams(text):
#     if not text or text.strip() == "":
#         return []

#     parts = [
#         part.strip() for part in re.split(r",\s|,\s?and\s| and ", text) if part.strip()
#     ]

#     result = []
#     for part in parts:
#         if " and " in part and not part.startswith("and "):
#             sub_parts = part.split(" and ")
#             result.extend([sub_part.strip() for sub_part in sub_parts])
#         else:
#             result.append(part)

#     clean_result = []
#     for item in result:
#         cleaned_item = re.sub(
#             r"\bsome\b|\bmatch\b|\bmatches\b", "", item, flags=re.IGNORECASE
#         ).strip()
#         clean_result.append(cleaned_item)

#     consolidated_result = []
#     for item in clean_result:
#         if re.search(r"men's and women's", item, re.IGNORECASE):
#             consolidated_result.append(
#                 re.sub(
#                     r"men's and women's.*",
#                     "national football team",
#                     item,
#                     flags=re.IGNORECASE,
#                 ).strip()
#             )
#         else:
#             consolidated_result.append(item)

#     return consolidated_result

def get_geolocation(stadium, city, country):
    gmaps = googlemaps.Client(key=google_maps_key)

    results = gmaps.geocode(f"{stadium}, {city}, {country}")

    if results and "geometry" in results[0]:
        location = results[0]["geometry"]["location"]

        if location:
            return location["lat"], location["lng"]

    return None, None
