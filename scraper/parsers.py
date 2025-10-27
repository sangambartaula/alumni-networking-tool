import re
from bs4 import BeautifulSoup

def extract_people(html: str):
    """
    Adjust selectors to match your target site.
    Keeps return schema stable for downstream CSV.
    """
    soup = BeautifulSoup(html, "html.parser")
    cards = soup.select(".alumni-card, .person-card, .profile-tile, li.alumni")
    def T(el): return el.get_text(strip=True) if el else ""

    for c in cards:
        name   = T(c.select_one(".alumni-name, h3, .name"))
        degree = T(c.select_one(".alumni-degree, .degree"))
        year   = re.sub(r"\D", "", T(c.select_one(".alumni-year, .grad-year")))
        major  = T(c.select_one(".alumni-major, .major, .department"))
        loc    = T(c.select_one(".alumni-location, .location"))

        if name:
            yield {
                "name": name,
                "degree": degree,
                "year": year,
                "major_or_department": major,
                "location": loc,
            }
