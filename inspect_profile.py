
import requests
from bs4 import BeautifulSoup
import re

url = "https://www.cricbuzz.com/profiles/1114/paul-stirling"
headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

try:
    r = requests.get(url, headers=headers)
    soup = BeautifulSoup(r.text, "html.parser")
    
    # Print the specific section containing personal info
    # Usuallly "Born" or "Birth Place"
    
    print("--- SEARCHING FOR 'Born' ---")
    born = soup.find(string=re.compile("Born"))
    if born:
        print(born.parent.parent.prettify())
    else:
        print("Born text not found")

    print("\n--- SEARCHING FOR 'Birth Place' ---")
    bp = soup.find(string=re.compile("Birth Place"))
    if bp:
        print(bp.parent.parent.prettify())
    else:
        print("Birth Place text not found")
        
    print("\n--- SEARCHING FOR 'Role' ---")
    role = soup.find(string=re.compile("Role"))
    if role:
        print(role.parent.parent.prettify())

except Exception as e:
    print(e)
