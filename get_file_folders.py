import requests
import sys
from pprint import pprint

BASE_URL = "https://api.rentman.net"
TOKEN = ("eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJtZWRld2Vya2VyIjoxODYwLCJhY2NvdW50IjoiaXRpbmVyYXBybyIsImNsaWVudF90eXBl"
         "Ijoib3BlbmFwaSIsImNsaWVudC5uYW1lIjoib3BlbmFwaSIsImV4cCI6MjA1ODU5NzU2MSwiaXNzIjoie1wibmFtZVwiOlwiYmFja2VuZFwi"
         "LFwidmVyc2lvblwiOlwiNC43MjguMC4zXCJ9IiwiaWF0IjoxNzQzMDY4MzYxfQ.AqegIhlTftQkz_T4WtJIpTpY1E1_vgNP0uT5SzoNE9w")

HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Accept": "application/json",
    "Content-Type": "application/json",
}

def fetch_project_file_folders(project_id: int):
    """Restituisce la collezione di cartelle file per il progetto indicato."""
    url = f"{BASE_URL}/projects/{project_id}/file_folders"
    response = requests.get(url, headers=HEADERS, timeout=20)
    response.raise_for_status()
    payload = response.json()
    return payload.get("data", [])

def main():
    if len(sys.argv) < 2:
        print("Uso: python get_file_folders.py <project_id>")
        sys.exit(1)

    project_id = int(sys.argv[1])
    folders = fetch_project_file_folders(project_id)

    print(f"📁 Cartelle trovate per il progetto {project_id}: {len(folders)}")
    for folder in folders:
        info = {
            "id": folder.get("id"),
            "name": folder.get("displayname") or folder.get("readable_name"),
            "parent": folder.get("parent"),
            "path": folder.get("path"),
        }
        pprint(info)

if __name__ == "__main__":
    main()