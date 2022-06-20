import os
import time
import requests
import pandas as pd
import numpy as np

from pandas import DataFrame
from typing import List
from typing import Optional, Dict, Union, Any
from requests import Response
from authlib.jose import jwt
from google.oauth2 import service_account
from googleapiclient import discovery

ZAPI_KEY=""
ZAPI_SECRET=""
ZMEETING_ID=""
RUBRIC=
DFOLDER=""
SERVICE_FILE= ""
SCOPES= ["https://www.googleapis.com/auth/drive", "https://www.googleapis.com/auth/drive.file"]



class Googl:
    def __init__(self, service_file: str, scopes: List[str]):
        self.service_file = ""
        self.scopes = scopes
        self.google_sheet_type = "application/vnd.google-apps.spreadsheet"
        self.creds = service_account.Credentials.from_service_account_file(self.service_file,
                                                                           scopes=self.scopes)
        self.drive = discovery.build("drive", "v3", credentials=self.creds)
        self.sheets = discovery.build("sheets", "v4", credentials=self.creds)

    def get_folder_id(self, folder_name: str) -> str:
        folders: dict = self.drive.files().list(q="mimeType='application/vnd.google-apps.folder'").execute()
        #print(folders)
        folder_id = [x.get("id") for x in folders.get("files") if x.get("name") == folder_name]

        return folder_id[0]

    def create_new_sheet(self, fname: str, pfolder_id: str) -> str:
        new_sheet_metadata = {
            "name": fname,
            "parents": [pfolder_id],
            "mimeType": self.google_sheet_type
        }

        new_sheet = self.drive.files().create(body=new_sheet_metadata).execute()
        #print(new_sheet)

        return new_sheet.get("id")

    def insert_df_to_sheet(self, google_sheet_id: str, df: DataFrame) -> dict:
        response = self.sheets.spreadsheets().values().append(
            spreadsheetId=google_sheet_id,
            valueInputOption="RAW",
            range="A1",
            body={"majorDimension": "ROWS",
                  "values": df.T.reset_index().T.values.tolist()}
        ).execute()

        return response

    def get_sheet_link(self, google_sheet_id: str,
                       return_all_fields: bool = False, fields_to_return: str = "webViewLink"):
        fields = "*" if return_all_fields else fields_to_return
        response = self.drive.files().get(fileId=google_sheet_id, fields=fields).execute()

        return response


class Zoom:
    def __init__(self, key: str, secret: str):
        self.key = ZAPI_KEY
        self.secret = ZAPI_SECRET
        self.base_url = "https://api.zoom.us/v2"
        self.reports_url = f"{self.base_url}/report/meetings"
        self.jwt_token_exp = 5400
        self.jwt_token_algo = "HS256"

    def get_meeting_participants(self, meeting_id: str, jwt_token: bytes,
                                 next_page_token: Optional[str] = None) -> Response:
        url: str = f"{self.reports_url}/{meeting_id}/participants"
        query_params: Dict[str, Union[int, str]] = {"page_size": 300}
        if next_page_token:
            query_params.update({"next_page_token": next_page_token})

        r: Response = requests.get(url,
                                   headers={"Authorization": f"Bearer {jwt_token.decode('utf-8')}"},
                                   params=query_params)

        return r

    def generate_jwt_token(self) -> bytes:
        iat = int(time.time())

        jwt_payload: Dict[str, Any] = {
            "aud": None,
            "iss": self.key,
            "exp": iat + self.jwt_token_exp,
            "iat": iat
        }

        header: Dict[str, str] = {"alg": self.jwt_token_algo}

        jwt_token: bytes = jwt.encode(header, jwt_payload, self.secret)

        return jwt_token


if __name__ == "__main__":
    zoom = Zoom(ZAPI_KEY, ZAPI_SECRET)

    jwt_token: bytes = zoom.generate_jwt_token()
    response: Response = zoom.get_meeting_participants(ZMEETING_ID, jwt_token)
    list_of_participants: List[dict] = response.json().get("participants")

    while token := response.json().get("next_page_token"):
        response = zoom.get_meeting_participants(ZMEETING_ID, jwt_token, token)
        list_of_participants += response.json().get("participants")

    df: DataFrame = pd.DataFrame(list_of_participants).drop(columns=["attentiveness_score"])
    df.join_time = pd.to_datetime(df.join_time).dt.tz_convert("US/Central")
    df.leave_time = pd.to_datetime(df.leave_time).dt.tz_convert("US/Central")

    df.sort_values(["id", "name", "join_time"], inplace=True)

    output_df: DataFrame = df.groupby(["name"]) \
        .agg({"duration": ["sum"], "join_time": ["min"], "leave_time": ["max"]}) \
        .reset_index() \
        .rename(columns={"duration": "Tiempoenclase", "name":"Nombre","join_time":"Ingreso","leave_time":"Salida"})

    output_df.columns = output_df.columns.get_level_values(0)

    output_df.Tiempoenclase = round(output_df.Tiempoenclase / 60, 2)

    output_df.Ingreso = output_df.Ingreso.dt.strftime("%Y-%m-%d %H:%M:%S")
    output_df.Salida = output_df.Salida.dt.strftime("%Y-%m-%d %H:%M:%S")
    
    output_df['Asistencia']=np.where(output_df['Tiempoenclase'] >= RUBRIC,'Asistencia','Tiempo insuficiente')

    meeting_date: str = output_df.Ingreso.tolist()[0].split(" ")[0]

    output_file: str = f"zoom_report_{meeting_date}"

    googl = Googl(SERVICE_FILE, SCOPES)

    zoom_folder_id: str = googl.get_folder_id(DFOLDER)
    sheet_id = googl.create_new_sheet(output_file, zoom_folder_id)
    result = googl.insert_df_to_sheet(sheet_id, output_df)
    sheet_link = googl.get_sheet_link(result.get("spreadsheetId"))

    print(f"Reporte listo en GDrive.\n"
          f"link: {sheet_link}")

