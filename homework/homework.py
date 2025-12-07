"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel


def clean_campaign_data():
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months



    """

    import os
    import zipfile
    import pandas as pd

    def ensure_output_folder(path="files/output"):
        os.makedirs(path, exist_ok=True)

    def load_zip_csv(zip_path):
        with zipfile.ZipFile(zip_path, "r") as archive:
            csv_inside = archive.namelist()[0]
            with archive.open(csv_inside) as csv_file:
                return pd.read_csv(csv_file)

    def load_all_data(n_files=10, input_folder="files/input"):
        frames = []
        for i in range(n_files):
            path = f"{input_folder}/bank-marketing-campaing-{i}.csv.zip"
            frames.append(load_zip_csv(path))
        return pd.concat(frames, ignore_index=True)

    def process_client_table(df):
        client = df[
            ["client_id", "age", "job", "marital", "education", "credit_default", "mortgage"]
        ].copy()
        client["job"] = (
            client["job"]
            .str.replace(".", "", regex=False)
            .str.replace("-", "_", regex=False)
        )
        client["education"] = (
            client["education"]
            .str.replace(".", "_", regex=False)
            .replace("unknown", pd.NA)
        )
        client["credit_default"] = (client["credit_default"] == "yes").astype(int)
        client["mortgage"] = (client["mortgage"] == "yes").astype(int)

        return client

    def process_campaign_table(df):
        campaign = df[
            [
                "client_id",
                "number_contacts",
                "contact_duration",
                "previous_campaign_contacts",
                "previous_outcome",
                "campaign_outcome",
                "day",
                "month",
            ]
        ].copy()
        campaign["previous_outcome"] = (campaign["previous_outcome"] == "success").astype(int)
        campaign["campaign_outcome"] = (campaign["campaign_outcome"] == "yes").astype(int)
        month_map = {
            "jan": "01", "feb": "02", "mar": "03", "apr": "04",
            "may": "05", "jun": "06", "jul": "07", "aug": "08",
            "sep": "09", "oct": "10", "nov": "11", "dec": "12"
        }
        campaign["last_contact_date"] = (
            "2022-" +
            campaign["month"].map(month_map) +
            "-" +
            campaign["day"].astype(str).str.zfill(2)
        )
        campaign.drop(columns=["day", "month"], inplace=True)

        return campaign

    def process_economics_table(df):
        return df[["client_id", "cons_price_idx", "euribor_three_months"]].copy()

    ensure_output_folder()
    data = load_all_data()
    client_df = process_client_table(data)
    campaign_df = process_campaign_table(data)
    econ_df = process_economics_table(data)
    client_df.to_csv("files/output/client.csv", index=False)
    campaign_df.to_csv("files/output/campaign.csv", index=False)
    econ_df.to_csv("files/output/economics.csv", index=False)

    return

if __name__ == "__main__":
    clean_campaign_data()
