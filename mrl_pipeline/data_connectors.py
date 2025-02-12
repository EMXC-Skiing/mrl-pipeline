"""data_connectors.py.

This module uses the Google Sheets API to fetch Google Sheets containing
prep_results data
"""

from __future__ import annotations

import json
import os

import pandas as pd
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build


class DriveService:
    """A service class to interact with Google Drive API to fetch Google Sheets.

    Attributes:
        credentials (google.oauth2.credentials.Credentials): The credentials for
            authenticating with Google Drive API. service
        (googleapiclient.discovery.Resource): The Google Drive service object.

    Methods:
        get_most_recent_gsheet_by_name(sheet_name):
            Fetches the most recent Google Sheet by its name.

        get_gsheets_by_prefix(prefix="prep_results"):
            Fetches all Google Sheets whose names contain the given prefix.

    """

    def __init__(
        self,
        service_account_path: str | None = None,
        servive_account_env_var: str | None = None,
        scopes: str | None = None,
    ) -> None:
        """Initialize the data connector with the provided authentication."""
        # Set default scopes
        if scopes is None:
            scopes = ["https://www.googleapis.com/auth/drive.metadata.readonly"]
        self.scopes = scopes

        # Load credentials
        if service_account_path:
            credentials = Credentials.from_service_account_file(
                service_account_path,
            ).with_scopes(self.scopes)
        elif servive_account_env_var:
            credentials = Credentials.from_service_account_info(
                json.loads(os.getenv(servive_account_env_var)),
            ).with_scopes(self.scopes)
        else:
            msg = "No service account provided"
            raise ValueError(msg)

        # Build the Drive service
        self.service = build("drive", "v3", credentials=credentials)

    def get_most_recent_gsheet_by_name(self, sheet_name: str):
        # Define the query to filter for Google Sheets
        gsheets_query = (
            "mimeType='application/vnd.google-apps.spreadsheet' "
            f"and name='{sheet_name}'"
        )

        # Search all files including shared drives
        results = (
            self.service.files()
            .list(
                q=gsheets_query,
                includeItemsFromAllDrives=True,  # Include files from shared drives
                supportsAllDrives=True,  # Support both My Drive and shared drives
                fields="files(id, name)",
                orderBy="createdTime desc",
            )
            .execute()
        )

        files = results.get("files", [])

        if files:
            return files[0]
        return None

    def get_gsheets_by_prefix(self, prefix: str | None = None):
        files = []
        page_token = None
        self.prefix = "prep_results" if prefix is None else prefix

        # Define the query to filter for Google Sheets
        query = "mimeType='application/vnd.google-apps.spreadsheet' "
        f"and name contains '{prefix}'"

        while True:
            response = (
                self.service.files()
                .list(
                    q=query,
                    includeItemsFromAllDrives=True,  # Include files from shared drives
                    supportsAllDrives=True,  # Support both My Drive and shared drives
                    fields="nextPageToken, files(name, id, modifiedTime)",
                    pageSize=1000,
                    pageToken=page_token,
                    orderBy="name",
                )
                .execute()
            )

            files.extend(response.get("files", []))  # Add results to the list

            page_token = response.get("nextPageToken")  # Get next page token
            if not page_token:
                break  # Exit loop if there are no more pages

        # Filter the list of files efficiently
        return (
            pd.DataFrame([file for file in files if file["name"].startswith(prefix)])
            .rename(
                columns={
                    "name": "race_id",
                    "id": "source_file_id",
                    "modifiedTime": "modified_at",
                },
            )
            .sort_values(by=["race_id", "modified_at"], ascending=[True, False])
        )

        # note: could filter down to most recent of each result sheet to guarantee
        # uniqueness by name
        # BUT would probably rather write a test for this to ensure there
        # aren't multiple versions of a result sheet floating around
        # df_files = df_files.groupby('race_id').nth(0).reset_index()
