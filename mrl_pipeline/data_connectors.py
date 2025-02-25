"""data_connectors.py.

This module uses the Google Sheets API to fetch Google Sheets containing
prep_results data
"""

import json
import os
from typing import Optional

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
        service_account_path: Optional[str] = None,
        service_account_env_var: Optional[str] = None,
        scopes: Optional[str] = None,
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
        elif service_account_env_var:
            credentials = Credentials.from_service_account_info(
                json.loads(os.getenv(service_account_env_var)),
            ).with_scopes(self.scopes)
        else:
            msg = "No service account provided"
            raise ValueError(msg)

        # Build the Drive service
        self.service = build("drive", "v3", credentials=credentials)

    def get_most_recent_gsheet_by_name(
        self,
        sheet_name: str,
    ) -> Optional[dict[str, str]]:
        """Retrieves the most recent Google Sheet file id by its name.

        Args:
            sheet_name (str): The name of the Google Sheet to search for.

        Returns:
            string or None: A dictionary containing the drive ID and name of the most
            recent Google Sheet matching the specified name, or None if no such file is
            found.

        """
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

    def get_gsheets_by_prefix(self, prefix: Optional[str] = None) -> pd.DataFrame:
        """Retrieve Google Sheets files from Google Drive that match a given prefix.

        This method queries Google Drive for Google Sheets whose names contain the
        specified prefix. It returns a pandas DataFrame containing the file
        names, IDs, and modification times, filtered and sorted by the prefix.

        Args:
            prefix (Optional[str]): The prefix to filter file names. If None,
                        defaults to "prep_results".

        Returns:
            pd.DataFrame: A DataFrame with columns 'race_id', 'file_id',
                  'modified_at', and 'source_type', containing the
                  filtered and sorted list of Google Sheets files.

        """
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
                    "id": "file_id",
                    "modifiedTime": "modified_at",
                },
            )
            .assign(
                source_type="google_sheets",
            )
            .sort_values(by=["race_id", "modified_at"], ascending=[True, False])
        )
