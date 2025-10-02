"""data_connectors.py. This module uses the Google Sheets API to fetch Google
Sheets containing prep_results data
"""


from typing import Dict, List, Optional

import pandas as pd

import json
import os

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build






class DriveService:
    """A service class to interact with Google Drive API to fetch Google
    Sheets.

    Attributes:
        credentials (google.oauth2.credentials.Credentials): The credentials
            for authenticating with Google Drive API.
        service (googleapiclient.discovery.Resource): The Google Drive service
            object.

    Methods:
        get_most_recent_gsheet_by_name(sheet_name): Fetches the most recent
            Google Sheet by its name.
        get_gsheets_by_prefix(prefix="prep_results"): Fetches all Google Sheets
            whose names contain the given prefix.

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


    def _list_files(
        self, q: str, drive_params: dict, fields: str, page_size: int = 1000,
    ) -> List[Dict]:
        files, page_token = [], None
        while True:
            resp = (
                self.service.files()
                .list(
                    q=q,
                    fields=f"nextPageToken, files({fields})",
                    pageSize=page_size,
                    pageToken=page_token,
                    includeItemsFromAllDrives=True,
                    supportsAllDrives=True,
                    **drive_params,  # e.g., corpora/driveId when set
                )
                .execute()
            )
            files.extend(resp.get("files", []))
            page_token = resp.get("nextPageToken")
            if not page_token:
                break
        return files

    def _traverse_folder_tree(
        self, root_folder_id: str, drive_params: dict,
    ) -> List[str]:
        """Breadth-first traversal to collect all descendant folder IDs starting at
        root.
        """
        folder_mime = "application/vnd.google-apps.folder"
        to_visit = [root_folder_id]
        all_folders = []

        while to_visit:
            current = to_visit.pop(0)
            all_folders.append(current)
            # list direct child folders of current
            q = f"mimeType='{folder_mime}' and '{current}' in parents and trashed=false"
            subfolders = self._list_files(q, drive_params, fields="id", page_size=1000)
            to_visit.extend([f["id"] for f in subfolders])
        return all_folders

    def get_gsheets_by_prefix(
        self,
        prefix: Optional[str] = None,
        *,
        shared_drive_id: Optional[str] = None,
        folder_id: Optional[str] = None,
        recursive: bool = False,
    ) -> pd.DataFrame:
        """Retrieve Google Sheets whose names match a prefix, optionally restricted to a
        shared drive
        and/or a folder (with optional recursive traversal).
    

        Args:
            prefix: Name prefix to filter (defaults to "prep_results"). shared_drive_id:
            If provided, restrict search to this shared drive. folder_id: If provided,
            restrict to this folder; with `recursive=True`, include all descendants.
            recursive: If True and `folder_id` provided, traverse subfolders.

        Returns:
            DataFrame with columns ['race_id','file_id','modified_at','source_type'].

        """
        self.prefix = "prep_results" if prefix is None else prefix

        # Drive scoping params
        drive_params = {}
        if shared_drive_id:
            drive_params.update({"corpora": "drive", "driveId": shared_drive_id})
        else:
            # Search the user scope + all drives the account can see
            # (corpora='allDrives' searches across My Drive + shared drives)
            drive_params.update({"corpora": "allDrives"})

        # Base filters: spreadsheets only, not trashed, name contains prefix (we'll
        # still post-filter for startswith)
        base_q = (
            "mimeType='application/vnd.google-apps.spreadsheet' "
            f"and name contains '{self.prefix}' and trashed=false"
        )

        # Folder restriction
        folders_to_search = None
        if folder_id:
            if recursive:
                folders_to_search = self._traverse_folder_tree(folder_id, drive_params)
            else:
                folders_to_search = [folder_id]

        files = []
        if folders_to_search:
            # Query each folder separately: "<base> and '<folder>' in parents"
            for fid in folders_to_search:
                q = f"{base_q} and '{fid}' in parents"
                files.extend(
                    self._list_files(
                        q,
                        drive_params,
                        fields="id, name, modifiedTime",
                    ),
                )
        else:
            # No folder scoping; just search the chosen corpora (optionally a single
            # shared drive)
            files = self._list_files(
                base_q,
                drive_params,
                fields="id, name, modifiedTime",
            )

        # Final in-Python filter to enforce true "prefix" behavior
        out = [f for f in files if f.get("name", "").startswith(self.prefix)]

        if not out:
            return pd.DataFrame(
                columns=["race_id", "file_id", "modified_at", "source_type"],
            )

        return (
            pd.DataFrame(out)
            .rename(
                columns={
                    "name": "race_id",
                    "id": "file_id",
                    "modifiedTime": "modified_at",
                },
            )
            .assign(source_type="google_sheets")
            .sort_values(by=["race_id", "modified_at"], ascending=[True, False])
            .reset_index(drop=True)
        )
