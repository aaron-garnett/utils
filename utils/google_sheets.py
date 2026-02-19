import pygsheets
import pandas as pd
import logging

logger = logging.getLogger(__name__)

def list_all_google_sheets(connection):
    """List all Google Sheets spreadsheets accessible with the given connection."""
    logger.debug('Listing all Google Sheets spreadsheets.')
    sheet_ids = connection.spreadsheet_ids()
    sheet_count = len(sheet_ids)
    logger.info(f'{sheet_count} Google Sheet spreadsheets found.')
    for sheet in sheet_ids:
        sh = connection.open_by_key(sheet)
        logger.info(f'Title: {sh.title}, ID: {sh.id}, Updated: {sh.updated}')
    return sheet_ids


def purge_all_google_sheets(connection, sheet_ids_to_keep):
    """Delete all Google Sheets spreadsheets except those specified in sheet_ids_to_keep."""
    logger.debug(f'Purging Google Sheets spreadsheets except {sheet_ids_to_keep}.')
    sheet_ids = list_all_google_sheets(connection)
    sheet_ids_to_keep = [sheet_ids_to_keep] if isinstance(sheet_ids_to_keep, str) else sheet_ids_to_keep
    logger.info(f'{len(sheet_ids_to_keep)} spreadsheets identified to keep.')
    sheet_ids_to_keep = [i for i in sheet_ids_to_keep if i in sheet_ids]
    sheet_ids_to_delete = [i for i in sheet_ids if i not in sheet_ids_to_keep]
    logger.info(f'{len(sheet_ids_to_keep)} spreadsheets will be kept, {len(sheet_ids_to_delete)} will be deleted.')
    for sheet in sheet_ids_to_keep:
        sh = connection.open_by_key(sheet)
        logger.info(f'Deleting spreadsheet Title: {sh.title}, ID: {sh.id}, Updated: {sh.updated}')
        sh.delete()


def write_df_to_google_sheet(google_service_acct_file: str, sheet_id: str, worksheet_name: str, dataframe: pd.DataFrame, clear_existing: bool=True, resize_existing=True, field_leading_character: str = '\'') -> None:
    """Write a pandas DataFrame to a Google Sheet worksheet."""
    logger.debug(f'Writing DataFrame to Google Sheet ID: {sheet_id}, Worksheet: {worksheet_name}.')
    # Connect to Google service account
    gc = pygsheets.authorize(service_account_file=google_service_acct_file)
    sh = gc.open_by_key(sheet_id)
    wks = sh.worksheet_by_title(worksheet_name)

    nrows, ncols = dataframe.shape
    if (nrows + 1) * ncols > 10 * 10**6:
        raise ValueError("Dataframe too large to fit in Google Sheet (max 10MM cells).")

    if field_leading_character:
        # Prepend leading character to all dataframe values to prevent Google Sheets from interpreting them as formulas or dates
        dataframe = dataframe.astype(str).map(lambda x: field_leading_character + x)

    if clear_existing:
        # Clear existing worksheet data
        wks.clear()

    if resize_existing:
        # Resize worksheet to fit dataframe
        wks.resize(rows=nrows + 1, cols=ncols)

    # Write dataframe to worksheet
    wks.set_dataframe(dataframe, (1, 1), copy_index=False)
