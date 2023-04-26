import pandas as pd
import os
import shutil
from datetime import datetime as dt
from settings import LEDGER, LEDGER_XRF_LOADED_COLUMN, MONTHLY_FOLDER, XRF_PROCESSED, XRF_RAW_COL_RENAME, MONTH,\
    XRF_RAW_FOLDER
from xrf_template import XRFCheck, XRFFormat
import time

RESULT_TEXT_FILENAME = "XRF_TEMPLATE_STATUS"

date_today = dt.today()
date_today_template = date_today.strftime("%d-%b-%Y")

# Creates XRFCheck instance
xrf_check = XRFCheck(ledger_file=LEDGER, ledger_file_xrf_loaded=LEDGER_XRF_LOADED_COLUMN,
                     results_folder=MONTHLY_FOLDER)

# List of files not in XRF ledger
files = xrf_check.file_check()

ledger_df_all = pd.DataFrame()

with open(f"{RESULT_TEXT_FILENAME}.txt", "w") as xrf_status:
    pass

# Loop through list of filenames to process
if len(files) > 0:
    for f in files:
        shutil.copyfile(MONTHLY_FOLDER+"//"+f, XRF_RAW_FOLDER+"//"+f)

    time.sleep(5)

    for f in files:
        print(f)
        xrf_format = XRFFormat(xrf_raw_file=f, folder=XRF_RAW_FOLDER)
        df_formatted = xrf_format.clean_xrf_file(XRF_RAW_COL_RENAME, instrument_sn_column="Instrument_SN",
                                                 date_column="ActionDate", seq_no="Reading")
        df_formatted_date_corrected = xrf_format.xrf_date_check(df=df_formatted, date_column="ActionDate",
                                                                time_column="ActionTime", month=MONTH,
                                                                date_format="%d/%m/%Y")

        xrf_format.xrf_export_csv(df=df_formatted_date_corrected, date_column="ActionDate", time_column="ActionTime",
                                  instrument_sn_column="Instrument_SN", export_folder=XRF_PROCESSED)

        ledger = xrf_format.xrf_ledger(df=df_formatted_date_corrected, date_column="ActionDate",
                                       time_column="ActionTime", instrument_sn_column="Instrument_SN")

        ledger["Orig_Filename"] = f

        ledger_df = pd.DataFrame.from_dict(ledger)
        ledger_df_all = ledger_df_all.append(ledger_df, ignore_index=True)

        with open(f"{RESULT_TEXT_FILENAME}.txt", "a") as xrf_status:
            xrf_status.write(f"{date_today} -- processed {f}\n")

    ledger_df_all.to_csv(XRF_PROCESSED + "\\" + "pXRF_Ledger_" + date_today.strftime("%Y%m%d") + ".csv", index=False,
                         line_terminator="\n")


else:
    with open(f"{RESULT_TEXT_FILENAME}.txt", "w") as xrf_status:
        xrf_status.write(f"{date_today} -- NO NEW XRF RESULTS\n")
