import os
import pandas as pd
from datetime import datetime as dt
import numpy as np

class XRFCheck:

    def __init__(self, ledger_file, ledger_file_xrf_loaded, results_folder):
        self.ledger_file = ledger_file
        self.results_folder = results_folder
        self.ledger_file_xrf_loaded = ledger_file_xrf_loaded

    def file_check(self):
        """Checks the results folder for files not in the XRF ledger. Returns a list of files"""

        results_folder_files = os.listdir(self.results_folder)

        ledger_df = pd.read_csv(self.ledger_file, usecols=[self.ledger_file_xrf_loaded])
        xrf_loaded = ledger_df[self.ledger_file_xrf_loaded].to_list()
        xrf_loaded_unique = list(set(xrf_loaded))

        xrf_file_not_loaded = []
        for file in results_folder_files:
            if ".csv" in file and file not in xrf_loaded_unique:
                xrf_file_not_loaded.append(file)

        return xrf_file_not_loaded


class XRFFormat:
    def __init__(self, xrf_raw_file, folder):
        self.xrf_raw_file = xrf_raw_file
        self.folder = folder
        self.xrf_raw_file_path = folder + "\\" + xrf_raw_file
        self.df = pd.DataFrame()

    def clean_xrf_file(self, xrf_col_name, instrument_sn_column, date_column, seq_no):

        df = pd.read_csv(self.xrf_raw_file_path, skip_blank_lines=True, sep=",", header=0)
        if df.columns[0] != "Instrument Serial Num":
            df = pd.read_csv(self.xrf_raw_file_path, skip_blank_lines=True, sep=",", header=1)

        df = df.loc[:, ~df.columns.str.contains('^Unnamed')]

        df.columns = df.columns.str.strip().str.replace(' Concentration', ''). \
            str.replace(' Error1s', '_Error')

        df.drop(columns=df.columns[df.columns.str.contains('Compound')], inplace=True)

        df.rename(columns=xrf_col_name, inplace=True)

        df['Field_Label1'] = 'Operator'
        df['Field_Label2'] = 'Sample Type'
        df['Field_Label3'] = 'Project No'
        df['Field_Label4'] = 'Test Label'
        df['Field_Label5'] = 'Orig_Filename'
        df['Field5'] = self.xrf_raw_file
        df["Field_Label6"] = "SampleID_Error"
        df["Field6"] = np.nan
        df["Field_Label7"] = "SampleID_Orig"
        try:
            df['Field1'] = df.Field1.astype(str)
            df['Field2'] = df.Field2.astype(str)
        except:
            df.to_csv('error.csv')

        for idx, i in df.iterrows():
            df.loc[idx, "Field7"] = str(i["SampleID"])
            if "pulp" in i["Field2"].lower():
                df.loc[idx, "SampleID"] = str(i["SampleID"]) + "_pulp"

        df.sort_values(by=[instrument_sn_column, date_column, seq_no])
        df[instrument_sn_column] = df[instrument_sn_column].astype(str)
        df[instrument_sn_column] = df[instrument_sn_column].str.replace("\.0", "")

        return df

    def xrf_date_check(self, df, date_column, time_column, month, date_format):
        try:
            date_leading_value = int(df[date_column][0].split("-")[1])
        except IndexError:
            date_leading_value = int(df[date_column][0].split("/")[1])

        date_time = df.iloc[0][date_column] + " " + df.iloc[0][time_column]
        if date_leading_value == month:
            try:
                dt_serial_month = dt.strptime(date_time, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                dt_serial_month = dt.strptime(date_time, "%d/%m/%Y %H:%M:%S")
        else:
            try:
                dt_serial_month = dt.strptime(date_time, "%Y-%d-%m %H:%M:%S")
            except ValueError:
                dt_serial_month = dt.strptime(date_time, "%m/%d/%Y %H:%M:%S")

        df[date_column] = dt_serial_month.strftime(date_format)

        return df

    def xrf_export_csv(self, df, instrument_sn_column, time_column, date_column, export_folder):
        instrument_sn = df[instrument_sn_column].unique()

        for serial in instrument_sn:
            df_serial = df[df[instrument_sn_column] == serial]

            unique_dates = df_serial[date_column].unique()

            for date in unique_dates:
                df_serial_date = df_serial[df[date_column] == date]

                df_serial_date['Live_Time1'] = df_serial_date['Live_Time1'].astype(float)
                df_serial_date['Live_Time2'] = df_serial_date['Live_Time2'].astype(float)
                df_serial_date['Live_Time3'] = df_serial_date['Live_Time3'].astype(float)

                date_time = df_serial_date.iloc[0][date_column] + " " + df_serial_date.iloc[0][time_column]

                dt_serial_month = dt.strptime(date_time, "%d/%m/%Y %H:%M:%S")

                batch = dt_serial_month.strftime("%Y-%m-%d-%H-%M") + "-" + serial

                df_serial_date.to_csv(export_folder + "\\" + batch + ".csv", index=False, line_terminator="\n")

    def xrf_ledger(self, df, instrument_sn_column, time_column, date_column):
        instrument_sn = df[instrument_sn_column].unique()
        ledger_list = []

        for serial in instrument_sn:
            df_serial = df[df[instrument_sn_column] == serial]

            unique_dates = df_serial[date_column].unique()

            for date in unique_dates:
                df_serial_date = df_serial[df[date_column] == date]

                date_time = df_serial_date.iloc[0][date_column] + " " + df_serial_date.iloc[0][time_column]

                dt_serial_month = dt.strptime(date_time, "%d/%m/%Y %H:%M:%S")

                batch = dt_serial_month.strftime("%Y-%m-%d-%H-%M") + "-" + serial

                count = len(df_serial_date)
                project = '; '.join(df_serial_date[df_serial_date['Note'].notnull()].Note.astype(str).str.strip().str.upper().unique())
                person = '; '.join(df_serial_date.Field1.str.strip().str.upper().unique())
                sample_type = '; '.join(df_serial_date.Field2.str.strip().str.upper().unique())
                first_sample = df_serial_date.iloc[0]['Field7']
                last_sample = df_serial_date.iloc[-1]['Field7']

                ledger_dict = {
                    'Batch_No': [batch],
                    'Instrument_SN': [serial],
                    'Analyzed_Date': [dt_serial_month.strftime('%d-%b-%Y')],
                    'Analysis_Count': [count],
                    'DataSet': [project],
                    'Sample_Type': [sample_type],
                    'Analyzed_By': [person],
                    'First_SampleID': [first_sample],
                    'Last_SampleID': [last_sample],
                    'Orig_Filename': [''],
                               }
                ledger_list.append(ledger_dict)

        return ledger_dict











