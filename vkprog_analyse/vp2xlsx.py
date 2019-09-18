"""
# Verkaufsprognose to Excels (vp2xlsx)

Für jeden aktiven Verkaufsberater mit mindestens 1 lead, wird ein Excel
erstellt.
"""
# %% Reset
# %reset -f

# make imports from pa_lib possible (parent directory of file's directory)
import sys
from pathlib import Path

file_dir = Path.cwd()
parent_dir = file_dir.parent
sys.path.append(str(parent_dir))

# %% Load modules
import pandas as pd
import numpy as np
from pathlib import Path
from os import mkdir

from pa_lib.log import info
from pa_lib.util import excel_col

#%% Define data location and deployment folder
gv_DIR_DATA       = Path.home() / 'data/2019-09-09_4J_2W_KW37_Buchung/' # please adjust accordingly
deployment_folder = Path('/mnt/predictiveanalytics/') / '2019_09_09_donttouch'       # please adjust accordingly

#%% Create deployment folder
os.mkdir(deployment_folder)

# %% Load Data: Active VBs, Complete scoring list

def load_csv(file_name, **params):
    file_path = gv_DIR_DATA / file_name
    df = pd.read_csv(file_path, low_memory=False, **params)
    return df


info('Load files')
ek_list = load_csv('EK_LIST_2W_KOMPLETT.csv',
                   sep=';',
                   encoding='ISO-8859-1')
vb_list = load_csv('vkber_data.csv',
                   sep=',',
                   encoding='UTF-8')


# %% Data Preparation:Complete Scoring Table
# %% Column & Row Selection:
def select_columns(df, pattern):
    return (df.columns
            .to_series()
            .loc[df.columns.str.match(pattern)]
            .to_list()
            )


_row_selection = (pd.isna(ek_list.Kleinkunde) &
                  pd.isna(ek_list.Neukunde) &
                  pd.isna(ek_list.Insolvenz) &
                  pd.isna(ek_list.Umsatz_erreicht) &
                  pd.isna(ek_list.kuerzlich_gebucht) &
                  pd.isna(ek_list.kuerzlich_im_aushang) &
                  pd.isna(ek_list.kuerzlich_im_kontakt) &
                  pd.isna(ek_list.VB_FILTER_AKTIV))

_col_selection = ("""ENDKUNDE_NR 
                    Endkunde 
                    HB_APG 
                    Agentur 
                    HB_Agentur 
                    PLZ 
                    Ort""".split() +
                  select_columns(ek_list, pattern='^Net_2') +
                  """letzte_VBs
                     letzter_Kontakt 
                     KZ_letzter_Ktkt 
                     Kanal 
                     Betreff 
                     letzte_Kamp_erfasst 
                     letzte_Kamp_Beginn 
                     Verkaufsgebiet 
                     VB_VK_Geb""".split() +
                  select_columns(ek_list, pattern='^prob_KW'))

ek_list = ek_list.loc[_row_selection, _col_selection]


# %% Data-Type Clean Up:

def parse_dates(df, columns, format):
    result = df.copy()
    for col in columns:
        result.loc[:, col] = pd.to_datetime(result.loc[:, col], format=format)
    return result


def parse_ints(df, columns):
    result = df.copy()
    result.loc[:, columns] = result.loc[:, columns].fillna(0).astype('int64')
    return result


date_columns = ['letzter_Kontakt', 'letzte_Kamp_erfasst', 'letzte_Kamp_Beginn']
int_columns = ["PLZ", "ENDKUNDE_NR"] + select_columns(ek_list, pattern='^Net_')

ek_list = (ek_list
           .pipe(parse_dates, date_columns, format='%Y-%m-%d')
           .pipe(parse_ints, int_columns)
           )

# %% Data Preparation: Active VB-list
# %% Zuteilung und die einzelnen VBs
vb_list = (
    vb_list.assign(
        Vorname=vb_list['KOMBI_NAME'].apply(lambda x: x.rpartition(' ')[2]),
        Nachname=vb_list['KOMBI_NAME'].apply(lambda x: x.rpartition(' ')[0])
    )
        .loc[:, ["Vorname", "Nachname", "E_MAIL", "FUNKTION", "KURZZEICHEN"]]
        .set_index("KURZZEICHEN")
)

# %% Data Preparation:
vb_ek_map = {}

for vb_kuerz in vb_list.index:
    vb_ek_map[vb_kuerz] = (
        ek_list.loc[
            (ek_list.HB_APG == vb_kuerz) |  # VB Endkunde
            (ek_list.HB_Agentur == vb_kuerz)  # VB Agentur
            ].sort_values(select_columns(ek_list, pattern='^prob_KW'), ascending=False)  # highest probability first.
    )

# %% Create VB overview with number of potential leads
vb_nleads = pd.DataFrame.from_records(columns=['total_leads'],
                                      data=[(vb_ek_map[kuerz].shape[0],)
                                            for kuerz in vb_ek_map.keys()],
                                      index=vb_ek_map.keys())

vb_list = vb_list.merge(vb_nleads,
                        left_index=True,
                        right_index=True)


# %% Define overview-excel creator
def overview_xlsx(df, file_name, sheet_name='df'):
    """
    Write df into a XLSX with fixed title row, enable auto filters
    """

    # column widths as max strlength of column's contents
    col_width = df.astype('str').apply(lambda col: max(col.str.len())).to_list()
    title_width = list(map(len, df.columns))

    # open file, create workbook & sheet
    file_path = deployment_folder / file_name
    info(f'Write file {file_path}')
    writer = pd.ExcelWriter(file_path, engine='xlsxwriter')
    df.to_excel(writer, index=False, sheet_name=sheet_name)
    workbook = writer.book
    worksheet = writer.sheets[sheet_name]

    # add formatting
    ncols = df.shape[-1]
    title_cells = (0, 0, 0, ncols - 1)
    bold = workbook.add_format({'bold': True, 'align': 'left'})
    worksheet.set_row(0, cell_format=bold)
    worksheet.autofilter(*title_cells)
    worksheet.freeze_panes(1, 0)

    # Column autowidth: set each to max(col_width, title_width) + 1
    for col in range(ncols):
        worksheet.set_column(col, col, max(col_width[col], title_width[col]) + 1)
    writer.save()


# %% Excel column names
# map all names on itself, then adjust those which need to be adjusted:

xlsx_col_names = dict(zip(ek_list.keys(),
                          ek_list.keys()))
xlsx_col_names.update(
    {'ENDKUNDE_NR': 'Gepard-Nr. Endkunde',
     'HB_APG': 'VB Endkunde',
     'HB_Agentur': 'VB Agentur',
     'letzte_VBs': 'VBs letzte Kampagnen',
     'letzter_Kontakt': 'Letzter CRM-Kontakt',
     'KZ_letzter_Ktkt': 'Kz letzter Kontakt',
     'letzte_Kamp_erfasst': 'Letzte Kampagne erfasst am',
     'letzte_Kamp_Beginn': 'Beginn letzte Kampagne',
     'VB_VK_Geb': 'Gebiets-VB'}
)

for x in xlsx_col_names.keys():
    if 'prob_' in x:
        xlsx_col_names[x] = 'Chance'
        break

# %% Excel Info text: This will be displayed in every Excel sheet.
info_text = """Liste von potenziell interessanten Kundenkontakten.
Die Liste wird alle 2 Wochen bereitgestellt.\n
Bitte in den letzten 2 Spalten Feedback eintragen, auch Vorschläge für die Verbesserung der Liste sind willkommen. 
Vielen Dank."""


# %% Define Excel Function
def vb_sales_xlsx(vb_lists, gv_VB_TOP_N=20):
    """
    Input: Dictionary mit VBs als Keys und Dataframes (Top20 pro VB) als value.
    Output: Pro VB wird ein formatiertes Excel generiert.
    """
    file_name_templ = 'EK_LIST_2W_KOMPAKT_{0}.xlsx'
    sheet_name_templ = 'EK_LIST_2W_KOMPAKT_{0}'

    for vb in vb_lists.keys():
        if len(vb_lists[vb]) == 0:
            info('Verkaufsberater ' + vb + ' hat keine Leads.')
            continue

        ## Technical Definitions:
        df_vb = vb_lists[vb].head(gv_VB_TOP_N)
        column_names = df_vb.keys()  # Column names, titles
        feedback_col = excel_col(len(column_names) + 1)  # Feedback-Spalte
        comment_col = excel_col(len(column_names) + 2)  # Kommentar-Spalte
        lengths = (list(np.vectorize(len)(df_vb.values.astype(str))
                        .max(axis=0)))
        # => Maximal character length for each column
        types = dict(df_vb.dtypes.astype(str))  # Types for each column
        excel_columns = {excel_col(i + 1): column_names[i]
                         for i in range(0, len(column_names))}
        # => Dictionary: Column to Name
        excel_width = {excel_col(i + 1): lengths[i] + 1
                       for i in range(0, len(column_names))}
        # => Dictionary: Column to Widths

        ## Define feedback list for Drop-Down:
        feedback = {'validate': 'list',
                    'source': ['hilfreich', 'nicht hilfreich',
                               'nicht bearbeitet'],
                    'input_title': 'Bitte beurteilen:',
                    'input_message': '- hilfreich\n- nicht hilfreich\n- nicht bearbeitet',
                    'error_title': 'Eingabe ungültig',
                    'error_message': 'Bitte auswählen:\n  - hilfreich\n  - nicht hilfreich\n  - nicht bearbeitet'}

        info(f"Write file {deployment_folder / file_name_templ.format(vb)}")

        ## Create a Pandas Excel writer using XlsxWriter as the engine:
        writer = pd.ExcelWriter(str(deployment_folder / file_name_templ.format(vb)),
                                # os.getcwd() + '\\output\\' + file_name_templ.format(vb)+'.xlsx',
                                engine='xlsxwriter',
                                datetime_format="dd.mm.yyyy")

        ## Convert the dataframe to an XlsxWriter Excel object:
        df_vb.to_excel(writer,
                       sheet_name=sheet_name_templ.format(vb),
                       index=False,
                       freeze_panes=(1, 5))

        ## Create xlsxwriter workbook object:
        workbook = writer.book
        worksheet = writer.sheets[sheet_name_templ.format(vb)]

        ## Define column formats:
        column_format_text = workbook.add_format(
            {'align': 'left', 'valign': 'bottom'})
        column_format_txt_wrap = workbook.add_format(
            {'align': 'left', 'valign': 'bottom', 'text_wrap': True})
        # => Commentary fields need
        column_format_umsatz = workbook.add_format(
            {'align': 'right', 'valign': 'bottom', 'num_format': "#,###"})
        # => Netto-Umsatz!
        column_format_prob = workbook.add_format(
            {'align': 'right', 'valign': 'bottom', 'num_format': "#.0"})
        # => Probability
        column_format_integer = workbook.add_format(
            {'align': 'right', 'valign': 'bottom', 'num_format': "#"})
        # => Integers for PLZ and Gepard-Nr
        column_format_left = workbook.add_format(
            {'align': 'left', 'valign': 'bottom'})  #
        column_format_dropdown = workbook.add_format(
            {'align': 'left', 'valign': 'bottom', 'bg_color': '#EAEAEA'})
        column_format_error = workbook.add_format({'bg_color': 'black'})

        ## Define templates for special cell formats:
        cell_color_yellow = workbook.add_format(
            {'align': 'center', 'valign': 'bottom', 'bg_color': '#ffff00',
             'bold': True, 'text_wrap': True, 'rotation': 90})
        cell_color_blue = workbook.add_format(
            {'align': 'right', 'valign': 'bottom', 'bg_color': '#bdd7ee',
             'bold': False, 'text_wrap': True})
        cell_color_rotate = workbook.add_format(
            {'align': 'center', 'valign': 'bottom', 'bg_color': '#EAEAEA',
             'bold': True, 'text_wrap': True, 'rotation': 90})
        cell_color_norot = workbook.add_format(
            {'align': 'left', 'valign': 'bottom', 'bg_color': '#EAEAEA',
             'bold': True, 'text_wrap': True})

        ## Automatized Setting/formating columns and top-row cells:
        for i in excel_columns.keys():
            # Write Columns:
            if types[excel_columns[i]] == 'float64':
                worksheet.set_column(i + ':' + i,
                                     5,  # fixed length
                                     column_format_prob)
                # => Net values.
            elif types[excel_columns[i]] == 'int64':
                if "Net_" in excel_columns[i]:
                    worksheet.set_column(i + ':' + i,
                                         excel_width[i] + 1,
                                         column_format_umsatz)
                else:
                    worksheet.set_column(i + ':' + i,
                                         excel_width[i],
                                         column_format_integer)
                    # => Should only effect PLZ and Gepard-Nr.
            elif types[excel_columns[i]] == 'datetime64[ns]':
                worksheet.set_column(i + ':' + i,
                                     10,  # Fixed length!
                                     column_format_text)
            elif types[excel_columns[i]] == 'object':
                if "Betreff" in excel_columns[i]:
                    worksheet.set_column(i + ':' + i,
                                         40,  # fixed length
                                         column_format_text)
                    # => Betreff: comments on last contact with customer
                else:
                    worksheet.set_column(i + ':' + i,
                                         excel_width[i],
                                         column_format_text)
            else:
                worksheet.set_column(i + ':' + i,
                                     excel_width[i],
                                     column_format_error)
                # => If this gets triggered, these cells are turned black

            # Write Top-row cells:
            if excel_width[i] - 10 < len(xlsx_col_names[excel_columns[i]]):
                worksheet.write(i + '1',
                                xlsx_col_names[excel_columns[i]],
                                cell_color_rotate)
            else:
                worksheet.write(i + '1',
                                xlsx_col_names[excel_columns[i]],
                                cell_color_norot)

        ## Manual Setting/formating columns and top-row cells:

        # Info-text box, should be vsible under the list on the left side:
        worksheet.insert_textbox('B' + str(gv_VB_TOP_N + 2 + 1),
                                 info_text,
                                 {'width': 480,
                                  'height': 100,
                                  'fill': {'color': '#ddd9c3'},
                                  'line': {'width': 3.25}})

        # Feedback-column:
        worksheet.set_column(feedback_col + ':' + feedback_col,
                             15,
                             column_format_left)
        worksheet.write(feedback_col + '1',
                        'Feedback - bitte auswählen',
                        cell_color_yellow)
        for i in range(2, gv_VB_TOP_N + 2):
            worksheet.write(feedback_col + str(i),
                            '',
                            column_format_dropdown)
        worksheet.data_validation(feedback_col + '2:' + feedback_col + str(gv_VB_TOP_N + 2), feedback)

        # General feedback, below list, aligned with Feedback-column W:
        begin_merge_cell = excel_col(len(column_names) - 4) + str(gv_VB_TOP_N + 3)
        end_merge_cell = excel_col(len(column_names)) + str(gv_VB_TOP_N + 3)
        worksheet.merge_range(begin_merge_cell + ':' + end_merge_cell,
                              'hier ein generelles Feedback wählen:',
                              cell_color_blue)
        worksheet.data_validation(feedback_col + str(gv_VB_TOP_N + 3), feedback)
        worksheet.write(feedback_col + str(gv_VB_TOP_N + 3), '', column_format_dropdown)
        # => Leave empty cell, so VBs have to fill out.

        # Comment column:
        worksheet.write(comment_col + '1',
                        'falls nicht hilfreich, bitte hier einen kurzen Kommentar angeben - entweder pro Zeile oder für die Gesamt-Liste',
                        cell_color_norot)
        worksheet.set_column(comment_col + ':' + comment_col, 44, column_format_txt_wrap)

        # Write file into working folder
        writer.save()


# %% Create Excels
vb_sales_xlsx(vb_ek_map, 20)

overview_xlsx(vb_list, "vkber_potential.xlsx", sheet_name='VK')
# %% End of file.

#######################
## Deployment Email! ##
#######################

##############
# Email list #
##############
notify_emails = (vb_list.query('total_leads > 0')
                        .loc[:, "E_MAIL"])
notify_emails.at['STC'] = 'sam.truong@apgsga.ch'
notify_emails.at['KPF'] = 'samcuong@gmx.ch'

#notify_emails = notify_emails.iloc[-2:2] # for testing purposes!
notify_emails = notify_emails.reset_index()

notify_emails=notify_emails.iloc[36:,:]

notify_emails.loc[:,"E_MAIL"]

#########
# libs ##
#########

from smtplib import SMTP

from email.message import EmailMessage
from email.headerregistry import Address

######################
# Define letter: msg #
######################

msg = EmailMessage()
msg['Subject'] = "Verkaufsprognose: Excel-Listen"
msg['From'] = Address("Predictive Analytics", "predictive_analytics", "apgsga.ch")
msg['To'] = ', '.join(map(str,list(notify_emails.loc[:,"E_MAIL"])))

msg.set_content("""\
Hallo zusammen,

Im folgenden Verzeichnis findet ihr unter eurem Kürzel die aktuellen Excel-Listen:
P:\Service\Kennzahlen\Verkauf\PredictiveAnalytics

Zusätzliche Infos findet ihr auf Wiki:
https://wiki.apgsga.ch/display/ohit21/Predictive+Analytics

Lieben Gruss,
Euer Data Analytics Team
""")

# Add the html version.  This converts the message into a multipart/alternative
# container, with the original text message as the first part and the new html
# message as the second part.
msg.add_alternative("""\
<html>
  <head></head>
  <body>
    <p>Hallo zusammen,</p>
    <p>Im folgenden Verzeichnis findet ihr unter eurem K&uuml;rzel die aktuellen Excel-Listen:</p>
    <p style="padding-left: 30px;"><a href="P:\Service\Kennzahlen\Verkauf\PredictiveAnalytics">Verkaufsprognose: Excels</a></p>
    <p>&nbsp;</p>
    <p>Zus&auml;tzliche Infos findet ihr auf Wiki:</p>
    <p style="padding-left: 30px;"><a href="https://wiki.apgsga.ch/display/ohit21/Predictive+Analytics">Wiki-Page</a></p>
    <p>&nbsp;</p>
    <p>Lieben Gruss,</p>
    <p>Euer Data Analytics Team</p>
  </body>
</html>
""", subtype='html')

print(msg.as_string())

###############
# Send emails #
###############

with SMTP(host='mailint.apgsga.ch') as mail_gateway:
    mail_gateway.set_debuglevel(True)
    mail_gateway.send_message(msg)
    
# End of file.