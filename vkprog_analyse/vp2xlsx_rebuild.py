"""
# Verkaufsprognose to Excels (vp2xlsx)

Für jeden aktiven Verkaufsberater mit mindestens 1 lead, wird ein Excel
erstellt.
"""
################################################################################
# make imports from pa_lib possible (parent directory of file's directory)
import sys
from pathlib import Path

file_dir = Path.cwd()
parent_dir = file_dir.parent
sys.path.append(str(parent_dir))

################################################################################
# %% Load modules
import os
import pandas    as pd
import numpy     as np
from pathlib     import Path
from os          import mkdir
from pa_lib.log  import info
from pa_lib.util import excel_col
from pa_lib.job  import request_job # lazy recursive job dependency request
from pa_lib.file import (
    project_dir,
    load_bin,
    load_csv,
    )

################################################################################
#%% Define data location and deployment folder

# Entscheidungsschwelle für Vorhersage:
gv_MIN_PROB = 0.01 # I don't know. Has been defined back in the days.

# please adjust accordingly:
name_depl_folder  = '2019_11_18_test_01' # Example: '2019_10_21'

#%% Create deployment folder (where all the xlsx-files go!)
deployment_folder = (
    ## If you're working on the server:
    #Path('/mnt/predictiveanalytics/') / name_depl_folder
    
    ## If you're working on your machine:
    Path('P:\Service\Kennzahlen\Verkauf\PredictiveAnalytics') / name_depl_folder
    )


os.mkdir(deployment_folder)

################################################################################
## Recursive Dependency Check:
request_job(job_name="vkber_prepare.py", current= "This Week") 
# output: vkber_data.csv

################################################################################
# %% Load Data: Active VBs, Complete scoring list

with project_dir("vkprog"):
    vb_list = load_csv(
        "vkber_data.csv",
        sep=',',
        encoding='UTF-8',
        )

with project_dir("vkprog/predictions"):
    ek_list = load_bin("20191118_ek_list.feather")

################################################################################
# %% Data Preparation:Complete Scoring Table

def select_columns(df, pattern):
    return (df.columns
            .to_series()
            .loc[df.columns.str.match(pattern)]
            .to_list()
            )

# Column selection:

_col_selection = ("""Endkunde_NR 
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


# Row selection/filter:
prob_KW     = [col for col in ek_list.columns if col.startswith("prob_KW")][0]
net_columns = [col for col in ek_list.columns if col.startswith("Net_")]

pauschale_filter = (
    # Minimum Probability:
    (ek_list.loc[:,prob_KW] > gv_MIN_PROB
    ) &
    
    # Insolvenz:
    (ek_list.loc[:,"Insolvenz"] 
         != True
    ) &
    
    # kuerzlich_gebucht (in den letzten 2 Monaten erfasste Kampagnen):
    (ek_list.loc[:,"letzte_Kamp_erfasst"] 
        < pd.Timestamp.now() - pd.DateOffset(months=2)
    ) &
    
    # kuerzlich_im_aushang (Aushangbeginn vor 1 Monat oder später):
    (ek_list.loc[:,"letzte_Kamp_Beginn"] 
        < pd.Timestamp.now() - pd.DateOffset(months=1)
    ) &
    
    # keine Kleinkunden (Ueber die letzten 4 Jahre nie mehr als 3'000 pro Jahr):
    (ek_list.loc[:,net_columns].max(axis=1).fillna(0) 
        > 3000
    ) &
    
    # keine Neukunden (Alle, die erst im aktuellen Jahr Umsatz hatten):
    ((ek_list.loc[:,sorted(net_columns, reverse=True)[1:]]
             .max(axis=1)
             .fillna(0))
        > 0
    ) &
    
    # Umsatz_erreicht (80% Netto-Umsatz gem. Vorjahr erreicht) 
    ((ek_list.loc[:,sorted(net_columns, reverse=True)[0]]
             .fillna(0))
         <= 0.8*(ek_list.loc[:,sorted(net_columns, reverse=True)[1]]
                        .fillna(0))
    ) &
    
    # kuerzlich_im_kontakt (keine Kunden, mit CRM-Kontakt in den letzten 4 Wochen)
    (ek_list.loc[:,"last_CRM_Ktkt_date"].fillna(pd.Timestamp.now() - pd.DateOffset(years=100)) 
        < pd.Timestamp.now() - pd.DateOffset(months=1)
    ) &
    
    # VB_FILTER_AKTIV (in CRM ist eine gültige Sperre für Kunden erfasst)
     ~(# We define the evil ones, and take the boolean opposite:
    
         # Both entries exist: Customer is right now within "Sperre"
        ((ek_list.loc[:,"VB_FILTER_VON"] < pd.Timestamp.now()) &
         (pd.Timestamp.now() <= ek_list.loc[:,"VB_FILTER_BIS"]))

        |# No end date, but begin date exists: 
        ((ek_list.loc[:,"VB_FILTER_VON"] < pd.Timestamp.now()) &
         (ek_list.loc[:,"VB_FILTER_BIS"].isna() ))

        |# No begin date, but end date exists:
        (ek_list.loc[:,"VB_FILTER_VON"].isna() & 
         (ek_list.loc[:,"VB_FILTER_BIS"] <= pd.Timestamp.now() )) 
     )
    
    )

# Apply: Row-Selection & Column Selection
ek_list = (ek_list.loc[pauschale_filter , _col_selection])

################################################################################
# %% Data-Type Clean Up:

ek_list.loc[:,prob_KW] = 100 * ek_list.loc[:,prob_KW] # shows percentage

def parse_ints(df, columns):
    result = df.copy()
    result.loc[:, columns] = result.loc[:, columns].fillna(0).astype('int64')
    return result

int_columns  = ["PLZ", "Endkunde_NR"] + select_columns(ek_list, pattern='^Net_')

ek_list = (ek_list
           .pipe(parse_ints, int_columns)
           )

################################################################################
# %% Data Preparation: Active VB-list
# %% Zuteilung und die einzelnen VBs

vb_list = (
    vb_list.assign(
        Vorname=vb_list['KOMBI_NAME'].apply(lambda x: x.rpartition(' ')[2]),
        Nachname=vb_list['KOMBI_NAME'].apply(lambda x: x.rpartition(' ')[0])
        )
        .loc[vb_list['KAM'] == False,
             ["Vorname", "Nachname", "E_MAIL", "FUNKTION", "KURZZEICHEN"]
            ]
        .set_index("KURZZEICHEN")
    )

################################################################################
# %% Data Preparation:

vb_ek_map = {}

for vb_kuerz in vb_list.index:
    vb_ek_map[vb_kuerz] = (
        ek_list.loc[
            (ek_list.HB_APG == vb_kuerz) |  # VB Endkunde
            (ek_list.HB_Agentur == vb_kuerz)  # VB Agentur
            ].sort_values(
                select_columns(ek_list,pattern='^prob_KW'),
                ascending=False
                )  # highest probability first.
    )

################################################################################
# %% Create VB overview with number of potential leads
vb_nleads = pd.DataFrame.from_records(
    columns = ['total_leads'],
    data    = [(vb_ek_map[kuerz].shape[0],) for kuerz in vb_ek_map.keys()],
    index   = vb_ek_map.keys()
    )

vb_list = vb_list.merge(vb_nleads,
                        left_index  = True,
                        right_index = True)

################################################################################
# %% Define overview-excel creator
def overview_xlsx(df, file_name, sheet_name='df'):
    """
    Write df into a XLSX with fixed title row, enable auto filters
    """

    # column widths as max strlength of column's contents
    col_width = (
        df.astype('str')
          .apply(lambda col: max(col.str.len()))
          .to_list()
        )
    
    title_width = list(map(len, df.columns))

    # open file, create workbook & sheet
    file_path = deployment_folder / file_name
    info(f'Write file {file_path}')
    writer = pd.ExcelWriter(file_path, engine='xlsxwriter')
    df.to_excel(writer, index=False, sheet_name=sheet_name)
    workbook  = writer.book
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
        worksheet.set_column(col, 
                             col,
                             max(col_width[col], title_width[col]) + 1
                            )
    writer.save()

################################################################################
# %% Excel column names
# map all names on itself, then adjust those which need to be adjusted:

xlsx_col_names = dict(zip(ek_list.keys(),
                          ek_list.keys()))
xlsx_col_names.update(
    {'Endkunde_NR': 'Gepard-Nr. Endkunde',
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

        
################################################################################
# %% Excel Info text: This will be displayed in every Excel sheet.
info_text = """Liste von potenziell interessanten Kundenkontakten.
Die Liste wird alle 2 Wochen bereitgestellt.\n
Bitte in den letzten 2 Spalten Feedback eintragen, auch Vorschläge für die Verbesserung der Liste sind willkommen. 
Vielen Dank."""


################################################################################
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
        writer = pd.ExcelWriter(
            str(deployment_folder / file_name_templ.format(vb)),
            # os.getcwd() + '\\output\\' + file_name_templ.format(vb)+'.xlsx',
            engine='xlsxwriter',
            datetime_format="dd.mm.yyyy"
            )

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
                                 {'width':  480,
                                  'height': 120,
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
            
        worksheet.data_validation(
            feedback_col + '2:' + feedback_col + str(gv_VB_TOP_N + 2),
            feedback
            )

        # General feedback, below list, aligned with Feedback-column W:
        begin_merge_cell = excel_col(len(column_names) - 4) + str(gv_VB_TOP_N + 3)
        end_merge_cell = excel_col(len(column_names)) + str(gv_VB_TOP_N + 3)
        worksheet.merge_range(begin_merge_cell + ':' + end_merge_cell,
                              'hier ein generelles Feedback wählen:',
                              cell_color_blue)
        worksheet.data_validation(feedback_col + str(gv_VB_TOP_N + 3), feedback)
        
        worksheet.write(feedback_col + str(gv_VB_TOP_N + 3),
                        '',
                        column_format_dropdown
                       )
        # => Leave empty cell, so VBs have to fill out.

        # Comment column:
        worksheet.write(comment_col + '1',
                        'falls nicht hilfreich, bitte hier einen kurzen Kommentar angeben - entweder pro Zeile oder für die Gesamt-Liste',
                        cell_color_norot)
        
        worksheet.set_column(comment_col + ':' + comment_col,
                             44,
                             column_format_txt_wrap)

        # Write file into working folder
        writer.save()

################################################################################
# %% Create Excels

vb_sales_xlsx(
    vb_lists=    vb_ek_map,
    gv_VB_TOP_N= 20
    )

overview_xlsx(
    df=         vb_list,
    file_name=  "vkber_potential.xlsx",
    sheet_name= 'VK'
    )

# %% End of file.
################################################################################

#######################
## Deployment Email! ##
#######################


##############
# Email list #
##############
notify_emails = (
    vb_list
    .query('total_leads > 0')
    .loc[:, "E_MAIL"]
    )
notify_emails.at['RPE'] = 'reto.pensa@apgsga.ch'
notify_emails.at['KPF'] = 'kaspar.pflugshaupt@apgsga.ch'
notify_emails.at['STC'] = 'sam.truong@apgsga.ch'
notify_emails = notify_emails.reset_index()
print(notify_emails.loc[:,"E_MAIL"])

# For maintenance purposes:
#notify_emails = notify_emails.iloc[-1:,:]

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

msg.set_content(fr"""
Guten Tag miteinander,

Im folgenden Verzeichnis findet ihr unter eurem Kürzel die aktuellen Verkaufsprognose-Listen
\\fppwi01\daten$\Service\Kennzahlen\Verkauf\PredictiveAnalytics\{name_depl_folder}

Zusätzliche Infos findet ihr auf https://wiki.apgsga.ch/display/ohit21/Predictive+Analytics

Bitte denkt daran, eure Feedbacks zu den Inhalten direkt in die Excel Liste zu schreiben.

Beste Grüsse
Euer Data Analytics Team

-----

Bonjour Toutes et Tous,
Dans le classeur suivant vous trouverez, sous vos initiales,  Les listes actualisées concernant les prévisions de vente.
\\fppwi01\daten$\Service\Kennzahlen\Verkauf\PredictiveAnalytics\{name_depl_folder}

Des informations complémentaires à ce sujet sont disponibles dans Wiki 
https://wiki.apgsga.ch/display/ohit21/Predictive+Analytics
Pensez à ajouter votre feedback directement dans la liste Excel (colonne jaune + X).


Avec nos remerciements anticipés et meilleures salutations.
Votre Data Analytics Team
""")

# Add the html version.  This converts the message into a multipart/alternative
# container, with the original text message as the first part and the new html
# message as the second part.
msg.add_alternative(fr"""
<html>
  <head></head>
  <body>
    <p>Guten Tag miteinander,</p>
    <p>Im folgenden Verzeichnis findet ihr unter eurem K&uuml;rzel <a href="\\fppwi01\daten$\Service\Kennzahlen\Verkauf\PredictiveAnalytics\{name_depl_folder}">die aktuellen Verkaufsprognose-Listen.</a></p>
    <p>Zus&auml;tzliche Infos findet ihr <a href="https://wiki.apgsga.ch/display/ohit21/Predictive+Analytics">hier auf Wiki</a></p>
    <p>Bitte denkt daran, eure Feedbacks zu den Inhalten direkt in die Excel Liste zu schreiben.</p>
    <p>&nbsp;</p>
    <p>Beste Gr&uuml;sse</p>
    <p>Euer Data Analytics Team</p>
    <p>&nbsp;</p>
    <hr />
    <p>&nbsp;</p>
    <p>Bonjour Toutes et Tous,</p>
    <p>Dans le classeur suivant vous trouverez, sous vos initiales,&nbsp; <a href="\\fppwi01\daten$\Service\Kennzahlen\Verkauf\PredictiveAnalytics\{name_depl_folder}">Les listes actualis&eacute;es concernant les pr&eacute;visions de vente</a>.</p>
    <p>Des informations compl&eacute;mentaires &agrave; ce sujet sont disponibles <a href="https://wiki.apgsga.ch/x/5pG8Ag">dans Wiki.</a></p>
    <p>Pensez &agrave; ajouter votre feedback directement dans la liste Excel (colonne jaune + X).</p>
    <p>&nbsp;</p>
    <p>Avec nos remerciements anticip&eacute;s et meilleures salutations.</p>
    <p>Votre Data Analytics Team</p>
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