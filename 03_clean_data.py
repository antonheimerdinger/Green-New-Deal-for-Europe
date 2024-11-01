import pandas as pd

df = pd.read_parquet('combined_data/EinheitenSolar.parquet', engine='pyarrow')

print(df.shape)

STATES = {
    1400: 'Brandenburg',
    1401: 'Berlin',
    1402: 'Baden-Württemberg',
    1403: 'Bayern',
    1404: 'Bremen',
    1405: 'Hessen',
    1406: 'Hamburg',
    1407: 'Mecklenburg-Vorpommern',
    1408: 'Niedersachsen',
    1409: 'Nordrhein-Westfalen',
    1410: 'Rheinland-Pfalz',
    1411: 'Schleswig-Holstein',
    1412: 'Saarland',
    1413: 'Sachsen',
    1414: 'Sachsen-Anhalt',
    1415: 'Thüringen',
    1416: 'Ausschließliche Wirtschaftszone'
}

# Specify the columns to retain despite having more than 1 million missing values
retain_columns = ["DatumEndgueltigeStilllegung"]

# Drop columns with more than 1 million missing values except the specified ones
df = df.dropna(thresh=len(df) - 1_000_000, axis=1).join(df[retain_columns])

df = df.dropna(subset='Bundesland')

df['Bundesland'] = df['Bundesland'].astype(int)
df['Bundesland'] = df['Bundesland'].map(STATES)

df = df.loc[df['Bundesland'] != 'Ausschließliche Wirtschaftszone']

df = df[df['DatumEndgueltigeStilllegung'].isna()]
df = df.drop(columns='DatumEndgueltigeStilllegung')

df['Registrierungsdatum'] = pd.to_datetime(df['Registrierungsdatum'])
df['Inbetriebnahmedatum'] = pd.to_datetime(df['Inbetriebnahmedatum'])

print(df.shape)

df.to_parquet('combined_data/cleaned_data.parquet', index=False)
