import os
import pandas as pd
import numpy as np
import datetime as dt
import gspread 
import dash
from dash import dcc, html, dash_table
import json
from google.oauth2.service_account import Credentials

# Load service account JSON from the Render Secret File
with open("/etc/secrets/RENDER_SECRET") as f:
    service_account_info = json.load(f)

credentials = Credentials.from_service_account_info(
    service_account_info,
    scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
)
gc = gspread.authorize(credentials)

pd.set_option('display.expand_frame_repr', False)  # Prevent line wrapping

def main():
    # Wild Card picks
    contestbeta = gc.open("2025 Playoffs - Wild Card (Responses)")
    pickinput = contestbeta.worksheet("Form Responses 1")
    picksraw = pd.DataFrame(pickinput.get_all_records())

    # Divisional picks
    divisional_contest = gc.open("2025 Playoffs - Divisional (Responses)")
    divisional_input = divisional_contest.worksheet("Form Responses 1")
    divisional_raw = pd.DataFrame(divisional_input.get_all_records())

    # Rename and process Wild Card picks
    picks = picksraw.set_axis(
        ['timestamp', 'email', 'name', 'afc1', 'afc1con', 'afc2', 'afc2con', 'afc3', 'afc3con',
         'nfc1', 'nfc1con', 'nfc2', 'nfc2con', 'nfc3', 'nfc3con', 'notes'],
        axis=1
    ).drop(columns=['email', 'notes'])
    picks['timestamp'] = pd.to_datetime(picks['timestamp'])

    # Rename and process Divisional picks
    divisional_picks = divisional_raw.set_axis(
        ['timestamp', 'email', 'name', 'afc4', 'afc4con', 'nfc4', 'nfc4con', 'nfc5', 'nfc5con', 'afc5', 'afc5con', 'notes'],
        axis=1
    ).drop(columns=['email', 'notes'])
    divisional_picks['timestamp'] = pd.to_datetime(divisional_picks['timestamp'])

    # Combine both rounds of picks
    combined_picks = pd.concat([picks, divisional_picks], ignore_index=True)

    # Define deadlines for Wild Card and Divisional rounds
    wild_card_deadlines = {
        'afc1': pd.Timestamp('2025-01-11 13:30:00'),
        'afc2': pd.Timestamp('2025-01-11 13:30:00'),
        'afc3': pd.Timestamp('2025-01-12 10:00:00'),
        'nfc1': pd.Timestamp('2025-01-12 10:00:00'),
        'nfc2': pd.Timestamp('2025-01-12 10:00:00'),
        'nfc3': pd.Timestamp('2025-01-12 10:00:00')
    }

    divisional_deadlines = {
        'afc4': pd.Timestamp('2025-01-18 13:30:00'),
        'nfc4': pd.Timestamp('2025-01-18 13:30:00'),
        'nfc5': pd.Timestamp('2025-01-19 12:00:00'),
        'afc5': pd.Timestamp('2025-01-19 12:00:00')
    }

    # Combine deadlines
    game_deadlines = {**wild_card_deadlines, **divisional_deadlines}

    # Pivot to long format, preserving multiple picks
    picks_long = pd.melt(combined_picks, 
                         id_vars=['timestamp', 'name'], 
                         value_vars=['afc1', 'afc2', 'afc3', 'nfc1', 'nfc2', 'nfc3',
                                     'afc4', 'nfc4', 'nfc5', 'afc5'],
                         var_name='game', 
                         value_name='pick')

    conf_long = pd.melt(combined_picks,
                        id_vars=['timestamp', 'name'], 
                        value_vars=['afc1con', 'afc2con', 'afc3con', 'nfc1con', 'nfc2con', 'nfc3con',
                                    'afc4con', 'nfc4con', 'nfc5con', 'afc5con'],
                        var_name='game', 
                        value_name='confidence')

    # Clean confidence column names to match picks
    conf_long['game'] = conf_long['game'].str.replace('con', '')

    # Merge picks with confidence
    df_merged = pd.merge(picks_long, conf_long, on=['timestamp', 'name', 'game'])

    # Scrub pick data to extract only the team name
    df_merged['pick'] = df_merged['pick'].str.extract(r'^(\w+)')

    # Assign deadline based on the game
    df_merged['deadline'] = df_merged['game'].map(game_deadlines)

    # Flag late picks
    df_merged['late'] = df_merged['timestamp'] > df_merged['deadline']

    # Sort 
    df_merged.sort_values(by=['game', 'name', 'timestamp'], inplace=True)

    # Filter to valid picks
    valid_picks = df_merged[df_merged['timestamp'] <= df_merged['deadline']]
    valid_picks = valid_picks[valid_picks['pick'] != '']
    valid_picks = valid_picks.sort_values(by=['timestamp']).groupby(['name', 'game']).tail(1)
    valid_picks = valid_picks.dropna(subset=['confidence'])
    valid_picks = valid_picks[valid_picks['confidence'].apply(lambda x: isinstance(x, int))]

    # Define the game order
    game_order = {
        'afc1': 1, 'afc2': 2, 'afc3': 3, 'nfc1': 4, 'nfc2': 5, 'nfc3': 6,
        'afc4': 7, 'nfc4': 8, 'nfc5': 9, 'afc5': 10
    }
    valid_picks['game_order'] = valid_picks['game'].map(game_order)
    valid_picks['dupval'] = None

    # Zero out duplicates
    used_conf = {}
    for idx, row in valid_picks.iterrows():
        player = row['name']
        conf = row['confidence']

        if player not in used_conf:
            used_conf[player] = set()

        if conf in used_conf[player]:
            valid_picks.at[idx, 'confidence'] = 0
            valid_picks.at[idx, 'dupval'] = conf
        else:
            used_conf[player].add(conf)

    valid_picks = valid_picks.drop(columns=['game_order'])

    # Prepare game_info
    game_info = pd.DataFrame({
        "game": list(game_deadlines.keys()),
        "deadline": list(game_deadlines.values())
    })

    # Prepare player_scores
    player_scores = valid_picks.groupby('name')['confidence'].sum().reset_index(name='total_points')

    # Prepare picks_results_pivot_with_status
    picks_results_pivot_with_status = valid_picks.pivot(
        index='name',
        columns='game',
        values='confidence'
    ).fillna('-')

    return game_info, player_scores, picks_results_pivot_with_status

# Initialize Dash app
app = dash.Dash(__name__)

# Call main and unpack returned values
game_info, player_scores, picks_results_pivot_with_status = main()

app.layout = html.Div([
    html.H1("NFL Playoff Contest Results"),

    html.H2("Game Results"),
    dash_table.DataTable(
        data=game_info.to_dict('records'),
        columns=[{"name": i, "id": i} for i in game_info.columns],
        style_table={'overflowX': 'auto'},
        style_cell={'textAlign': 'center', 'padding': '10px'},
        style_header={'backgroundColor': 'lightblue', 'fontWeight': 'bold'}
    ),

    html.H2("Scoreboard"),
    dash_table.DataTable(
        data=player_scores.to_dict('records'),
        columns=[
            {"name": "Name", "id": "name"},
            {"name": "Total Points", "id": "total_points"},
            {"name": "Remaining Confidence Points", "id": "remaining_conf"}
        ],
        style_table={'overflowX': 'auto'},
        style_cell={'textAlign': 'center', 'padding': '10px'},
        style_header={'backgroundColor': 'lightblue', 'fontWeight': 'bold'}
    ),

    html.H2("Player Picks and Points"),
    dash_table.DataTable(
        data=picks_results_pivot_with_status.reset_index().to_dict('records'),
        columns=[{"name": i, "id": i} for i in picks_results_pivot_with_status.reset_index().columns if not i.endswith('_status')],
        style_table={'overflowX': 'auto'},
        style_cell={'textAlign': 'center', 'padding': '10px'},
        style_header={'backgroundColor': 'lightblue', 'fontWeight': 'bold'},
        style_data_conditional=[
            {
                'if': {
                    'filter_query': '{{{col}_status}} = "correct"'.format(col=col),
                    'column_id': col
                },
                'backgroundColor': 'lightgreen',
                'color': 'black',
            } for col in picks_results_pivot_with_status.columns if not col.endswith('_status')
        ] + [
            {
                'if': {
                    'filter_query': '{{{col}_status}} = "incorrect"'.format(col=col),
                    'column_id': col
                },
                'backgroundColor': 'lightcoral',
                'color': 'black',
            } for col in picks_results_pivot_with_status.columns if not col.endswith('_status')
        ]
    )
], style={'fontFamily': 'Arial, sans-serif'})

# Run the Dash app
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8000))  # Default to 8000 if PORT is not set
    app.run_server(debug=True, host='0.0.0.0', port=port)
