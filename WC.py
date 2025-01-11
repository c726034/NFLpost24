import os
import json
import gspread
from google.oauth2.service_account import Credentials
import dash
from dash import dcc, html, dash_table

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

# Data Preparation
# Load your Sheets and process as needed
# Example placeholder - Replace with your actual logic
contestbeta = gc.open("2024 Playoffs - Wild Card (Responses)")
worksheet = contestbeta.sheet1
data = worksheet.get_all_records()

# Process data
# Placeholder variables (replace with actual data processing code)
valid_picks = ...  # Processed data from Google Sheets
player_confidence = ...  # Derived confidence data
wc_results = ...  # Results table loaded elsewhere

# Prepare Game Info Table
game_info = wc_results[['game', 'away', 'home', 'homeline', 'awaypts', 'homepts', 'winner_ATS']]

# Prepare Player Scores Table
player_scores = valid_picks.groupby('name')['points_won'].sum().reset_index(name='total_points')
player_scores = player_scores.merge(
    player_confidence[['name', 'remaining_conf']], 
    on='name', 
    how='left'
)
player_scores['remaining_conf'] = player_scores['remaining_conf'].apply(lambda x: ', '.join(map(str, x)))

# Prepare Full Picks Results Table
picks_results = valid_picks[['name', 'game', 'pick', 'confidence', 'points_won']]

# Create Dash App
app = dash.Dash(__name__)

# Dash Layout
app.layout = html.Div([
    html.H1("NFL Playoff Contest Results"),

    # Game Results Table
    html.H2("Game Results"),
    dash_table.DataTable(
        data=game_info.to_dict('records'),
        columns=[{"name": i, "id": i} for i in game_info.columns],
        style_table={'overflowX': 'auto'},
        style_cell={'textAlign': 'center', 'padding': '10px'},
        style_header={'backgroundColor': 'lightblue', 'fontWeight': 'bold'}
    ),

    # Player Scores Summary Table
    html.H2("Player Score Summary"),
    dash_table.DataTable(
        data=player_scores.to_dict('records'),
        columns=[{"name": i, "id": i} for i in player_scores.columns],
        style_table={'overflowX': 'auto'},
        style_cell={'textAlign': 'center', 'padding': '10px'},
        style_header={'backgroundColor': 'lightblue', 'fontWeight': 'bold'}
    ),

    # Full Picks Results Table
    html.H2("Player Picks and Points"),
    dash_table.DataTable(
        data=picks_results.to_dict('records'),
        columns=[{"name": i, "id": i} for i in picks_results.columns],
        style_table={'overflowX': 'auto'},
        style_cell={'textAlign': 'center', 'padding': '10px'},
        style_header={'backgroundColor': 'lightblue', 'fontWeight': 'bold'}
    )
])

# Run the Dash app
if __name__ == '__main__':
    app.run_server(debug=True)
