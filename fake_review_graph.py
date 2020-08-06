import sys

import dash
import dash_core_components as dcc
import dash_html_components as html
import pandas as pd
import os
import warnings

warnings.simplefilter(action='ignore', category=FutureWarning)

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
app = dash.Dash(__name__, external_stylesheets=external_stylesheets)


def get_file_names(input_directory):
    result_list = []
    for file_name in os.listdir(input_directory):
        if file_name.endswith(".json"):
            result_list.append(file_name)
    return result_list


def get_detail_data(input_directory, file_list):
    outdata = pd.DataFrame()
    for file_name in file_list:
        path = input_directory + "/" + file_name
        data = pd.read_json(path, lines=True)
        outdata = pd.concat([outdata, data], ignore_index=True)
    return outdata


def get_interval_data(input_directory, file_list):
    outdata = pd.DataFrame()
    for file_name in file_list:
        path = input_directory + "/" + file_name
        data = pd.read_json(path, lines=True)
        outdata = pd.concat([outdata, data], ignore_index=True)
    return outdata


if len(sys.argv) != 4:
    print("Invalid argument")
    print("Usage:\nfake_review_graph.py [detail_input] [interval_input] [port]")
    exit(0)
detail = sys.argv[1]
interval = sys.argv[2]

# ================================THIS IS FOR DEMO USE=========================================
user_id = "Percentage_A32NEDX6UVV8DE"
user_data = get_detail_data(user_id, get_file_names(user_id))
if sys.argv[3] == str(8050):
    user_id = "Percentage_A16CZRQL23NOIW"
    user_data = get_detail_data(user_id, get_file_names(user_id))

# =============================================================================================

detail_data = get_detail_data(detail, get_file_names(detail))
frequency = detail_data.groupby('date_time').size().reset_index(name='count')
cate_ct = detail_data.groupby('category').size().reset_index(name='count')
brand_ct = detail_data.groupby('brand').size().reset_index(name='count')
interval_data = get_interval_data(interval, get_file_names(interval))
year_list = interval_data['year'].unique()
month_list = interval_data['month'].unique()

app.layout = html.Div([
    html.Div(
        className="row",
        children=[
            html.H2("Fake Review Analysis Result"),
            html.Div([
                html.Div([
                    dcc.Graph(
                        id='cate_dist',
                        figure={
                            'data': [
                                {
                                    'labels': cate_ct['category'],
                                    'values': cate_ct['count'],
                                    # 'domain': {'column': 0},
                                    'name': 'Category<br>distribution',
                                    'hoverinfo': "category",
                                    'hole': .4,
                                    'type': 'pie',
                                }
                            ],
                            'layout': {
                                'width': 650,
                                'height': 500,
                                'title': 'Reviews Distributions cross Categories',
                                'margin': {'l': 40, 'b': 20, 't': 60, 'r': 40},
                                "annotations": [
                                    {
                                        "font": {
                                            "size": 20
                                        },
                                        "showarrow": False,
                                        "text": "Category",
                                        "x": 0.50,
                                        "y": 0.50
                                    }
                                ]
                            }
                        }
                    ),
                ], style={'width': '47%', 'display': 'inline-block', 'padding': '0 20'}
                ),
                html.Div([
                    dcc.Graph(
                        id='user_percentage',
                        figure={
                            'data': [
                                {
                                    'labels': user_data['category'],
                                    'values': user_data['sum(price)'],
                                    # 'domain': {'column': 0},
                                    'name': 'Cost<br>distribution',
                                    'hoverinfo': "category",
                                    'hole': .4,
                                    'type': 'pie',
                                }
                            ],
                            'layout': {
                                'width': 650,
                                'height': 500,
                                'title': 'User Total Cost Distributions',
                                'margin': {'l': 40, 'b': 20, 't': 60, 'r': 40},
                                "annotations": [
                                    {
                                        "font": {
                                            "size": 20
                                        },
                                        "showarrow": False,
                                        "text": "Cost",
                                        "x": 0.50,
                                        "y": 0.50
                                    }
                                ]
                            }
                        }
                    )
                ], style={'width': '47%', 'display': 'inline-block', 'padding': '0 20'}),
                html.Div([
                    dcc.Graph(
                        id='brand_dist',
                        figure={
                            'data': [
                                {
                                    'labels': brand_ct['brand'],
                                    'values': brand_ct['count'],
                                    # 'domain': {'column': 0},
                                    'name': 'Category<br>distribution',
                                    'hoverinfo': "label",
                                    'hole': .4,
                                    'type': 'pie',
                                }
                            ],
                            'layout': {
                                'width': 850,
                                'height': 600,
                                'title': 'Brand Distributions',
                                'margin': {'l': 40, 'b': 20, 't': 60, 'r': 40},
                                "annotations": [
                                    {
                                        "font": {
                                            "size": 30
                                        },
                                        "showarrow": False,
                                        "text": "Brand",
                                        "x": 0.50,
                                        "y": 0.50
                                    }
                                ]
                            }
                        }
                    )
                ], style={'width': '47%', 'display': 'inline-block', 'padding': '0 20'}),

            ],
                style={"padding": 50, "width": "100%", "margin-left": "auto", "margin-right": "auto"}
            )
        ]
    ),
    html.Div(
        children=[
            dcc.Graph(
                id='interval',
                figure={
                    'data': [
                        {
                            'x': interval_data['date_time'],
                            'y': interval_data['date_diff'],
                            'type': 'points',
                            'name': 'interval',
                            'marker': {'color': 'F77F00'},
                        }
                    ],
                    'layout': {
                        'title': 'Interval of review in days over the years',
                        'width': 650,
                        'height': 500,
                        'margin': {'l': 40, 'b': 20, 'r': 60, 't': 80},
                        # 'yaxis': {'autorange': False, 'range': [-10, 60]}
                    }
                }
            )
        ],
        style={"padding": 50, "width": "60%", "margin-left": "auto", "margin-right": "auto"}
    ),
    html.Div([
        dcc.Graph(id='interval_year'),
        html.Div([
            dcc.Dropdown(
                id='select_year_only',
                options=[{'label': i, 'value': i} for i in year_list],
                value='',
                placeholder='select year'
            )
        ],
            style={"width": "80%", "float": "center"}
        ),
        dcc.Graph(id='interval_year_month'),
        html.Div([
            dcc.Dropdown(
                id='year_selected',
                options=[{'label': i, 'value': i} for i in year_list],
                value='',
                placeholder='select year'
            )
        ],
            style={"width": "50%", "float": "left"}
        ),
        html.Div([
            dcc.Dropdown(
                id='month_selected',
                options=[{'label': i, 'value': i} for i in month_list],
                value='',
                placeholder='select month'
            )
        ],
            style={"width": "50%", "float": "right"}
        )
    ],
        style={"padding": 50, "width": "60%", "margin-left": "auto", "margin-right": "auto"}
    )
])


@app.callback(
    dash.dependencies.Output('interval_year', 'figure'),
    [dash.dependencies.Input('select_year_only', 'value')]
)
def update_graph(year_selected):
    df = interval_data[interval_data['year'] == year_selected]
    return {
        'data': [dict(
            x=df['date_time'], y=df['date_diff'], type='bar', name='interval_year', marker={'color': '7917D5'})],
        'layout': {
            'title': 'Interval in days between reviews at ' + str(year_selected),
            'height': 300,
            'margin': {'l': 20, 'b': 50, 'r': 10, 't': 30},
            # 'yaxis': {'autorange': False, 'range': [-5, 25]}
        }
    }


@app.callback(
    dash.dependencies.Output('interval_year_month', 'figure'),
    [dash.dependencies.Input('year_selected', 'value'), dash.dependencies.Input('month_selected', 'value')]
)
def update_graph(year_selected, month_selected):
    df = interval_data[interval_data['year'] == year_selected]
    df = df[df['month'] == month_selected]
    return {
        'data': [dict(
            x=df['date_time'], y=df['date_diff'], type='bar', name='interval_year_month', marker={'color': 'E61271'})],
        'layout': {
            'title': 'Interval in days between reviews at ' + str(year_selected) + '-' + str(month_selected),
            'height': 300,
            'margin': {'l': 20, 'b': 50, 'r': 10, 't': 30},
            # 'yaxis': {'autorange': False, 'range': [-5, 15]}
        }
    }


if __name__ == '__main__':
    app.run_server(port=sys.argv[3])
