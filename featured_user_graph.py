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


def get_data(input_directory, file_list):
    outdata = pd.DataFrame()
    for file_name in file_list:
        path = input_directory + "/" + file_name
        data = pd.read_json(path, lines=True)
        outdata = pd.concat([outdata, data], ignore_index=True)
    return outdata


most_spend = "Most_Spend"
most_item = "Most_Item"
most_category = "Most_category"

most_spend_data = get_data(most_spend, get_file_names(most_spend))
most_item_data = get_data(most_item, get_file_names(most_item))
most_category_data = get_data(most_category, get_file_names(most_category))

app.layout = html.Div([
    html.H2("Most Money Spend, Most Item Bought, Most Categories",
            style={"padding": 20, "width": "80%", "margin-left": "auto", "margin-right": "auto"}),
    html.Div([
        dcc.Graph(
            id='most_spend',
            figure={
                'data': [
                    {
                        'labels': most_spend_data['category'],
                        'values': most_spend_data['sum(price)'],
                        # 'domain': {'column': 0},
                        'name': 'Cost over cate',
                        'hoverinfo': "sum(price)",
                        'hole': .4,
                        'type': 'pie',
                    }
                ],
                'layout': {
                    'width': 650,
                    'height': 500,
                    'title': 'Most Spend user Distributions cross Categories',
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
            id='most_item',
            figure={
                'data': [
                    {
                        'labels': most_item_data['category'],
                        'values': most_item_data['sum(count)'],
                        # 'domain': {'column': 0},
                        'name': 'Cost over cate',
                        'hoverinfo': "category",
                        'hole': .4,
                        'type': 'pie',
                    }
                ],
                'layout': {
                    'width': 650,
                    'height': 500,
                    'title': 'Most Item user Distributions cross Categories',
                    'margin': {'l': 40, 'b': 20, 't': 60, 'r': 40},
                    "annotations": [
                        {
                            "font": {
                                "size": 20
                            },
                            "showarrow": False,
                            "text": "Product",
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
            id='most_cate',
            figure={
                'data': [
                    {
                        'labels': most_category_data['category'],
                        'values': most_category_data['sum(price)'],
                        # 'domain': {'column': 0},
                        'name': 'Cost over cate',
                        'hoverinfo': "category",
                        'hole': .4,
                        'type': 'pie',
                    }
                ],
                'layout': {
                    'width': 650,
                    'height': 500,
                    'title': 'Most Categories Distributions',
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
        )
    ], style={'width': '47%', 'display': 'inline-block', 'padding': '0 20'})
])

if __name__ == '__main__':
    app.run_server(port=sys.argv[1])
