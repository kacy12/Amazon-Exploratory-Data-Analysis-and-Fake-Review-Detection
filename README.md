# CMPT732_project

In this project, we did some exploration on Amazon review data and Amazon product data.

1. General statistics over rating, reviews, categories from 2003 to 2013.

2. Product popularity analysis, shows users shopping trends in different time period through all categories.

3. Brand effect analysis, shows ranking of all brands and how it effects users choice.

4. Possible fake reviewer detection and analysis. This algorithm will detect potential fake reviewer based on their average score, product category distribution, review frequency and summary keywords.

5. Correlation between all categories.

6. Web front-end was implemented by python using plotly dash.


Please refer to RUNNING.txt to test and run all programs, and details about the project were illustrated in the report.

Instructions on running sample data(Archive)
Python module Dependencies: 
dash
dash_core_components
dash_html_components

UI with sample data can be shown by simply running:
<br>>python3 featured_user_graph.py
<br>>python3 plot_rating.py
<br>>python3 plot_brand.py
<br>>python3 plot_popularity.py

The web front-end can be accessed by:
<br>http://127.0.0.1:8050/
<br>http://127.0.0.1:8051/
<br>http://127.0.0.1:8052/