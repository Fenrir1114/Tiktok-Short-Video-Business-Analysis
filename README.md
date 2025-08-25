# Tiktok-Short-Video-Business-Analysis
I. Project Overview
This project aims to systematically process and analyze the active behavior of short video platform creators, the characteristics of shopping and non-shopping videos, variable construction, descriptive statistics, and correlation analysis. Through efficient data cleaning, variable construction, and multi-dimensional statistical analysis, it seeks to uncover key factors influencing the effectiveness of short video shopping promotions and provide data support for subsequent research and business decisions.

II. Data Sources
The data originally used in this project was obtained by Professor Wang Juan of Chongqing University and her team through methods such as web scraping and platform cooperation. The runtime records in the Notebook and the files in the Research Product folder were all generated from the original data. Due to data confidentiality and other considerations, the data actually imported into the data folder is only a minimal excerpt of the real data, serving as a reference for the project without practical significance.

III. Methodology Overview
1.Data Processing and Active Creator Filtering:
Utilize Dask to efficiently read large-scale data, filter active creators within the target period, distinguish between shopping and non-shopping videos, and perform multi-table association and data cleaning.
2.Field-Level Statistical Analysis:
For each field, calculate unique values, missing values, common values, etc., and output an Excel report to aid data understanding and subsequent modeling.
3.Variable Construction:
For shopping and non-shopping samples, construct outcome variables, antecedent variables, moderating variables, and control variables, and output a standardized variable indicator table.
4.Descriptive Statistics and Visualization:
Compute descriptive statistics such as mean, median, and variance for each variable, generate distribution comparison charts (e.g., hourly distribution, weekly distribution, violin plots, etc.), and output them to Excel.
5.Correlation Analysis:
Calculate correlation coefficients and significance between variables, generate distribution comparison charts for grouped variables and outcome variables, and output a comprehensive analysis report.

IV. Key Tools
Data Processing and Analysis: Pandas, Dask
Excel File Reading/Writing: openpyxl, XlsxWriter
Data Visualization: Matplotlib, Seaborn
Numerical Computation and Statistical Analysis: NumPy, SciPy
Image Processing: PIL

V. Project Structure
.
├── Python Script/
│ ├── 1.Data Processing, Active Blogger Filtering and Report Association.py
│ ├── 2.Field Statistics.py
│ ├── 3.Variable Construction.py
│ ├── 4.Descriptive Statistics.py
│ └── 5.Correlation Analysis.py
├── Research Product/
│ ├── Example of Correlation Analysis.png
│ ├── Example of Frequency Distribution Chart.jpg
│ └── Example of Violin Diagram.jpg
├── Notebook.ipynb
├── READ ME.md
└── data/
├── video_data.csv
├── authors_info.csv
├── product_data.csv
├── fan_trend.csv
└── ... (intermediate output and analysis result files)
