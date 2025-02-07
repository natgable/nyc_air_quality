# Analysis of NYC Air Quality

In this project, I aim to integrate the following datasets into a single analysis:
* NYC air quality surveillance data (from NYC Open Data)
    * This includes data about particle concentration in different neighborhoods over timie
    * This also includes data on air-quality-related health outcomes
* Household median income statistics (from the ACS)

This is a personal project with the goals of 1) to practice writing code, 2) to practice doing some readable analysis, and 3) to hopefully make some nice maps!
This is a work in progress - more to come!

The main file in this project is `analysis.ipynb`. That is where all the analysis and plotting is done. `extract.py` includes some simple
functions for reading and wrangling data into some working dataframes from NYC Open Data air quality dataset, the ACS 5-year survey, and the raw shapefiles 
from NYC Open Data.

Scratch work is in `scratch/`.
