This repository shows our work progress for Query processing project. 

## Data Scraping, Cleaning

Initial data is downloaded from kaggle. (https://www.kaggle.com/code/chanoncharuchinda/sample-top-100-korean-dramas/data)
After that the cast data was scraped from wikipedia using the "CastingData2.ipynb" in 1_Data_Mining&Preparation
After the data was cleaned and turned into our required schema using "DataCleaner_Cast.ipynb" and "DataCleaner_Genre.ipynb" in 2_data folder
Images were gathered using "GetImage.ipynb" notebook

## CSV Adapter

Our primary goal was to combine CSV adapter and file adapter. However we extended the CSV adapter to do our project. Go to folder 3

### How to Run the code

 * Install IntelliJ IDEA
 * open the folder 3 and open pom.xml file. Open file as project
 * Build and run QueryProcessing2022Application
 * for demo run it on localhost:8080 (tomcat server)
 
### Code walkthrough

TO see application detail see presentation slide.Our application is a Springboot application. So we post the queries in index file. The index files the maps the query in QueryController.java and get the results from CsvService. CsvService gets the model from Model configuration. And this file connects to Calcite via the model.json file. 

As for Calcite's part the main code to classes to implement are CsvSchema and CsvSchemaFactory Class.This files return table in our required format. See the Calcite folder's code to see the entire implementation.

## File adapter implementation trial and other codes

Even though we did not show this in our main code however we did some other works to turn our code to o

