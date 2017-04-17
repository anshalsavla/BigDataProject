Authors: Anshal Savla, Drumil Bakhai, Tannavee Watts
Project: BigData Part1 
Data: 311 complaints -https://data.cityofnewyork.us/Social-Services/311/wpe2-h2i5

Subdirectories:
    CleaningScripts
        This directory contains 52 scripts to find dataquality issues in each of the
        52 columns in the dataset and generates a txt file for 
        each column indicating the [basetype, semantictype and label]. 
        The output looks like:
        ===========================================================================
        |          BaseType         |     Semantic Type        |      Label       |
        |---------------------------|--------------------------|------------------|
        |int,float, string, datetime|Key, Address, Phonenumber,|Valid, Invalid,   |
        |                           |Zipcode, Location,DateTime|N/A               |
        ===========================================================================

        Details on running the cleaning script are present in the README.md file in the CleaningScripts directory.

    AnalysisScripts

