#!/usr/bin/python

#------------------------------------------------------------------------------
#           Name: addSearchJobsToDBTable.py
#         Author: Ra Inta, 20150709
#  Last Modified: 20150709
# This reads the search results files and adds them to a SQLite database
#------------------------------------------------------------------------------

import xml.etree.ElementTree as ET
import subprocess
import bz2
import sqlite3
from string import lstrip, rstrip, split, find
import os


search_bands_file = bz2.BZ2File( "search_bands.xml.bz2", 'rb')
#fscan_H1_file =    bz2.BZ2File( "fscan_sfts_H1.txt.bz2", 'rb')
#fscan_L1_file =    bz2.BZ2File( "fscan_sfts_L1.txt.bz2", 'rb')
fscan_H1_file = "fscan_sfts_H1.txt.bz2"
fscan_L1_file = "fscan_sfts_L1.txt.bz2"

searchBaseDir = os.path.abspath("jobs/search")
searchResultFilenames = []

def findSearchResults( dirName ):
    '''Takes in a directory name and walks down the tree until it finds
    search_results.txt. files, adding them to the list'''
    for dirname, dirnames, filenames in os.walk( dirName ):
        for filename in filter(lambda x: x.find("search_results.txt."), filenames):
          searchResultFilenames.append(os.path.join(dirname, filename))
    pass


db_name = "search_db"


def parseSearchResults( searchResultsFile ):
    """Reads in textfile and puts into its own database table"""
    searchResultText = []
    for lines in open( searchResultsFile, 'r').readlines():
        if not lines[0] == '%':
            searchResultText.append(lines)
    return searchResultText



