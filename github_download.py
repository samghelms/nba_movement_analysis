"""Creds to https://sookocheff.com/post/tools/downloading-directories-of-code-from-github-using-the-github-api/
"""
import base64
import logging
import requests

def read_write_file_from_github(filename, repository):
    try:
        path = filename.path
        r = requests.get('https://github.com/samghelms/nba-movement-data/raw/master/'+path)
        file_content = r.content
        file_out = open("data/temp/"+filename.name, "wb")
        file_out.write(file_content)
        file_out.close()
    except IOError as exc:
        logging.error('Error processing %s: %s', filename.path, exc)
