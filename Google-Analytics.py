"""
Created on Fri Jul 20 12:42:33 2018

@author: nauyan.rashid
"""

"""Hello Analytics Reporting API V4."""

from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import datetime


SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
KEY_FILE_LOCATION = 'Service Account Credentials'
VIEW_ID = '168834542'


def initialize_analyticsreporting():
  """Initializes an Analytics Reporting API V4 service object.

  Returns:
    An authorized Analytics Reporting API V4 service object.
  """
  credentials = ServiceAccountCredentials.from_json_keyfile_name(
      KEY_FILE_LOCATION, SCOPES)

  # Build the service object.
  analytics = build('analyticsreporting', 'v4', credentials=credentials)

  return analytics


def get_report(analytics):
  """Queries the Analytics Reporting API V4.

  Args:
    analytics: An authorized Analytics Reporting API V4 service object.
  Returns:
    The Analytics Reporting API V4 response.
  """
  return analytics.reports().batchGet(
      body={
        'reportRequests': [
        {
          'viewId': VIEW_ID,
          'dateRanges': [{'startDate': 'yesterday', 'endDate': 'yesterday'}],
          'metrics': [{'expression': 'ga:sessions'},{'expression': 'ga:newUsers'},{'expression': 'ga:bounces'},
          {'expression': 'ga:pageviewsPerSession'},{'expression': 'ga:avgSessionDuration'},{'expression': 'ga:avgTimeOnPage'},
          {'expression': 'ga:avgPageLoadTime'},{'expression': 'ga:avgServerResponseTime'},{'expression': 'ga:users'}],
          'dimensions': [{'name': 'ga:cityId'},{'name': 'ga:deviceCategory'},{'name': 'ga:browser'},{'name': 'ga:dateHour'},
          {'name': 'ga:sourceMedium'},{'name': 'ga:screenResolution'}],
          'pageSize': 10000
        }]
      }
  ).execute()
#  
#,{'name': 'ga:deviceCategory'},{'name': 'ga:browser'},{'name': 'ga:dateHour'},{'name': 'ga:sourceMedium'},{'name': 'ga:screenResolution'}
def print_response(response):
  list = []
  # get report data
  for report in response.get('reports', []):
    # set column headers
    columnHeader = report.get('columnHeader', {})
    dimensionHeaders = columnHeader.get('dimensions', [])
    metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])
    rows = report.get('data', {}).get('rows', [])
    
    for row in rows:
        # create dict for each row
        dict = {}
        dimensions = row.get('dimensions', [])
        dateRangeValues = row.get('metrics', [])

        # fill dict with dimension header (key) and dimension value (value)
        for header, dimension in zip(dimensionHeaders, dimensions):
          dict[header] = dimension

        # fill dict with metric header (key) and metric value (value)
        for i, values in enumerate(dateRangeValues):
          for metric, value in zip(metricHeaders, values.get('values')):
            #set int as int, float a float
            if ',' in value or ',' in value:
              dict[metric.get('name')] = float(value)
            else:
              dict[metric.get('name')] = float(value)

        list.append(dict)
    
    df = pd.DataFrame(list)
    fn=datetime.datetime.today().strftime('%Y-%m-%d')
    df.to_csv(str(fn)+".csv", sep='\t')
    #print(df)
    return df

def main():
  analytics = initialize_analyticsreporting()
  response = get_report(analytics)
  print_response(response)

if __name__ == '__main__':
  main()