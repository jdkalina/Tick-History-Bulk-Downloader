class TickHistory:

	def __init__(self, name, pw):
		self.name = name
		self.pw = pw
		self.authenticate()


	def all_fields(self,template):
		"""
		template: 
		options available below:
		    'elektron_timeseries'
		    'historical_reference'
		    'intraday_summaries'
		    'time_and_sales'
		    'market_depth'
		"""
		from time import sleep
		import requests

		def create_url(report):
			_url = "https://hosted.datascopeapi.reuters.com/RestApi/v1/Extractions/GetValidContentFieldTypes(ReportTemplateType=ThomsonReuters.Dss.Api.Extractions.ReportTemplates.ReportTemplateTypes"
			return _url + "'"+ report + "')"

		_header={
				"Prefer":"respond-async",
				"Content-Type":"application/json",
				"Authorization": "Token " + self.token
			}

		if template == 'historical_reference':
			_url = create_url('HistoricalReference')
		elif template == 'elektron_timeseries':
			_url = create_url('ElektronTimeseries')
		elif template == 'intraday_summaries':
			_url = create_url('TickHistoryIntradaySummaries')
		elif template == 'time_and_sales':
			_url = create_url('TickHistoryTimeAndSales')
		elif template == 'market_depth':
			_url = create_url('TickHistoryMarketDepth')
		else:
			print("Template Name not found")

		fields = []
		resp = requests.get(_url, headers = _header)
		while resp.status_code != 200:
			sleep(1)
			resp = requests.get(_url, headers = _header)

		for i in json.loads(resp.content)['value']:
			fields.append(i['Name'])
			return fields
			
	def authenticate(self):
		_header={
			"Prefer":"respond-async",
            		"Content-Type":"application/json"
			}
			
		_body={"Credentials": {"Username": self.name,"Password": self.pw}}
        	_auth = requests.post("https://hosted.datascopeapi.reuters.com/RestApi/v1/Authentication/RequestToken",json=_body,headers=_header)

	        if _auth.status_code != 200:
			print('issue with the token')
		else:
			self.token = json.loads(_auth.text.encode('ascii', 'ignore'))["value"]

	def instruments(self, instrument, start, end, ischain = False):
		"""
	        instrument: Either a character string for a single RIC or Chain RIC or list of RICs.
        	start: "YYYY-MM-DD" format.
        	end: "YYYY-MM-DD" format.
        	ischain: True or False. is chain activates chain_expand if True.
		"""
		self.start = start
		self.end = end
		if ischain:
			self.expand_chain(instrument, start, end)
		else:
			if type(instrument) is str:
				self.rics = [instrument]
			else:
				self.rics = instrument
				
	
	def intraday_summaries(self, fields, interval):
		"""
		This method should be called for Tick History Intraday Summary Files.

		fields: A list of field names to be used with this historical reference files.
		interval:Options are below:
			'FifteenMinutes'
			'FiveMinutes'
			'FiveSeconds'
			'OneHour'
			'OneMinute'
			'OneSecond'
			'TenMinutes'
	        """
        
	        if interval in ['FifteenMinutes','FiveMinutes','FiveSeconds','OneHour','OneMinute','OneSecond','TenMinutes']:
			_header={
				"Prefer":"respond-async",
        		        "Content-Type":"application/json",
                		"Authorization": "Token " + self.token
			}

			_body={
				"ExtractionRequest": {
					"@odata.type": "#ThomsonReuters.Dss.Api.Extractions.ExtractionRequests.TickHistoryIntradaySummariesExtractionRequest",
					"ContentFieldNames": [],
					"IdentifierList": {
						"@odata.type": "#ThomsonReuters.Dss.Api.Extractions.ExtractionRequests.InstrumentIdentifierList",
						"InstrumentIdentifiers": [],
					},
					"Condition": {
						"ReportDateRangeType": "Range",
						"SummaryInterval": interval,
						"QueryStartDate": self.start,
						"QueryEndDate": self.end,
					}
				}
			}
			
			for i in fields:
				_body["ExtractionRequest"]["ContentFieldNames"].append(i)
			for i in self.rics:
				_body["ExtractionRequest"]["IdentifierList"]["InstrumentIdentifiers"].append({"Identifier": i,"IdentifierType": "Ric"})
			self.requestUrl = 'https://hosted.datascopeapi.reuters.com/RestApi/v1/Extractions/ExtractRaw'
			self.requestBody = _body
			self.requestHeader = _header
		else:
			print('error: Interval in Intraday Summaries is incorrect')

	def elektron_timeseries(self, fields):
		_header={
			"Prefer":"respond-async",
            		"Content-Type":"application/json",
            		"Authorization": "Token " + self.token
		}
		_body={
			"ExtractionRequest": {
				"@odata.type": "#ThomsonReuters.Dss.Api.Extractions.ExtractionRequests.ElektronTimeseriesExtractionRequest",
				"ContentFieldNames": [],
				"IdentifierList": {
					"@odata.type": "#ThomsonReuters.Dss.Api.Extractions.ExtractionRequests.InstrumentIdentifierList",
					"InstrumentIdentifiers": []
				},
				"Condition": {
					"ReportDateRangeType": "Range",
					"QueryStartDate": self.start,
					"QueryEndDate": self.end
				}
			}
		}

		for i in fields:
			_body["ExtractionRequest"]["ContentFieldNames"].append(i)
		for i in self.rics:
			_body["ExtractionRequest"]["IdentifierList"]["InstrumentIdentifiers"].append({"Identifier": i,"IdentifierType": "Ric"})
		self.requestUrl = 'https://hosted.datascopeapi.reuters.com/RestApi/v1/Extractions/ExtractWithNotes'
		self.requestBody = _body
	        self.requestHeader = _header
		
	def time_and_sales(self, fields):
		_header={
			"Prefer":"respond-async",
			"Content-Type":"application/json",
        		"Authorization": "Token " + self.token
		}
		
		_body={
			"ExtractionRequest": {
				"@odata.type": "#ThomsonReuters.Dss.Api.Extractions.ExtractionRequests.TickHistoryTimeAndSalesExtractionRequest",
				"ContentFieldNames": [],
				"IdentifierList": {
					"@odata.type": "#ThomsonReuters.Dss.Api.Extractions.ExtractionRequests.InstrumentIdentifierList",
                			"InstrumentIdentifiers": []
				},
		                "Condition": {
					"MessageTimeStampIn": "GmtUtc",
                			"ReportDateRangeType": "Range",
                    			"QueryStartDate": self.start,
                    			"QueryEndDate": self.end
				}
			}
		}
	        for i in fields:
			_body["ExtractionRequest"]["ContentFieldNames"].append(i)
		for i in self.rics:
			_body["ExtractionRequest"]["IdentifierList"]["InstrumentIdentifiers"].append({"Identifier": i,"IdentifierType": "Ric"})
		self.requestUrl = 'https://hosted.datascopeapi.reuters.com/RestApi/v1/Extractions/ExtractRaw'
        	self.requestBody = _body
        	self.requestHeader = _header

	def raw(self, fields):
		"""
		This method should be called for Tick History Raw Files.
	        fields: A list of field names to be used with this historical reference files.
        	"""
	        _header={
			"Prefer":"respond-async",
			"Content-Type":"application/json",
        		"Authorization": "Token " + self.token
		}
        	_body={
			"ExtractionRequest": {
				"@odata.type": "#ThomsonReuters.Dss.Api.Extractions.ExtractionRequests.TickHistoryRawExtractionRequest",
                		"ContentFieldNames": [],
                		"IdentifierList": {
					"@odata.type": "#ThomsonReuters.Dss.Api.Extractions.ExtractionRequests.InstrumentIdentifierList",
                    			"InstrumentIdentifiers": [],
                    			"ValidationOptions": {"AllowHistoricalInstruments": "true"},
                    			"UseUserPreferencesForValidationOptions": "false"
				},
				"Condition": {
					"ReportDateRangeType": "Range",
					"QueryStartDate": self.start,
					"QueryEndDate": self.end
				}
			}
		}
	        for i in fields:
			_body["ExtractionRequest"]["ContentFieldNames"].append(i)
		for i in self.rics:
			_body["ExtractionRequest"]["IdentifierList"]["InstrumentIdentifiers"].append({"Identifier": i,"IdentifierType": "Ric"})
		self.requestUrl = 'https://hosted.datascopeapi.reuters.com/RestApi/v1/Extractions/ExtractWithNotes'
		self.requestBody = _body
		self.requestHeader = _header

	def market_depth(self, fields):
		print("Note, market depth in this example is delivered as NormalizedLL2 format, there also exist RawMarketByOrder, RawMarketByPrice, RawMarketMaker, and LegacyLevel2")
		_header={
			"Prefer":"respond-async",
        		"Content-Type":"application/json",
            		"Authorization": "Token " + self.token
		}

	        _body={
			"ExtractionRequest": {
				"@odata.type": "#ThomsonReuters.Dss.Api.Extractions.ExtractionRequests.TickHistoryMarketDepthExtractionRequest",
        		        "ContentFieldNames": [],
        		        "IdentifierList": {
					"@odata.type": "#ThomsonReuters.Dss.Api.Extractions.ExtractionRequests.InstrumentIdentifierList",
                    			"InstrumentIdentifiers": []
				},
                		"Condition": {
					"View": "NormalizedLL2",
                    			"NumberOfLevels": 10,
                    			"MessageTimeStampIn": "GmtUtc",
                    			"ReportDateRangeType": "Range",
                    			"QueryStartDate": self.start,
                    			"QueryEndDate": self.end
				}
			}
		}

	        for i in fields:
			_body["ExtractionRequest"]["ContentFieldNames"].append(i)
		for i in self.rics:
			_body["ExtractionRequest"]["IdentifierList"]["InstrumentIdentifiers"].append({"Identifier": i,"IdentifierType": "Ric"})
		self.requestUrl = 'https://hosted.datascopeapi.reuters.com/RestApi/v1/Extractions/ExtractRaw'
		self.requestBody = _body
		self.requestHeader = _header

	def export(self, file):
        	import requests
		from time import sleep
        	import pandas as pd

	        _resp = requests.post(self.requestUrl, json=self.requestBody, headers=self.requestHeader)
        	self.status_code = _resp.status_code
        	if self.status_code == 202:
			_requestUrl = _resp.headers["location"]

            	_requestHeaders={
			"Prefer":"respond-async",
                	"Content-Type":"application/json",
                	"Authorization":"Token " + self.token
		}
		
		while (self.status_code == 202):
			sleep(30)
			_return = requests.get(_requestUrl,headers=_requestHeaders)
			self.status_code = _return.status_code

			if self.status_code != 200 :
				print('ERROR: An error occurred. Try to run this cell again. If it fails, re-run the previous cell.')

			if self.status_code == 200:
				pd.DataFrame(json.loads(_return.content)).dropna(how = 'all').to_csv(file,index = False)
				print('Successfully downloaded file')
			else:
				print(_status_code, "- Issue raised")
		elif self.status_code == 200:
			pd.DataFrame(json.loads(_return.content)).dropna(how = 'all').to_csv(file,index = False)
            		print('Successfully downloaded file')
		else:
			print('Oh Boy, not sure whats going on in the export command.')
	
	def expand_chain(self, chain, start, end):
        	import json
        
		_header = {"Prefer":"respond-async",
			"Content-Type":"application/json",
			"Authorization":"Token " + self.token
			}

		def bodyGenerator(chain, start, end):
			return {
				"Request": {
					"ChainRics": [chain],
				"Range": {
					"Start": start,
					"End": end
				}
			}
		}

	        _body = bodyGenerator(chain, start, end)
        	_chain = requests.post('https://hosted.datascopeapi.reuters.com/RestApi/v1/Search/HistoricalChainResolution',
					headers = _header, 
					json= _body)
		_data = json.loads(_chain.content)

		self.rics = []
       		for i in _data['value']:
			for p in i['Constituents']:
				self.rics.append(p['Identifier'])
				
	def serial_requests(self, template, concurrent_files, directory, file_name, fields, ifintervalsummary = ''):
		"""
		This is the post, async call, and download from the TRTH servers.
		:directory: this is the local path where you will be downloading your files.
		:template: options available below:
			'elektron_timeseries'
			'historical_reference'
			'intraday_summaries'
			'time_and_sales'
			'market_depth'
		:concurrent_files: should be a number 1-50
        	:directory: file path where the file where be saved.
        	:file_name: name of the output file (extension should not include '.csv')
        	:fields: list of fields for the template of choice
        	:ifintervalsummary: If using the intraday template what is the interval.
        	"""
		from datetime import datetime
		from math import ceil

		ric_uni = self.rics.copy()
		beg = 0
		num_rics = ceil(len(self.rics)/concurrent_files)
		end = num_rics
		for i in range(concurrent_files):
			self.rics = ric_uni[beg:end]
		    	if template == 'historical_reference':
				self.historical_reference(fields)
				self.async_post(file = directory + file_name + str(i + 1) + '.csv')
			elif template == 'elektron_timeseries':
				self.elektron_timeseries(fields)
				self.async_post(file = directory + file_name + str(i + 1) + '.csv')
			elif template == 'intraday_summaries':
				self.intraday_summaries(fields, interval = ifintervalsummary)
				self.async_post(file = directory + file_name + str(i + 1) + '.csv')
			elif template == 'time_and_sales':
				self.time_and_sales(fields)
				self.async_post(file = directory + file_name + str(i + 1) + '.csv')
			elif template == 'market_depth':
				self.market_depth(fields)
				self.async_post(file = directory + file_name + str(i + 1) + '.csv')
			else:                
				print("Template Name not found")            
			beg += num_rics
			end += num_rics
            
            
	def historical_reference(self, fields):
		"""
		This method should be called for Tick History Historical Reference Files.

		:fields: A list of field names to be used with this historical reference files.
		"""
		_header={
				"Prefer":"respond-async",
		    "Content-Type":"application/json",
		    "Authorization": "Token " + self.token
			}

		_body={
			"ExtractionRequest": {
				"@odata.type": "#ThomsonReuters.Dss.Api.Extractions.ExtractionRequests.HistoricalReferenceExtractionRequest",
				"ContentFieldNames": [],
				"IdentifierList": {
					"@odata.type": "#ThomsonReuters.Dss.Api.Extractions.ExtractionRequests.InstrumentIdentifierList",
					"InstrumentIdentifiers": [],
					"ValidationOptions": {"AllowHistoricalInstruments": "true"},
					"UseUserPreferencesForValidationOptions": "false"
					},
				"Condition": {
					"ReportDateRangeType": "Range",
			    		"QueryStartDate": self.start,
			    		"QueryEndDate": self.end
					}
				}
			}
		for i in fields:
			_body["ExtractionRequest"]["ContentFieldNames"].append(i)
		for i in self.rics:
			_body["ExtractionRequest"]["IdentifierList"]["InstrumentIdentifiers"].append({"Identifier": i,"IdentifierType": "Ric"})
		self.requestUrl = 'https://hosted.datascopeapi.reuters.com/RestApi/v1/Extractions/ExtractWithNotes'
		self.requestBody = _body
		self.requestHeader = _header
