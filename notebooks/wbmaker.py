import os
import requests
import pandas as pd
from urllib.parse import urlparse
from urllib.parse import parse_qs
from wikibaseintegrator.wbi_config import config as wbi_config
from wikibaseintegrator import WikibaseIntegrator, wbi_login

class WikibaseConnection:
    def __init__(self, bot_name: str):
        if "SPARQL_ENDPOINT_URL" not in os.environ:
            raise ValueError("Environment does not appear to contain required variables to run this code.")
            
        self.sparql_endpoint = os.environ['SPARQL_ENDPOINT_URL']
        self.wikibase_url = os.environ['WIKIBASE_URL']
        
        # Hard assumed values
        self.prop_instance_of = 'P1'
        self.prop_subclass_of = 'P2'
        self.class_dataset = 'Q11'
        self.prop_query_string = 'P29'

        # WikibaseIntegrator config
        wbi_config['MEDIAWIKI_API_URL'] = os.environ['MEDIAWIKI_API_URL']
        wbi_config['SPARQL_ENDPOINT_URL'] = os.environ['SPARQL_ENDPOINT_URL']
        wbi_config['WIKIBASE_URL'] = os.environ['WIKIBASE_URL']
        wbi_config['USER_AGENT'] = f'{bot_name}/1.0 ({os.environ["WIKIBASE_URL"]})'
        
        # Establish authentication connection to instance
        login_instance = wbi_login.Login(
            user=os.environ['BOT_NAME'],
            password=os.environ['BOT_PASS']
        )
        self.wbi = WikibaseIntegrator(login=login_instance)        

    # Parameters
    def sparql_namespaces(self):
        namespaces = """
        PREFIX wd: <%(wikibase_url)sentity/>
        PREFIX wdt: <%(wikibase_url)sprop/direct/>
        
        """ % {'wikibase_url': self.wikibase_url}

        return namespaces

    # Core Functions
    def item_by_label(self, label: str):
        label_query = """
        SELECT ?item
        WHERE {
            ?item ?label "%s"@en .
        }
        """ % label
        
        results = self.sparql_query(
            query=label_query,
            output="raw"
        )
        
        return results
    
    def properties(self, output: str = 'lookup'):
        prop_query = """
        %s

        SELECT ?property ?propertyLabel ?property_type WHERE {
        ?property a wikibase:Property .
        ?property wikibase:propertyType ?property_type .
        SERVICE wikibase:label { bd:serviceParam wikibase:language "en" . }
        }
        """ % self.sparql_namespaces()

        return self.sparql_query(
            query=prop_query,
            output=output
        )

    def classification(self, output: str = 'lookup'):
        class_query = """
        %(namespaces)s

        SELECT ?item ?itemLabel 
        WHERE {
            ?item wdt:%(prop_subclass)s ?subclass.
            SERVICE wikibase:label { bd:serviceParam wikibase:language "en" . }
        }
        """ % {
            'namespaces': self.sparql_namespaces(),
            'prop_subclass': self.prop_subclass_of
        }
        
        return self.sparql_query(
            query=class_query,
            output=output
        )

    def datasources(self, output: str = 'dataframe'):
        datasource_query = """
        %(namespaces)s

        SELECT ?ds ?dsLabel ?query_string
        WHERE {
            ?ds wdt:%(prop_instance_of)s wd:%(class_dataset)s .
            OPTIONAL { ?ds wdt:%(prop_query_string)s ?query_string . }
            SERVICE wikibase:label { bd:serviceParam wikibase:language "en" . }
        }
        """ % {
            'namespaces': self.sparql_namespaces(),
            'prop_instance_of': self.prop_instance_of,
            'class_dataset': self.class_dataset,
            'prop_query_string': self.prop_query_string
        }
        
        return self.sparql_query(
            query=datasource_query,
            output=output
        )

    # Utilities
    def sparql_query(self, query: str, endpoint: str = None, output: str = 'raw'):
        if not endpoint:
            endpoint = self.sparql_endpoint
        
        r = requests.get(
            endpoint, 
            params = {'format': 'json', 'query': query}
        )

        if r.status_code != 200:
            return

        try:
            json_results = r.json()
        except Exception as e:
            return
        
        if not json_results['results']['bindings']:
            return

        if output == 'raw':
            return json_results
        else:
            data_records = []
            var_names = json_results['head']['vars']

            for record in json_results['results']['bindings']:
                data_record = {}
                for var_name in var_names:
                    data_record[var_name] = record[var_name]['value'] if var_name in record else None
                data_records.append(data_record)

            if output == 'dataframe':
                return pd.DataFrame(data_records)
            elif output == 'lookup':
                # Assumes first column contains identifier and second column contains label
                df = pd.DataFrame(data_records)
                df['lookup_value'] = df.iloc[:, 1]
                df['identifier'] = df.iloc[:, 0].apply(lambda x: x.split('/')[-1])
                return df[['lookup_value','identifier']].set_index('lookup_value').to_dict()['identifier']
            else:
                return data_records

    def parse_sparql_url(self, url: str, param: str = 'query'):
        x = urlparse(url)
        
        sparql_endpoint=f"{x.scheme}://{x.netloc}{x.path}"
        sparql_query = parse_qs(x.query)[param][0]
        
        return sparql_endpoint, sparql_query
