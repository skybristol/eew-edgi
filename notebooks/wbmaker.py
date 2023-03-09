import os
import requests
import pandas as pd
from urllib.parse import urlparse
from urllib.parse import parse_qs
from wikibaseintegrator.wbi_config import config as wbi_config
from wikibaseintegrator import WikibaseIntegrator, wbi_login
from nested_lookup import nested_lookup

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
        self.prop_html_table = 'P31'

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

        SELECT ?ds ?dsLabel ?query_string ?html_table
        WHERE {
            ?ds wdt:%(prop_instance_of)s wd:%(class_dataset)s .
            OPTIONAL { ?ds wdt:%(prop_query_string)s ?query_string . }
            OPTIONAL { ?ds wdt:%(prop_html_table)s ?html_table . }
            SERVICE wikibase:label { bd:serviceParam wikibase:language "en" . }
        }
        """ % {
            'namespaces': self.sparql_namespaces(),
            'prop_instance_of': self.prop_instance_of,
            'class_dataset': self.class_dataset,
            'prop_query_string': self.prop_query_string,
            'prop_html_table': self.prop_html_table
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
    
    def get_html_table(self, url: str, table_ordinal: int = 0):
        tables_on_page = pd.read_html(url)
        
        if not tables_on_page:
            return
        
        # Build a converter to get everything as strings
        data_preview = tables_on_page[table_ordinal]
        converters = {c:str for c in data_preview.columns}
        
        tables_on_page = pd.read_html(url, converters=converters)
        
        return tables_on_page[table_ordinal]

    def get_claims(self, qid: str):
        existing_items_api = f"{os.environ['WIKIBASE_URL']}w/api.php?format=json&action=wbgetclaims&entity={qid}"
        r = requests.get(existing_items_api)

        return nested_lookup('mainsnak', r.json())
    
    def simplify_claim_value(self, row):
        if pd.notna(row['id']):
            return row['id']
        
        if 'latitude' in row and pd.notna(row['latitude']):
            return f"{str(row['latitude'])},{str(row['longitude'])}"
        
        return row[0]
    
    def qid_property_fetcher(self, qids: list):
        df_qids = pd.DataFrame(list(set(qids)), columns=["qid"])
        df_qids['claims'] = df_qids.qid.apply(self.get_claims)

        df_qids_claims = df_qids.explode('claims').reset_index(drop=True)

        df_qids_props = pd.concat([
            df_qids_claims.drop(['claims'], axis=1), 
            df_qids_claims['claims'].apply(pd.Series)
        ], axis=1)
        
        df_qids_props.drop(columns=["snaktype","hash"], inplace=True)
        
        df_qids_datavalues = pd.concat([
            df_qids_props.drop(['datavalue'], axis=1), 
            df_qids_props['datavalue'].apply(pd.Series)
        ], axis=1)

        df_qids_values = pd.concat([
            df_qids_datavalues.drop(['value'], axis=1), 
            df_qids_datavalues['value'].apply(pd.Series)
        ], axis=1)
        
        df_qids_values['value'] = df_qids_values.apply(self.simplify_claim_value, axis=1)

        return df_qids_values[["qid","property","value"]]