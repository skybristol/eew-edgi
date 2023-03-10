import os
import requests
import pandas as pd
from urllib.parse import urlparse
from urllib.parse import parse_qs
from wikibaseintegrator.wbi_config import config as wbi_config
from wikibaseintegrator import WikibaseIntegrator, wbi_login
from wikibaseintegrator import models, datatypes
from nested_lookup import nested_lookup

class WikibaseConnection:
    def __init__(self, bot_name: str):
        if "SPARQL_ENDPOINT_URL" not in os.environ:
            raise ValueError("Environment does not appear to contain required variables to run this code.")
            
        self.sparql_endpoint = os.environ['SPARQL_ENDPOINT_URL']
        self.wikibase_url = os.environ['WIKIBASE_URL']
        
        # Hard assumed values
        # Knowing these values in advance saves some time
        self.prop_instance_of = 'P1'
        self.prop_subclass_of = 'P2'
        self.class_dataset = 'Q11'
        self.prop_query_string = 'P29'
        self.prop_html_table = 'P31'
        self.prop_classifier = 'P35'
        self.prop_declaration = 'P37'
        
        self.models = models
        self.datatypes = datatypes

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
    
    def properties(self):
        prop_query = """
        %s

        SELECT ?property ?propertyLabel ?property_type WHERE {
        ?property a wikibase:Property .
        ?property wikibase:propertyType ?property_type .
        SERVICE wikibase:label { bd:serviceParam wikibase:language "en" . }
        }
        """ % self.sparql_namespaces()

        df = self.sparql_query(
            query=prop_query,
            output='dataframe'
        )
        
        df['pid'] = df.property.apply(lambda x: x.split('/')[-1])
        df['p_type'] = df.property_type.apply(lambda x: x.split('#')[-1])
        
        return df

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
    
    def datasets(self):
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

    def datasource(self, ds_qid: str, output: str = 'dataframe'):
        ds_query = """
        %(namespaces)s

        SELECT ?wdLabel ?ps_ ?ps_Label ?wdpqLabel ?pq_Label 
        {
          VALUES (?datasource) {(wd:%(ds_qid)s)}

          ?datasource ?p ?statement .
          ?statement ?ps ?ps_ .

          ?wd wikibase:claim ?p.
          ?wd wikibase:statementProperty ?ps.

          OPTIONAL {
          ?statement ?pq ?pq_ .
          ?wdpq wikibase:qualifier ?pq .
          }

          SERVICE wikibase:label { bd:serviceParam wikibase:language "en" }
        } ORDER BY ?datasourceLabel ?wd ?statement ?ps_
        """ % {
            'namespaces': self.sparql_namespaces(),
            'ds_qid': ds_qid
        }

        ds_wb_source = self.sparql_query(
            query=ds_query,
            output="dataframe"
        )
        
        if output == 'datatrame':
            return ds_wb_source
        
        return self.format_datasource(ds_qid, ds_wb_source)

    def format_datasource(self, ds_qid, ds_wb_source):
        d_config = {
            ds_qid: {
                'label_prop': None,
                'description_prop': None,
                'alias_prop': None,
                'claims': []
            }   
        }
        
        label_item = ds_wb_source[ds_wb_source.ps_Label == 'label']
        if not label_item.empty:
            d_config[ds_qid]['label_prop'] = label_item.iloc[0].pq_Label

        alias_item = ds_wb_source[ds_wb_source.ps_Label == 'alias']
        if not alias_item.empty:
            d_config[ds_qid]['alias_prop'] = alias_item.iloc[0].pq_Label

        description_item = ds_wb_source[ds_wb_source.ps_Label == 'description']
        if not description_item.empty:
            d_config[ds_qid]['description_prop'] = description_item.iloc[0].pq_Label

        html_table = ds_wb_source[ds_wb_source.wdLabel == 'html table']
        if not html_table.empty:
            d_config[ds_qid]['interface_type'] = 'html table'
            d_config[ds_qid]['interface_url'] = html_table.iloc[0].ps_

        entity_classifier = ds_wb_source[ds_wb_source.wdLabel == 'entity classifier']
        if not entity_classifier.empty:
            d_config[ds_qid]['instance_of_qid'] = entity_classifier.iloc[0].ps_.split('/')[-1]
            d_config[ds_qid]['instance_of_label'] = entity_classifier.iloc[0].ps_Label

        property_map_items = ds_wb_source[
            (ds_wb_source.wdLabel == 'property from data source')
            &
            (~ds_wb_source.ps_Label.isin(['label','alias','description']))
        ]
        if not property_map_items.empty:
            for index, row in property_map_items.iterrows():
                d_config[ds_qid]['claims'].append({
                    'pid': row.ps_.split('/')[-1],
                    'label': row.ps_Label,
                    'source_prop': row.pq_Label
                })
                
        return d_config
    
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
    
    def item_collection_processor(self, dict_items):
        if not isinstance(dict_items, dict):
            return
        all_items = []
        for i, item_list in dict_items.items():
            all_items.extend(item_list)
        return all_items