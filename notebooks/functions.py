import requests
import pandas as pd

def sparql_query(endpoint: str, query: str, output: str = 'raw'):
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

    wd_countries = pd.DataFrame(r.json()['results']['bindings'])

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
        else:
            return data_records


def kb_props():
    prop_query = """
    PREFIX wdt: <https://edji-knows.wikibase.cloud/prop/direct/>

    SELECT ?property ?propertyLabel ?item_of_prop ?item_of_propLabel ?id_len WHERE {
    ?property a wikibase:Property .
    OPTIONAL {
        ?property wdt:P12 ?item_of_prop .
        ?property wdt:P13 ?id_len .
    }
    SERVICE wikibase:label { bd:serviceParam wikibase:language "en" . }
    }
    """
    props = sparql_query(
        endpoint="https://edji-knows.wikibase.cloud/query/sparql",
        query=prop_query,
        output="raw"
    )

    prop_records = []
    for result in props['results']['bindings']:
        prop_records.append({k:v['value'] for k,v in result.items()})

    df_props = pd.DataFrame(prop_records)
    df_props['prop_qid'] = df_props.property.apply(lambda x: x.split('/')[-1])
    df_props['instance_of_qid'] = df_props.item_of_prop.apply(lambda x: x.split('/')[-1] if isinstance(x, str) else None)
    item_of_props = df_props[df_props.item_of_prop.notnull()].copy()
    item_of_props = item_of_props.convert_dtypes()

    prop_lookup = df_props[["propertyLabel","prop_qid"]].set_index("propertyLabel").to_dict()["prop_qid"]

    return df_props, prop_lookup

def value_key_dict(results):
    vk_mapping = {}

    label_key = next((i for i in results[0].keys() if i.endswith("Label")), None)
    id_key = label_key.replace('Label', '')
 
    for x in results:
        vk_mapping[x[label_key]['value']] = x[id_key]['value'].split('/')[-1]

    return vk_mapping

def kb_datasources():
    datasource_query = """
    PREFIX wd: <https://edji-knows.wikibase.cloud/entity/>
    PREFIX wdt: <https://edji-knows.wikibase.cloud/prop/direct/>

    SELECT ?ds ?dsLabel WHERE {
    ?ds wdt:P1 wd:Q4 .
    SERVICE wikibase:label { bd:serviceParam wikibase:language "en" . }
    }
    """

    datasources = sparql_query(
        endpoint="https://edji-knows.wikibase.cloud/query/sparql",
        query=datasource_query
    )

    return value_key_dict(datasources['results']['bindings'])

def valid_classes():
    class_query = """
    PREFIX wdt: <https://edji-knows.wikibase.cloud/prop/direct/>

    SELECT ?class ?classLabel WHERE {
    ?class wdt:P2 [].
    SERVICE wikibase:label { bd:serviceParam wikibase:language "en" . }
    }
    """

    classes = sparql_query(
        endpoint="https://edji-knows.wikibase.cloud/query/sparql",
        query=class_query
    )

    return value_key_dict(classes['results']['bindings'])
