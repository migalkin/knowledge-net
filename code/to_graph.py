import json
import requests
import urllib
import codecs
from pathlib import Path
from tqdm import tqdm
import io

class QueryEngine:
    """
        Queries wikidata endpoint
    """
    def __init__(self, url, login=None, password=None):
        self.endpoint = url
        self.login = login
        self.password = password

    def query(self, query_string, format="application/sparql-results+json"):
        params = urllib.parse.urlencode({"query": query_string})
        if self.login:
            r = requests.get(self.endpoint, params=params, headers={'Accept': format,
                                                                    'User-Agent':'kg_net'},
                             auth=(self.login, self.password))
        else:
            r = requests.get(self.endpoint, params=params, headers={'Accept': format,
                                                                    'User-Agent':'kg_net'})
        try:
            if "json" in format:
                results = json.loads(r.text)
            else:
                results = r.text
            return results
        except Exception as e:
            raise Exception("Smth is wrong with the endpoint", str(e), " , ", r.status_code)

    def get_truthy_neighbourhood(self, entity, format="application/n-triples"):
        query_outgoing = f"""
            CONSTRUCT {{ ?s ?p ?o }}
            WHERE {{
                VALUES ?s {{ wd:{entity} }}
                {{ ?s ?p ?o .
                   [] wikibase:directClaim ?p }}
                }}
        """
        query_incoming = f"""
        CONSTRUCT {{ ?o ?p ?s }}
            WHERE {{
                VALUES ?s {{ wd:{entity} }}
                {{ ?o ?p ?s .
                   [] wikibase:directClaim ?p }}
                }}
        """

        results = self.query(query_outgoing, format=format)
        return results


    def get_statement_neighbourhood(self, entity, format="application/n-triples"):
        """later alligator"""
        raise NotImplementedError


    def extract_wikidata_subgraph(self, entities, mode="truthy"):
        assert mode == "truthy" or mode == "statement" , "allowed modes are truthy or statement"
        with io.open("wikidata_dump.nt", "w") as target:
            for e in tqdm(entities):
                try:
                    if mode == "truthy":
                        e_descr = self.get_truthy_neighbourhood(e)
                    else:
                        e_descr = self.get_statement_neighbourhood(e)
                    target.write(e_descr)
                except Exception:
                    print(f"Error processing entity {e}")
                    continue
        print("The dump is saved in wikidata_dump.nt")




class Parser:
    """
        Process the training file
    """
    def __init__(self, train_file):
        """
        :param train_file: path to the train.json in JSON lines format, i.e., each line is a separate json object
        """
        self.source = []
        with codecs.open(train_file, "r", encoding="UTF-8") as source:
            for i, line in enumerate(source):
                self.source.append(json.loads(line))
        print(f"Loaded {i+1} samples")

    def collect_entities(self, save=False):
        """
        :return: a list of all unique Wikidata entities
        """
        entities = set()
        total_facts = 0
        for sample in self.source:
            for passage in sample['passages']:
                for fact in passage['facts']:
                    entities.add(fact['subjectUri'])
                    entities.add(fact['objectUri'])
                total_facts += len(passage['facts'])
        entities.remove('')  # remove empty string
        print(f"Extracted {len(entities)} wikidata entities from {total_facts} facts")
        entities = [e.split("/")[-1] for e in entities]
        if save:
            with open("entities.txt", "w") as target:
                target.write("\n".join(sorted(entities)))
                print("Dump is stored in entities.txt")
        return sorted(entities)

    def collect_relations(self, save=False):
        """
        :return: a list of unique relations in the dataset
        """
        relations = []
        total_relations = 0
        for sample in self.source:
            for passage in sample['passages']:
                relations.extend(passage['exhaustivelyAnnotatedProperties'])
        relations = list({v['propertyId']:v for v in relations}.values())
        print(f"Extracted {len(relations)} relations")
        if save:
            with open("relations.json", "w") as target:
                json.dump(relations, target)
                print("Dump is stored in relations.json")
        return list(relations)



if __name__ == "__main__":
    train_file = Path("../train.json")
    parser = Parser(train_file)
    engine = QueryEngine("https://query.wikidata.org/sparql")
    entities = parser.collect_entities()
    relations = parser.collect_relations()
    engine.extract_wikidata_subgraph(entities)



