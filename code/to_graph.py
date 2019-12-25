import json
import requests
import urllib
import codecs
from pathlib import Path
from tqdm import tqdm
import io
import spacy
import neuralcoref
import csv
import re
from spacy.matcher import PhraseMatcher

nlp = spacy.load('en_core_web_lg')
neuralcoref.add_to_pipe(nlp)
pattern = re.compile("(?<=\().*(?=\))")

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

    def save_abstracts(self, save=False):
        """
        :return: a dictionary of id:abstract of a data point in natural language
        """
        Path('./raw_texts').mkdir(parents=True, exist_ok=True)
        p = Path('./raw_texts')
        abstracts = {}
        for sample in self.source:
            id = sample['documentId']
            text = sample['documentText'].replace("“","\"").replace("”","\"").replace("’","\'")
            abstracts[id] = text
            if save:
                with open(p / f"{str(id)}.txt", "w") as output:
                    output.write(text)

        if save:
            print("Saved in the raw_texts directory")
        return abstracts

    def add_coref(self, save=False):
        """
        Adding coref to passages without explicit subject URI
        :return: each sample might have an additional field 'coref', new file is saved under train_wcoref.json
        """
        print("Adding coref to passages without explicit subjectURI")
        resolved = 0
        for sample in tqdm(self.source):
            text = sample['documentText']
            doc = nlp(text)
            #print(doc._.coref_clusters)
            for passage in sample['passages']:
                for fact in passage['facts']:
                    if fact['subjectUri'] == "":
                        subject = fact['subjectText']
                        # now lets find the subject text in the doc and find if it has any coref clusters
                        sub_span = doc.char_span(fact['subjectStart'], fact['subjectEnd'])  # can be evaluated to None in some rare cases
                        if sub_span:
                            sub_coref = sub_span._.coref_cluster
                            if sub_coref is not None:
                                fact['coref'] = sub_coref.main.text
                                resolved += 1

        print(f"Resolved {resolved} facts")
        if save:
            json.dump(self.source, open('train_wcoref.json',"w"), ensure_ascii=False, indent=4)
            print("Saved under train_wcoref.json")



def save_corefs(abstracts):
    Path('./coref').mkdir(parents=True, exist_ok=True)
    p = Path("./coref")
    for id in list(abstracts.keys()):
        with open(p / f"{id}.txt", "w") as output:
            doc = nlp(abstracts[id])
            output.write(str(doc._.coref_clusters))

    print("Done")


def find_main_coref(clusters, text):
    for i in clusters:
        mentions = [k.text for k in i.mentions]
        if text in mentions:
            return i.main.text
    return None

def map_ie_coref(abstracts):
    Path('./openie_coref').mkdir(parents=True, exist_ok=True)
    out_path = Path("./openie_coref")
    openie_path = Path("./processed_data")
    available_files = [str(i) for i in openie_path.glob("*.txt")]

    for id in tqdm(list(abstracts.keys())):
        if f"{openie_path}/{id}.txt" not in available_files:
            continue

        text = abstracts[id].replace("\n\n", " ").replace("\n", " ")  # get rid of \n in the beginning
        doc = nlp(text)
        coref_clusters = doc._.coref_clusters

        # load openie output
        with open(openie_path / f"{id}.txt", newline="") as oi, open(out_path / f"{id}.txt", "w", newline="") as res:
            triples = csv.reader(oi, delimiter="\t", quoting=csv.QUOTE_NONE)
            csv_w = csv.writer(res, delimiter="\t")
            """
            columns in each file
            0(confidence) 1(context) 2(subject) 3(predicate) 4(object) 5(sentence) 
            """
            for t in triples:
                # prune non-triples
                if t[2] == "" or t[4] == "":
                    continue
                s = pattern.search(t[2]).group(0).split(",List")[0]
                o = pattern.search(t[4]).group(0).split(",List")[0]

                # get annotated sentence in spacy doc
                sent = t[5].replace("\n\n", " ").strip()
                spacy_p = [nlp.make_doc(sent)]
                matcher = PhraseMatcher(nlp.vocab, attr="LOWER")
                matcher.add("SomeList", None, *spacy_p)
                matches = matcher(doc)  # should be only one match (sentence)
                _, ms, me = matches[0]
                sent_span = doc[ms:me]

                # find s / o pattern as a span in the spacy doc
                match_s, match_o = [nlp.make_doc(s)], [nlp.make_doc(o)]
                matcher_entities_s, matcher_entities_o = PhraseMatcher(nlp.vocab, attr="LOWER"), PhraseMatcher(nlp.vocab, attr="LOWER")
                matcher_entities_s.add("Entities_s", None, *match_s)
                matcher_entities_o.add("Entities_o", None, *match_o)
                matches_s = matcher_entities_s(nlp.make_doc(sent_span.text))
                matches_o = matcher_entities_o(nlp.make_doc(sent_span.text))
                if len(matches_s) > 0:
                    _, sub_start, sub_end = matches_s[0]  # if not None - should be always 1
                    final_s = doc[ms+sub_start:ms+sub_end]
                    coref_s = final_s._.coref_cluster  # link to the main doc and find a coref of that span
                    if coref_s is not None:
                        main_s = coref_s.main.text
                        t[2] = t[2].replace(s, main_s)

                if len(matches_o) > 0:
                    _, ob_start, ob_end = matches_o[0]  # if not None - should be always 1
                    final_o = doc[ms+ob_start:ms+ob_end]
                    coref_o = final_o._.coref_cluster  # link to the main doc and find a coref of that span
                    if coref_o is not None:
                        main_o = coref_o.main.text
                        t[4] = t[4].replace(o, main_o)

                csv_w.writerow(t)




        # process each line and replace possible subject / object with the main coref cluster label (if exists)

def read_mappings():
    mappings = json.load(open("mappings.json", "r"))
    properties = [item for sublist in list(mappings.values()) for item in sublist]
    return properties


def filter_dump():
    maps = read_mappings()
    with open("wikidata_dump.nt", "r") as source, open("wikidata_filtered.nt", "w") as output:
        for line in source:
            for p in maps:
                if f"<{p}>" in line:
                    output.write(line)

    print("Done")


def resolve_coref(text):
    doc = nlp(text)
    print(doc._.has_coref)
    print(doc._.coref_clusters)




if __name__ == "__main__":
    train_file = Path("../train.json")
    parser = Parser(train_file)
    parser.add_coref(save=True)
    # engine = QueryEngine("https://query.wikidata.org/sparql")
    # entities = parser.collect_entities()
    # engine.extract_wikidata_subgraph(entities)



