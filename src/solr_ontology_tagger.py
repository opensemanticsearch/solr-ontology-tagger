#!/usr/bin/python3
# -*- coding: utf-8 -*-

#
# Tag / enrich indexed documents with URIs, aliases & synonyms from SKOS thesaurus or RDF(S) ontology
#

# Apply new thesauri with some dozens, some hundred or some thousand entries or new entries to existing index will take too much time for very big thesauri or ontologies with many entries
# since every subject will need one query and a change of all affected documents

# For very big dictionaries / ontologies with many entries and tagging of new documents config/use the Open Semantic ETL data enrichment plugin enhance_ner_dictionary for ontology annotation / tagging before / while indexing


import logging
import requests
import json
import rdflib
from rdflib import Graph
from rdflib import RDFS
from rdflib import Namespace
import opensemanticetl.export_solr

# define used ontologies / standards / properties
skos = Namespace('http://www.w3.org/2004/02/skos/core#')
owl = Namespace('http://www.w3.org/2002/07/owl#')

logging.basicConfig()


# append labels to synonyms config file
def append_labels_to_synonyms_configfile(labels, synonyms_configfile):
			
	synonyms_configfile = open(synonyms_configfile, 'a', encoding="utf-8")

 	# append all labels comma separated and with endline
	synonyms_configfile.write(','.join(labels) + '\n')

	synonyms_configfile.close()


#
# add value to facet of data array, if not there yet
#

def add_value_to_facet(facet, value, data = None ):

	if not data:
		data = {}

	if facet in data:
		# if not list, convert to list
		if not isinstance(data[facet], list):
			data[facet] = [ data[facet] ]
		data[facet].append(value)
	else:
		data[facet] = value

	return data


#
# build Lucene query from labels
#

def labels_to_query(labels):

	query = ''
	first = True
	
	for label in labels:
				
		# if not the first label of query, add an OR operator and delimiter in between
		if first:
			first = False
		else:
			query += " OR "

		# embed label in phrase by " and mask special/reserved char for Solr
		query += "\"" + opensemanticetl.export_solr.solr_mask(label) + "\""

	return query



class OntologyTagger(Graph):

	# defaults
	verbose = False
	
	solr = 'localhost:8983/solr/'
	solr_core = 'opensemanticsearch'
	solr_entities = None
	solr_core_entities = None
	source_facet = '_text_'
	target_facet = 'tag_ss'
	
	tag = False

	labelProperties = (rdflib.term.URIRef(u'http://www.w3.org/2004/02/skos/core#prefLabel'), rdflib.term.URIRef(u'http://www.w3.org/2000/01/rdf-schema#label'), rdflib.term.URIRef(u'http://www.w3.org/2004/02/skos/core#altLabel'), rdflib.term.URIRef(u'http://www.w3.org/2004/02/skos/core#hiddenLabel'))
	
	synonyms_embed_to_document = False
	synonyms_configfile = False
	synonyms_ressourceid = False
	wordlist_configfile = False
	labels_configfile = False

	appended_words = []
	
	
	#
	# append labels to Solr REST managed ressource
	#
	def append_labels_to_synonyms_ressource(self, labels):

		url = self.solr + self.solr_core + '/schema/analysis/synonyms/' + self.synonyms_ressourceid
		headers = {'content-type' : 'application/json'}
				
		label = str(labels[0])

		synonyms=[]
		for synonym in labels[1:]:
			synonyms.append(str(synonym))

		data = { label: synonyms }
		
		r = requests.post(url=url, data=json.dumps(data), headers=headers)


	#
	# get all labels, alternate labels / synonyms for the URI/subject, if not there, use subject (=URI) as default
	#

	def get_labels(self, subject):

		labels = []
	
		# append RDFS.label

		# get all labels for this obj
		for label in self.objects(subject=subject, predicate=rdflib.RDFS.label):
			labels.append(label)

		#
		# append SKOS labels
		#
			
		# append SKOS prefLabel
		skos = rdflib.Namespace('http://www.w3.org/2004/02/skos/core#')
		for label in self.objects(subject=subject, predicate=skos['prefLabel']):
			labels.append(label)

		# append SKOS altLabels
		for label in self.objects(subject=subject, predicate=skos['altLabel']):
			labels.append(label)

		# append SKOS hiddenLabels
		for label in self.objects(subject=subject, predicate=skos['hiddenLabel']):
			labels.append(label)

		return labels



	# best/preferred label as title
	def get_preferred_label(self, subject, lang='en'):
		
			preferred_label = self.preferredLabel(subject=subject, lang=lang, labelProperties=self.labelProperties)

			# if no label in preferred language, try with english, if not preferred lang is english yet)
			if not preferred_label and not lang == 'en':
				
				preferred_label = self.preferredLabel(subject=subject, lang='en', labelProperties=self.labelProperties)

			# use label from some other language
			if not preferred_label:
				
				preferred_label = self.preferredLabel(subject=subject, labelProperties=self.labelProperties)

			# if no label, use URI
			if preferred_label:
				# since return is tuple with type and label take only the label
				preferred_label = preferred_label[0][1]
			else:
				preferred_label = subject

			return preferred_label

	

	#
	# tag the concept with URI/subject s to target_facet of all documents including at least of the labels
	#

	def tag_documents_with_concept(self, s, target_facet='tag_ss', source_facet="_text_", lang='en', narrower=True):
			
		# get all Labels for this subject
		labels = self.get_labels(s)

		#
		# if any, add labels / synonyms to tagging facet/field
		#
		
		if len(labels):

			# values which the document will be tagged
			tagdata = {}
			
			# add URI of the entity, so we can filter/export URIs/entities, too
			tagdata[target_facet + '_uri_ss'] = s

			# normalized/best/preferred label to facet for normalized label
			preferred_label = self.get_preferred_label(subject=s, lang=lang)

			tagdata = add_value_to_facet(facet = target_facet + '_preferred_label_ss', value = preferred_label, data=tagdata)


			#
			# Linked concepts
			#
			
			# linked other concepts or same concepts in other ontologies or thesauri
			# by SKOS:exactMatch or OWL:sameAs

			for o in self.objects(s, skos['exactMatch']):
				labels.extend( self.get_labels(o) )

			for o in self.objects(s, owl['sameAs']):
				labels.extend( self.get_labels(o) )

	
			# Todo: deeper than first degree (recursive with stack to prevent loops)
			# Todo: concepts where this subject is the object of property SKOS:broader 
			if narrower:
				
				for o in self.objects(s, skos['narrower']):
					labels.extend( self.get_labels(o) )

				for o in self.objects(s, skos['narrowMatch']):
					labels.extend( self.get_labels(o) )


			#
			# Append labes to list for dictionary based named entity extraction
			#

			if self.labels_configfile:

				labels_file = open(self.labels_configfile, 'a', encoding="utf-8")

				for label in labels:
					label = str(label)
					labels_file.write(label + "\n")

				labels_file.close()

			
			#
			# Append single words of concept labels to wordlist for OCR word dictionary
			#

			if self.wordlist_configfile:

				wordlist_file = open(self.wordlist_configfile, 'a', encoding="UTF-8")

				for label in labels:
					label = str(label)
					words = label.split()
					for word in words:
						word = word.strip("(),")
						if word:
							if word not in self.appended_words:
								self.appended_words.append(word)
								self.appended_words.append(word.upper())
								wordlist_file.write(word + "\n")
								wordlist_file.write(word.upper() + "\n")
				wordlist_file.close()


			#
			# Add alternate labels and synonyms
			#
			# - to document (word embedding) 
			# - and / or to synonym config (mapping)
			#

			if self.synonyms_ressourceid or self.synonyms_configfile or self.synonyms_embed_to_document:

				if len(labels) > 1:

					if self.synonyms_embed_to_document:

						for label in labels:
		
							tagdata = add_value_to_facet(facet = target_facet + '_synonyms_ss', value = label, data=tagdata)
						
					if self.synonyms_configfile:
							
							append_labels_to_synonyms_configfile(labels, self.synonyms_configfile)

					if self.synonyms_ressourceid:
							self.append_labels_to_synonyms_ressource(labels)

			if self.solr or self.solr_entities:
				connector = opensemanticetl.export_solr.export_solr()
				connector.verbose = self.verbose

			# If Solr server for tagging set
			# which is not, if only export of synonyms without tagging of documents in index
			if self.tag:

				connector.solr = self.solr
				connector.core = self.solr_core
				
				# build lucene query to search for at least one label of all labels
				query = labels_to_query(labels)
	
				# search only in source facet
				query = source_facet + ':(' + query + ')'
	
				# tag (add facets and values) documents matching this query with this URIs & labels
				count =  connector.update_by_query(query=query, data=tagdata)

			# If Solr server / core for entities index for normalization or disambiguation
			if self.solr_entities:

				data = {
					'id': s,
					'preferred_label_s': preferred_label
				}
				
				# append RDFS.label

				data['label_ss'] = []
				# get all labels for this obj
				for label in self.objects(subject=s, predicate=rdflib.RDFS.label):
					data['label_ss'].append(label)
		
				#
				# append SKOS labels
				#
					
				# append SKOS prefLabel
				data['skos_prefLabel_ss'] = []

				for label in self.objects(subject=s, predicate=skos['prefLabel']):
					data['skos_prefLabel_ss'].append(label)
		
				# append SKOS altLabels
				data['skos_altLabel_ss'] = []

				for label in self.objects(subject=s, predicate=skos['altLabel']):
					data['skos_altLabel_ss'].append(label)
		
				# append SKOS hiddenLabels
				data['skos_hiddenLabel_ss'] = []
				for label in self.objects(subject=s, predicate=skos['hiddenLabel']):
					data['skos_hiddenLabel_ss'].append(label)
				
				connector = opensemanticetl.export_solr.export_solr()
				connector.verbose = self.verbose
				connector.solr = self.solr_entities
				connector.core = self.solr_core_entities
				connector.post(data=data)


	#
	# For all found entities (IDs / synonyms / aliases) of the ontology:
	# - write synonyms config
	# - tag the matching documents in index
	#
	
	def apply(self, target_facet="tag_ss", source_facet="_text_", lang='en', narrower=True):
	
		# since this is returing subjects more than one time ...
		#for s in g.subjects(predicate=None, object=None):
	
		# we use a SPARQL query with distinct to get subjects only once
		res = self.query(
	    """SELECT DISTINCT ?subject
	       WHERE {
	          ?subject ?predicate ?object .
	       }""")
	
		for row in res:
	
			# get subject of the concept from first column
			s = row[0]	
	
			# tag the documents containing a label of this concept with this subject/concept and its aliases
			self.tag_documents_with_concept(s, target_facet=target_facet, source_facet=source_facet, lang=lang, narrower=narrower)
	
	
#
# Read command line arguments and start tagging
#

# if running (not imported to use its functions), run main function
if __name__ == "__main__":

	#get filenames from command line args

	from optparse import OptionParser

	parser = OptionParser("solr-ontology-tagger ontology-filename")
	parser.add_option("-u", "--solr-uri", dest="solr", default=None, help="URI of Solr server")
	parser.add_option("-c", "--solr-core", dest="solr_core", default=None, help="Solr core name")
	parser.add_option("-s", "--synonyms_configfile", dest="synonyms_configfile", default=None, help="Solr synonyms config file to append synonyms")
	parser.add_option("-r", "--synonyms_ressource", dest="synonyms_ressource", default=None, help="Solr REST managed synonyms ressource to append synonyms")
	parser.add_option("-w", "--wordlist_configfile", dest="wordlist_configfile", default=None, help="OCR wordlist/dictionary config file to append words")
	parser.add_option("-a", "--sourcefacet", dest="source_facet", default="_text_", help="Facet / field to analyze")
	parser.add_option("-f", "--facet", dest="facet", default="tag_ss", help="Facet / field to tag to")
	parser.add_option("-l", "--lang", dest="lang", default="en", help="Language for normalized / preferred label")
	parser.add_option("-n", "--narrower", dest="narrower", action="store_true", default=True, help="Tag with narrower concepts, too")
	parser.add_option("-v", "--verbose", dest="verbose", action="store_true", default=None, help="Print debug messages")
	parser.add_option("-t", "--tag-documents", dest="tag", action="store_true", default=False, help="Tag documents")

	(options, args) = parser.parse_args()

	if len(args) < 1:
		parser.error("No filename given")

	ontology = args[0]

	ontology_tagger = OntologyTagger()

	
	# set options from command line

	if options.solr:
		ontology_tagger.solr = options.solr

	if options.solr_core:
		ontology_tagger.solr_core = options.solr_core

	if options.synonyms_configfile:
		ontology_tagger.synonyms_configfile = options.synonyms_configfile

	if options.synonyms_ressource:
		ontology_tagger.synonyms_ressourceid = options.synonyms_ressource

	if options.wordlist_configfile:
		ontology_tagger.wordlist_configfile = options.wordlist_configfile

	if options.tag:
		ontology_tagger.tag = True

	if options.verbose == False or options.verbose==True:
		ontology_tagger.verbose=options.verbose


	#load graph from RDF file
	ontology_tagger.parse(ontology)

	# tag the documents on Solr server with all entities in the ontology	
	ontology_tagger.apply(target_facet=options.facet, source_facet=options.source_facet, lang=options.lang, narrower=options.narrower)
