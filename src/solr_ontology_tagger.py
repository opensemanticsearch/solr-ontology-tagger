#!/usr/bin/python
# -*- coding: utf-8 -*-

#
# Tag / enrich indexed documents with URIs, aliases & synonyms from new or changed SKOS thesaurus or RDF(S) ontology
#

# Tool to apply new thesauri with some dozens, some hundret or some thousand entries or new entries to existing index

# This way will take too much time for very big thesauri or ontologies with many entries
# since every subject will need one query and a change of all affected documents

# For very big dictionaries / ontologies with many entries config/use an ETL or data enrichment plugin for ontology annotation / tagging before / while indexing


import logging

import rdflib
from rdflib import Graph
from rdflib import RDFS
from rdflib import Namespace

import etl.export_solr

# define used ontologies / standards / properties
skos = Namespace('http://www.w3.org/2004/02/skos/core#')
owl = Namespace('http://www.w3.org/2002/07/owl#')

logging.basicConfig()


# append labels to synonyms config file

def append_labels_to_synonyms_configfile(rdf_labels, synonyms_configfile):

	labels = []
	for label in rdf_labels:
		labels.append(label[1])
			
	synonyms_configfile = open(synonyms_configfile, 'a')

 	# append all labels comma separated and with endline
	synonyms_configfile.write(','.join(labels).encode('UTF-8') + '\n')

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
		
		# rdflibs prefferedLabels returns RDF labels are returned as tuples with type and label, we need the label only
		label = label[1]
		
		# if not the first label of query, add an OR operator and delimiter in between
		if first:
			first = False
		else:
			query += " OR "

		# embed label in phrase by " and mask special/reserved char for Solr
		query += "\"" + etl.export_solr.solr_mask(label) + "\""

	return query


class OntologyTagger(Graph):

	# defaults
	verbose = False
	
	solr = 'localhost:8983/solr/core1/'
	source_facet = '_text_'
	target_facet = 'tag_ss'
	
	synonyms_embed_to_document = False
	synonyms_configfile = False
	
	#
	# get all labels, alternate labels and synonyms for the URI/subject
	#

	def get_labels(self, s):
	
		labels = self.preferredLabel(subject=s, labelProperties=(rdflib.term.URIRef(u'http://www.w3.org/2004/02/skos/core#prefLabel'), rdflib.term.URIRef(u'http://www.w3.org/2000/01/rdf-schema#label'), rdflib.term.URIRef(u'http://www.w3.org/2004/02/skos/core#altLabel'), rdflib.term.URIRef(u'http://www.w3.org/2004/02/skos/core#hiddenLabel')))
	
		return labels


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


			# normalized/best/preferred label  to facet for normalized label

			preferred_label = None

			preferred_label = self.preferredLabel(subject=s, lang=lang, labelProperties=(rdflib.term.URIRef(u'http://www.w3.org/2004/02/skos/core#prefLabel'), rdflib.term.URIRef(u'http://www.w3.org/2000/01/rdf-schema#label'), rdflib.term.URIRef(u'http://www.w3.org/2004/02/skos/core#altLabel')))

			# if no label in preferred language, try with english, if not preferred lang is english yet)
			if not preferred_label and not lang == 'en':
				
				preferred_label = self.preferredLabel(subject=s, lang='en', labelProperties=(rdflib.term.URIRef(u'http://www.w3.org/2004/02/skos/core#prefLabel'), rdflib.term.URIRef(u'http://www.w3.org/2000/01/rdf-schema#label'), rdflib.term.URIRef(u'http://www.w3.org/2004/02/skos/core#altLabel')))

			# if not there in own or english language, use first available label from other language
			if preferred_label:
				# since return is tuple with type and label take only the label
				preferred_label = preferred_label[0][1]
			else:
				preferred_label = labels[0][1]
						
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
			# Add alternate labels and synonyms
			#
			# - to document (word embedding) 
			# - and / or to synonym config (mapping)
			#

			if self.synonyms_configfile or self.synonyms_embed_to_document:

				if len(labels) > 1:

					if self.synonyms_embed_to_document:

						for label in labels:
		
							tagdata = add_value_to_facet(facet = target_facet + '_synonyms_ss', value = label[1], data=tagdata)
						
					if self.synonyms_configfile:
							
							append_labels_to_synonyms_configfile(labels, self.synonyms_configfile)

			# If Solr server for tagging set
			# which is not, if only export of synonyms without tagging of documents in index
			if self.solr:
				
				# build lucene query to search for at least one label of all labels
				query = labels_to_query(labels)
	
				# search only in source facet
				query = source_facet + ':(' + query + ')'
	
				# tag (add facets and values) documents matching this query with this URIs & labels
				connector = etl.export_solr.export_solr()
				connector.verbose = self.verbose
				count =  connector.update_by_query(query=query, data=tagdata)


	#
	# For all found entities (IDs / synonyms / aliases) of the ontology:
	# - write synonyms config
	# - tag the matching documents in index
	#
	
	def apply(self, target_facet, source_facet="_text_", lang='en', narrower=True):
	
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
	parser.add_option("-u", "--solr-uri", dest="solr", default=None, help="URI of Solr server and index, where to tag documents")
	parser.add_option("-s", "--synonyms_configfile", dest="synonyms_configfile", default=None, help="Solr synonyms config file to append synonyms")
	parser.add_option("-a", "--sourcefacet", dest="source_facet", default="_text_", help="Facet / field to analyze")
	parser.add_option("-f", "--facet", dest="facet", default="tag_ss", help="Facet / field to tag to")
	parser.add_option("-l", "--lang", dest="lang", default="en", help="Language for normalized / preferred label")
	parser.add_option("-n", "--narrower", dest="narrower", action="store_true", default=True, help="Tag with narrower concepts, too")
	parser.add_option("-v", "--verbose", dest="verbose", action="store_true", default=None, help="Print debug messages")

	(options, args) = parser.parse_args()

	if len(args) < 1:
		parser.error("No filename given")

	ontology = args[0]

	ontology_tagger = OntologyTagger()

	
	# set options from command line

	if options.solr:
		ontology_tagger.solr = options.solr

	if options.synonyms_configfile:
		ontology_tagger.synonyms_configfile = options.synonyms_configfile
	
	if options.verbose == False or options.verbose==True:
		ontology_tagger.verbose=options.verbose


	#load graph from RDF file
	ontology_tagger.parse(ontology)

	# tag the documents on Solr server with all entities in the ontology	
	ontology_tagger.apply(target_facet=options.facet, source_facet=options.source_facet, lang=options.lang, narrower=options.narrower)
