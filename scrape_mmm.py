#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Best-effort scraper for mmm documents from http://knesset.gov.il/mmm/heb/MMM_Results.asp
# retrieves all files to to local cache, convertes them to text and crudely tries to
# identify the mks referenced as the person who requested the document
#
# deps: runs on linux, pdftotext must be installed, and the python packages bs4
#       and fuzzywuzzy (https://github.com/seatgeek/fuzzywuzzy)
#
# dumps out 3 files:
# - the document meta data scraped from the webpage goes in LINKSFILE
# - the metadata for documents which matched with a score > SCORE_THRESHOLD
#   goes in MATCHESFILE
# - a count of docments per mk is dumped in csv form into CSVFILE
#
# 25/6/12: there are 3086 links, 127 of which appear more then once,
# after distincting, we have 2956 documents, out of which 517 have a
# match with score > SCORE_THRESHOLD.
#
# the documents go back to 2000, but hte mks.json file only holds
# mks from the 18th knesset right now.
#
# BSD.
# Copyright (c) 2012, y-p (repos: http://github.com/y-p)
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
# * Redistributions of source code must retain the above copyright
# notice, this list of conditions and the following disclaimer.
# * Redistributions in binary form must reproduce the above copyright
# notice, this list of conditions and the following disclaimer in the
# documentation and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
# DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
# ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.


import urllib,os,json,sys,datetime,codecs
from bs4 import BeautifulSoup as bs4
import re
import multiprocessing
from collections import Counter
from fuzzywuzzy import fuzz
import logging
import subprocess


REG_TOKEN = re.compile("[\w\d]+")
MKSJSONFILE="mks.json"
DATADIR='./data/'
SCORE_THRESHOLD=90
#START_DATE=datetime.date(2009,2,24)
LINKSFILE="mmm.json"
MATCHESFILE="matches.json"
CSVFILE="counts.csv"

logging.basicConfig(level=logging.INFO,
	format='%(asctime)s %(name)-4s %(levelname)-8s %(message)s',
	datefmt='%m-%d %H:%M:%S',
#		    filename='/tmp/myapp.log',
#		    filemode='w'
)

logger=logging.getLogger("mmm-scrape")

# MKSJSONFILE should contain mappings from names
# to ids (the included file maps names to mk  ids as they appear on oknesset.org)
#
# can't hide the global inside a class
# because multiprocessing.Map chokes when given a method
# instead of a function

with open(MKSJSONFILE,"rb") as jsonfile:
	mks=json.load(jsonfile)


def	score(d):
	""" fuzzy match between all the records in mks """
	""" and all the lines present inside d['candidates'] """
	results=[]
	for heading in d['candidates']:
		results.append( [{'url' : d['url'],
						  'score' :0 if len(heading)<6 else fuzz.partial_ratio(mkname,heading),
						  'mkname': mkname,
						  'id':id,
						  'heading' : heading}	for (mkname,id) in mks])

	return results

def scrape(url):
	""" get the page, extract the data, return a list of dicts"""
	""" with keys 'title','url','date' and 'author'"""

	logger.info("Retrieving  %s" % url)
	h=urllib.urlopen(url).read()
	logger.info("Parsing HTML")
	s=bs4(h)
	logger.info("Extracting document metadata")
	d_links=filter(lambda x: x['href'].find("/pdf/") >=0,s.find_all("a","Link3"))
	d_links= ['http://knesset.gov.il'+x['href'] for x in d_links]
	d_titles=[x.text for x in s.find_all("td","Title2")]
	d_body=s.find_all("td","Text13")
	d_date=[x.text for x in [a.find_all("font")[0] for a in d_body]]
	d_author=[x.text for x in [a.find_all("font")[1] for a in d_body]]

	if not len(d_links)==len(d_titles)==len(d_date)==len(d_author):
		print "Had trouble processing the data from the page. dying"
		sys.exit(1)

	data=zip(d_titles,d_links,d_date,d_author)
	data=[{'title':d[0],'url':d[1],'date':d[2],'author':d[3]} for d in data]

	return data

def main():
	data=scrape("http://knesset.gov.il/mmm/heb/MMM_Results.asp")
	with codecs.open(LINKSFILE,"wb",encoding='utf-8') as f:
		json.dump(data,f)
		logger.info("saved data on documents as json in %s",LINKSFILE)

	# load back the data
	with codecs.open(LINKSFILE,encoding='utf-8') as f:
			data=json.load(f)

	# convert to dict keyed by URL
	datadict={d['url']:d for d in data}

	keys=[x['url'] for x in data]+datadict.keys()
	cnt=Counter(keys)
	dupes=filter(lambda x: x[1]>2,cnt.iteritems())

	logging.info("%d documents have dupes, for a total of %d duplicates" % (len(dupes),sum ([x[1]-2 for x in dupes])))

	# retrieve each missing file from the net if needed
	# convert each file to text
	# filter the lines to find thos with the magic pattern
	# save all such lines in d['candidates']
	for (k,d) in datadict.iteritems():
		basename=d['url'].split("/")[-1]
		fullpath=os.path.join(DATADIR,basename)
		if not os.path.exists(fullpath):
			logger.info("Retrieving %s into %s" % (d['url'],DATADIR))
			with open(fullpath,"wb") as f:
				f.write(urllib.urlopen(d['url']).read())
				pass

		cmd = "pdftotext %s -" % fullpath
		logger.info("converting %s to text" % fullpath)

		p = subprocess.Popen(cmd.strip().split(' '), stdout=subprocess.PIPE)
		(contents, errf) = p.communicate()
		lines=[x.decode('utf-8') for x in contents.split("\n")]
		pat=filter(lambda x: re.search(u"מסמך\s+זה",x),lines)
		datadict[k]['candidates']=pat

	logger.info("Scoring candidates..." )
	# use all cores to do the scoring, this is O(no. of mks * number of lines with magic pattern)
	# can take a little while.
	p=multiprocessing.Pool(multiprocessing.cpu_count())
	scores=p.map(score,datadict.values(),len(data)/4)
	scores=reduce(lambda x,y:x+y,scores)

	# there's a subtle point here, that if multiple lines in a file match the magic pattern
	# and they both match with a high enough score, then the last line is the one
	# that will be saved as the best match, regardless.
	# in practice, not a problem
	i=0
	for v in scores:
		best=[ e for e in v if e['score'] == max([z['score'] for z in v ])][0]
		if (best['score'] > SCORE_THRESHOLD):
			datadict[best['url']].update(best)
			i+=1

	logger.info("Located %d matches with score > %d out of %d unique documents"%(i,SCORE_THRESHOLD,len(datadict)))

	# dump the matches with high enough scores to matches.json
 	with codecs.open(MATCHESFILE,"wb",encoding='utf-8') as f:
		keepers=filter(lambda x: x.get('score',0)>SCORE_THRESHOLD,datadict.values())
		json.dump(keepers,f)
		logger.info("saved matches as json in %s",MATCHESFILE)

	# load it back up
	with codecs.open(MATCHESFILE,"r",encoding='utf-8') as f:
		matches=json.load(f)

	# build a table which holds the number of documents associated with each name
	logger.info("Preparing rankings")
	cnt= list(Counter ([x['mkname'] for x in matches]).iteritems())
	cnt.sort(key=lambda x:x[1])

	# output an excel-compatible CSV of ranking
	with codecs.open(CSVFILE,"wb",encoding='utf-16-le') as f:
		for (name,count) in cnt:

			f.write(u"%s\t%d\n" % (name,count))

	logger.info("saved rankings in %s",CSVFILE)

	logger.info("Cheers.")

if __name__ == "__main__":
	main()
