#!/usr/bin/env python
#This is a comment
import sys, urllib, time, json, os, logging, subprocess, thread
import uuid, OpenSSL
from getopt import getopt
from bottle import route, run, request, abort
from pymongo import Connection
import daemon, asyncore
import urllib, re, socket, pymongo, bson
from bs4 import BeautifulSoup
from urlparse import urlparse, urljoin

max_number_sockets = 1

# CORE OBJECTS
connection = Connection('33.33.33.10', 27017)
connection_q = connection.mp_crawler
connection_w = connection_q
db_q_handle = connection_q.mp_q
#db_q_handle.authenticate(env['dbuser'], env['dbpass'])
db_w_handle = connection_w.mp_w

# DATA RETRIEVAL THROTTLING
max_levels_deep = 1
max_child_urls_per_parent = 10

# LOGGING CONFIG
logging.basicConfig(level=logging.DEBUG)

class httpClient(asyncore.dispatcher):

  def __init__(self, job_id):

    asyncore.dispatcher.__init__(self)
    self.job_id = job_id
    q_doc = db_q_handle.find_one({'job_id':self.job_id})
    self.next_url = q_doc['link']
    db_q_handle.remove({'_id': bson.objectid.ObjectId(q_doc['_id'])})
    u = urlparse(self.next_url)
    self.buffer = 'GET %s HTTP/1.1\r\nHost: %s\r\n\r\n' % (u.path, u.netloc)
    print 'Send buffer = %s' % self.buffer
    self.levels_deep = 0
    self.parent_url = ''
    self.url = ''
    
    self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
    u = urlparse(self.next_url)
    print "Connecting socket to %s" %u.netloc
    self.connect((u.netloc, 80))
  
  def handle_connect(self):
    pass

  def handle_close(self):
    print "entering handle_close method"
    if self.next_url:
      print "self.next_url is set, connecting to new host"
      u = urlparse(self.next_url)
      print "Connecting socket to %s" %u.netloc
      self.connect((u.netloc, 80))
    self.close()

  def handle_read(self):
    time.sleep(3)
    self.html = self.recv(819222)
    self.url = self.next_url
    self.next_url = ''
    self.soup = BeautifulSoup(self.html)
    crawl_links = getLinks(self.url, self.soup, self.html)
    img_links = getImgLinks(self.url, self.html)
    print 'length of crawl links = %d' % len(crawl_links)
    print 'length of img links = %d' % len(img_links)
    sys.exit(0) 
    if self.levels_deep < max_levels_deep:
      for found_link in crawl_links[:max_child_urls_per_parent]:
        if urlparse(found_link).netloc:
          pass
        else:
          found_link = urljoin(urlparse(self.url).netloc, urlparse(found_link).path)
        q_data = {}
        q_data['job_id'] = self.job_id
        q_data['link'] = found_link 
        print "New Crawl Links Found: %s" % found_link
        q_data['levels_deep'] = self.levels_deep+1
        q_data['parent_url'] = self.url
        db_q_handle.insert(q_data)
        print "Job ID: %s From URL: %s Levels Deep: %d Crawl Link Found: %s" % (self.job_id, self.url, self.levels_deep, found_link) 
    w_data = {}
    w_data['job_id'] = self.job_id
    w_data['link'] = self.url
    w_data['levels_deep'] = self.levels_deep
    w_data['parent_url'] = self.parent_url
    w_data['img_links'] = img_links
    {'job_id': self.job_id, 'levels_deep': self.levels_deep, 'link': self.url, 'parent_url': self.parent_url, 'img_links': img_links}
    db_w_handle.insert(w_data)
    try:
      q_doc = db_q_handle.find_one({'job_id':self.job_id})
      self.next_url = q_doc['link']
      db_q_handle.remove({'_id': bson.objectid.ObjectId(q_doc['_id'])})
      u = urlparse(self.next_url)
      self.buffer = 'GET %s HTTP/1.0\r\n\r\n' % u.path
      print 'Send buffer = %s' % self.buffer
    except:
      self.next_url = ''

  def handle_write(self):
    
    print "Entering handle_write method"
    print "sending %s" % self.buffer
    sent = self.send(self.buffer)
    self.buffer = ''

  def writable(self):
    print "entering writable method"
    return (len(self.buffer) > 0)

def newJobThread(job_id):
 # daemon.createDaemon() 
  for i in range(max_number_sockets):
    print "launching new socket"
    httpClient(job_id)
  asyncore.loop()

def getLinks(url, soup, html):
  links = []
  print "Running get links"
  for link in soup.findAll('a'):
    print link
    if link.get('href')[:6] == 'http://':
      insert_link = link.get('href')
    else:
      insert_link = urljoin(url, link.get('href'))
    # Need to scan link if it starts with http: append it if ot do a urlparse.urljoin with the domain"
    links.append(insert_link)
  return links

def getImgLinks(url, html):
  img_links = []
  imgUrls = re.findall('img .*?src="(.*?)"', html)
  for link in imgUrls:
    if urlparse(link).netloc:
      pass
    else:
      link = urljoin(urlparse(url).netloc, urlparse(link).path)
    img_links.append(link)
  return img_links


@route('/result/<job_id>', method='GET')
def getJobResults(job_id):
  logging.info('New GET results request received for job %s' % job_id)
  response = {}
  response['img_urls'] = []
  urls_crawled = db_q_handle.find({'job_id':job_id, 'keep_track_queue': {'$exists': 0}, 'keep_track_crawl': {'$exists':0}})
  #try:
  for url in urls_crawled:
    for image_url in url['img_links']:
      response['img_urls'].append(image_url)
  #except:
  #response['img_urls'] = []
  return json.dumps(response)

# REST API - GET STATUS OF JOB
# returns a json document containing the number of urls
# crawled and waiting to be crawled for a certain job_id.
# Note that the number of urls waiting to be crawled will
# change quickly up and down as new child URLs are added 
# and then processed

@route('/status/<job_id>', method='GET')
def getJobStatus(job_id):
  logging.info('New GET status request received for job %s' % job_id)
  response = {}
  crawl_count = db_q_handle.find_one({'job_id':job_id, 'keep_track_crawl': 1, 'crawl_count': {'$exists': 1}})
  response['crawl_count'] = crawl_count['crawl_count']
  queue_count = db_q_handle.find_one({'job_id':job_id, 'keep_track_queue': 1, 'queue_count': {'$exists': 1}})
  response['queue_count'] = queue_count['queue_count']
  return json.dumps(response) 

# REST API - POST NEW JOB
# requires a json document to be posted containing a list of
# URL's to be crawled. Assigns a unique job ID and initiates
# a new job thread, then returns the job ID

@route('/', method='POST') 
def postNewJob():
  received_data = request.body.readline()
# ERROR CHECKING ON RECEIVED REQUEST
  if not received_data:
    abort(400, 'No data received')
  try:
    processed_data = json.loads(received_data)
    urls = processed_data['urls']
  except:
    abort(400, "Request body requires a 'urls' key and value list of urls")
  try:
    for url in processed_data['urls']:
      u = urlparse(url)
      if u.scheme != 'http':
        abort(400, "Only HTTP protocol supported.")
      if not u.netloc:
        abort(400, "Bad URL value list netloc")
  except:
    abort(400, "Bad URL value list: can't parse")

  urls = processed_data['urls']
  # Assign new unique Job ID
  job_id = str(uuid.UUID(bytes = OpenSSL.rand.bytes(16)))[:7]
  while job_id in db_w_handle.find({'job_id':job_id}, {'job_id':1, '_id':0}):
    job_id = str(uuid.UUID(bytes = OpenSSL.rand.bytes(16)))[:7]
 
  # SEED URLS INTO DATABASE
  for url in urls:
    u = urlparse(url)
    url_in = str(u.netloc+u.path)
   # q_data = {'job_id': job_id}
    q_data = {}
    q_data["job_id"] = job_id
    q_data["link"] = url
   # q_data = {'job_id': job_id, 'url':url_in, 'levels_deep':  0}
   # q_data = {'job_id': job_id, 'url':url_in, 'levels_deep':  0, 'parent_url': ''}
    print q_data
    db_q_handle.insert(q_data)

  newJobThread(job_id)

  # Close HTTP session, return Job ID, give thread enough time to kickff daemon mode
  time.sleep(20)
  return 'Your job ID is %s' % str(job_id)

if __name__ == "__main__":
  logging.info('Attempting to start webserver listening on 8080...')
  run(host='33.33.33.10', port=8080)
