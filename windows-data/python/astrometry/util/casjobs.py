#! /usr/bin/env python
import sys

import httplib
httplib.HTTPConnection.debuglevel = 1 

import urllib
import urllib2
#from xml.parsers import expat
import xml.dom
from xml.dom import minidom
import pickle
import os
import os.path
import re
import time

from numpy import *

from astrometry.util.sqlcl import query as casquery
from astrometry.util.file import *

galex_cas = { 'base_url': 'http://galex.stsci.edu/casjobs/',
			  'submiturl': 'SubmitJob.aspx',
			  'actionurl': 'mydbcontent.aspx?tableName=%s&kind=tables',
			  'defaultdb': 'GALEXGR4Plus5',
			  'outputbaseurl': 'http://mastweb.stsci.edu/CasOutPut/FITS/',
			  }
sdss_cas = { 'base_url': 'http://casjobs.sdss.org/CasJobs/',
			 'submiturl': 'submitjobhelper.aspx',
			 'actionurl': 'mydbcontent.aspx?ObjName=%s&ObjType=TABLE&context=MyDB&type=normal',
			 'defaultdb': 'DR7',
			 'request_output_extra': { 'targetDDL':'TargDR7Long' },
			 'outputbaseurl': 'http://casjobs.sdss.org/CasJobsOutput2/FITS/',
			 }

# See also magic values in submit_query()...

cas_params = sdss_cas

def get_url(relurl):
	return cas_params['base_url'] + relurl

def login_url():
	return get_url('login.aspx')

def submit_url():
	return get_url(cas_params['submiturl'])

def mydb_url():
	return get_url('MyDB.aspx')

def mydb_index_url():
	return get_url('mydbindex.aspx')

def mydb_action_url():
	return get_url(cas_params['actionurl'])

def drop_url():
	return get_url('DropTable.aspx?tableName=%s')

def output_url():
	return get_url('Output.aspx')

def job_details_url():
	return get_url('jobdetails.aspx?id=%i')

def cancel_url():
	return get_url('cancelJob.aspx')

def query(sql):
	f = casquery(sql)
	header = f.readline().strip()
	if header.startswith('ERROR'):
		raise RuntimeError('SQL error: ' + f.read())
	cols = header.split(',')
	results = []

	for line in f:
		words = line.strip().split(',')
		row = []
		for w in words:
			try:
				ival = int(w)
				row.append(ival)
				continue
			except ValueError:
				pass
			try:
				fval = float(w)
				row.append(fval)
				continue
			except ValueError:
				pass
			row.append(w)
		results.append(row)
	return (cols, results)



def submit_query(sql, table='', taskname='', dbcontext=None):
	if dbcontext is None:
		dbcontext = cas_params['defaultdb']
	data = urllib.urlencode({
		'targest': dbcontext,
		'sql': sql,
		'queue': 500,
		'syntax': 'false',
		'table': table,
		'taskname': taskname,
		})
	f = urllib2.urlopen(submit_url(), data)
	doc = f.read()

	redirurl = f.geturl()
	# older CasJobs version redirects to the job details page: just pull the jobid
	# out of the redirected URL.
	print 'Redirected to URL', redirurl
	pat = re.escape(job_details_url().replace('%i','')) +  '([0-9]*)'
	#print 'pattern:', pat
	m = re.match(pat, redirurl)
	if m is not None:
		jobid = int(m.group(1))
		print 'jobid:', jobid
		return jobid
	#write_file(doc, 'sub.out')
	
	xmldoc = minidom.parseString(doc)
	jobids = xmldoc.getElementsByTagName('jobid')
	if len(jobids) == 0:
		print 'No <jobid> tag found:'
		print doc
		return None
	if len(jobids) > 1:
		print 'Multiple <jobid> tags found:'
		print doc
		return None
	jobid = jobids[0]
	if not jobid.hasChildNodes():
		print '<jobid> tag has no child node:'
		print doc
		return None
	jobid = jobid.firstChild
	if jobid.nodeType != xml.dom.Node.TEXT_NODE:
		print 'job id is not a text node:'
		print doc
		return None
	jobid = int(jobid.data)
	if jobid == -1:
		# Error: find error message.
		print 'Failed to submit query.  Looking for error message...'
		founderr = False
		msgs = xmldoc.getElementsByTagName('message')
		for msg in msgs:
			if msg.hasChildNodes():
				c = msg.firstChild
				if c.nodeType == xml.dom.Node.TEXT_NODE:
					print 'Error message:', c.data
					founderr = True
		if not founderr:
			print 'Error message not found.  Whole response document:'
			print
			print doc
			print
	return jobid

def login(username, password):
	print 'Logging in.'
	data = urllib.urlencode({'userid': username, 'password': password})
	f = urllib2.urlopen(login_url(), data)
	d = f.read()
	#headers = f.info()
	#print 'headers:', headers
	#print 'Got response:'
	#print d
	return None

def cancel_job(jobid):
	data = urllib.urlencode({'id': jobid, 'CancelJob': 'Cancel Job'})
	f = urllib2.urlopen(cancel_url(), data)
	f.read()

# Returns 'Finished', 'Ready', 'Started', 'Failed', 'Cancelled'
def get_job_status(jobid):
	#print 'Getting job status for', jobid
	url = job_details_url() % jobid
	doc = urllib2.urlopen(url).read()
	for line in doc.split('\n'):
		for stat in ['Finished', 'Ready', 'Started', 'Failed', 'Cancelled']:
			if ('<td > <p class = "%s">%s</p></td>' %(stat,stat) in line or
				'<td > <p class="%s">%s</p></td>' %(stat,stat) in line or
				'<td class="center"> <p class = "%s">%s</p></td>' %(stat,stat) in line or
				'<td class="center"> <p class = "%s">Running</p></td>' %(stat) in line): # Galex "Started"/Running
				return stat
	return None

def get_viewstate_and_eventvalidation(doc):
	rex = re.compile('<input type="hidden" name="__VIEWSTATE" (?:id="__VIEWSTATE" )?value="(.*)" />')
	vs = None
	for line in doc.split('\n'):
		m = rex.search(line)
		if not m:
			continue
		vs = m.group(1)
		break

	rex = re.compile('<input type="hidden" name="__EVENTVALIDATION" id="__EVENTVALIDATION" value="(.*)" />')
	ev = None
	for line in doc.split('\n'):
		m = rex.search(line)
		if not m:
			continue
		ev = m.group(1)
		break

	return (vs,ev)

def drop_table(dbname):
	url = drop_url() % dbname
	try:
		f = urllib2.urlopen(url)
	except Exception,e:
		print 'Failed to drop table', dbname
		print e
		return False
	doc = f.read()
	(vs,ev) = get_viewstate_and_eventvalidation(doc)
	data = urllib.urlencode({'yesButton':'Yes',
							 '__EVENTVALIDATION':ev,
							 '__VIEWSTATE':vs})
	print 'Dropping table', dbname
	try:
		f = urllib2.urlopen(url, data)
	except Exception,e:
		print 'Failed to drop table', dbname
		print e
		return False
	d = f.read()
	#print 'Got response:'
	#print d
	#write_file(d, 'res.html')
	return True


	
def request_output(mydbname):
	url = mydb_action_url() % mydbname
	#print 'url', url
	try:
		## Need to prime the VIEWSTATE by "clicking" through...
		f = urllib2.urlopen(mydb_url())
		f.read()
		f = urllib2.urlopen(mydb_index_url())
		f.read()
		#request = urllib2.Request(url)
		#request.add_header('User-Agent', 'Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.9.0.11) Gecko/2009060214 Firefox/3.0.11')
		#f = urllib2.urlopen(request)
		#Referer: http://galex.stsci.edu/casjobs/mydbindex.aspx
		f = urllib2.urlopen(url)
	except urllib2.HTTPError,e:
		print 'HTTPError:', e
		print '  code', e.code
		print '  msg', e.msg
		print '  hdrs', e.hdrs
		print '  data:', e.fp.read()
		raise e
	doc = f.read().strip()
	#write_file(doc, 'r1.html')
	(vs,ev) = get_viewstate_and_eventvalidation(doc)
	data = {'extractDDL':'FITS', 'Button1':'Go',
			'__VIEWSTATE':vs}
	if ev is not None:
		data['__EVENTVALIDATION'] = ev
	extra = cas_params.get('request_output_extra')
	if extra is not None:
		data.update(extra)
		print 'requesting FITS output of MyDB table', mydbname
	#print 'url', url
	#print 'data', urllib.urlencode(data)
	try:
		f = urllib2.urlopen(url, urllib.urlencode(data))
	except urllib2.HTTPError,e:
		print 'HTTPError:', e
		print '  code', e.code
		#print '  reason', e.reason
		raise e
	d = f.read()
	#print 'Got response:'
	#print d
	#write_file(d, 'res.html')
	return

def get_ready_outputs():
	f = urllib2.urlopen(output_url())
	doc = f.read()
	urls = []
	fns = []
	rex = re.compile('<a href="(' + re.escape(cas_params['outputbaseurl']) + '(.*))">Download</a>')
	for line in doc.split('\n'):
		m = rex.search(line)
		if not m:
			continue
		urls.append(m.group(1))
		fns.append(m.group(2))
	return (urls, fns)

def setup_cookies():
	cookie_handler = urllib2.HTTPCookieProcessor()
	opener = urllib2.build_opener(cookie_handler)
	# ...and install it globally so it can be used with urlopen.
	urllib2.install_opener(opener)

def find_new_outputs(durls, dfns, preurls):
	newurls = [u for u in durls if not u in preurls]
	newfns =  [f for (f,u) in zip(dfns,durls) if not u in preurls]
	return (newurls, newfns)


# Requests output of the given list of databases, waits for them to appear,
# downloads them, and writes them to the given list of local filenames.
#
# 'dbs' and 'fns' must be either strings, or lists of string of the same length.
#
# If 'dodelete' is True, the databases will be deleted after download.
#
def output_and_download(dbs, fns, dodelete=False, sleeptime=10):
	if type(dbs) is str:
		dbs = [dbs]
		assert(type(fns) is str)
		fns = [fns]

	print 'Getting list of available downloads...'
	(preurls,nil) = get_ready_outputs()
	for db in dbs:
		print 'Requesting output of', db
		request_output(db)
	while True:
		print 'Waiting for output to appear...'
		(durls,dfns) = get_ready_outputs()
		(newurls, newfns) = find_new_outputs(durls, dfns, preurls)
		print 'New outputs available:', dfns
		for (fn,db) in zip(fns,dbs):
			for (dfn,durl) in zip(newfns,newurls):
				# the filename will contain the db name.
				if not db in dfn:
					continue
				print 'Output', dfn, 'looks like it belongs to database', db
				print 'Downloading to local file', fn
				cmd = 'wget -O "%s" "%s"' % (fn, durl)
				print '  (running: "%s")' % cmd
				w = os.system(cmd)
				if not os.WIFEXITED(w) or os.WEXITSTATUS(w):
					print 'download failed.'
					return -1
				dbs.remove(db)
				fns.remove(fn)
				if dodelete:
					print 'Deleting database', db
					drop_table(db)
		if not len(dbs):
			break
		print 'Waiting...'
		time.sleep(sleeptime)
	return 0

if __name__ == '__main__':
	args = sys.argv[1:]

	if len(args) < 2:
		print '%s <username> <password> [command <args>]' % sys.argv[0]
		print
		print 'commands include:'
		print '   delete <database> [...]'
		print '   query  ( <sql> or <@sqlfile> ) [...]'
		print '   querywait  ( <sql> or <@sqlfile> ) [...]'
		print '     -> submit query and wait for it to finish'
		print '   output <database> [...]'
		print '     -> request that a database be output as a FITS table'
		print '   outputdownload <database> <filename> [...]'
		print '     -> request output, wait for it to finish, and download to <filename>'
		print '   outputdownloaddelete <database> <filename> [...]'
		print '     -> request output, wait for it to finish, download to <filename>, and drop table.'
		sys.exit(-1)

	if sys.argv[0].startswith('galex'):
		print 'Using GALEX CasJobs'
		cas_params = galex_cas

	instance = 4

	username = args[0]
	password = args[1]
	cmd = None
	if len(args) > 2:
		cmd = args[2]

	setup_cookies()

	login(username, password)

	if cmd == 'delete':
		if len(args) < 4:
			print 'Usage: ... delete <db>'
			sys.exit(-1)
		db = args[3]
		print 'Dropping', db
		drop_table(db)
		sys.exit(0)

	if cmd in ['query', 'querywait']:
		qs = args[3:]
		if len(qs) == 0:
			print 'Usage: ... query <sql> or <@file> [...]'
			sys.exit(-1)
		jids = []
		for q in qs:
			if q.startswith('@'):
				q = read_file(q[1:])
			jobid = submit_query(q)
			print 'Submitted job id', jobid
			jids.append(jobid)
		if cmd == 'querywait':
			# wait for them to finish.
			while True:
				print 'Waiting for job ids:', jids
				for jid in jids:
					jobstatus = get_job_status(jid)
					print 'Job id', jid, 'is', jobstatus
					if jobstatus in ['Finished', 'Failed', 'Cancelled']:
						jids.remove(jid)
				if not len(jids):
					break
				print 'Sleeping...'
				time.sleep(10)
		sys.exit(0)

	if cmd == 'output':
		dbs = args[3:]
		if len(dbs) == 0:
			print 'Usage: ... output <db> [<db> ...]'
			sys.exit(-1)
		for db in dbs:
			print 'Requesting output of db', db
			request_output(db)
		sys.exit(0)

	if cmd in ['outputdownload', 'outputdownloaddelete']:
		dodelete = (cmd == 'outputdownloaddelete')
		dbfns = args[3:]
		if len(dbfns) == 0 or len(dbfns) % 2 == 1:
			print 'Usage: ... outputdownload <db> <filename> [...]'
			sys.exit(-1)
		dbs = dbfns[0::2]
		fns = dbfns[1::2]

		output_and_download(dbs, fns, dodelete)
		sys.exit(0)

	statefile = 'submit.pickle'
	if os.path.exists(statefile):
		print 'Loading state from', statefile
		state = pickle.load(open(statefile))
	else:
		state = {}

	if cmd == 'setruns':
		runs = [int(a) for a in args[3:]]
		print 'Setting target list of runs to', runs
		jobids = [None] * len(runs)
		status = ['ready'] * len(runs)
		state['runs'] = runs
		state['jobids'] = jobids
		state['status'] = status
		pickle.dump(state, open(statefile, 'w'))
		sys.exit(0)

	if not 'runs' in state:
		# Get list of runs.
		print 'Getting list of runs...'
		sql = 'select distinct run from Field'
		(cols, runs) = query(sql)
		runs = list(array(runs).ravel())
		print 'runs', runs
		jobids = [None] * len(runs)
		status = ['ready'] * len(runs)
		state['runs'] = runs
		state['jobids'] = jobids
		state['status'] = status
		pickle.dump(state, open(statefile, 'w'))

	runs = state['runs']
	jobids = state['jobids']
	statuses = state['status']
	dbnames = ['run_%04i_v%i' % (r,instance) for r in runs]

	if cmd == 'cancelall':
		for i,stat in enumerate(statuses):
			if stat != 'waiting-query':
				continue
			jobstatus = get_job_status(jobids[i])
			if not jobstatus in ['Ready', 'Started']:
				continue
			print 'Cancelling job', jobids[i]
			cancel_job(jobids[i])
		sys.exit(0)
	elif cmd == 'resetall':
		# reset all 'fail's to 'ready's
		for i,stat in enumerate(statuses):
			if stat != 'failed':
				continue
			print 'Reset run', runs[i], 'to ready.'
			statuses[i] = 'ready'
		state['status'] = statuses
		pickle.dump(state, open(statefile, 'w'))
		sys.exit(0)
	elif cmd == 'sizes':
		for i,run in enumerate(runs):
			sql = 'select count(*) from RunsDB.PhotoObjAll where run=%i' % run
			(colnames, n) = query(sql)
			n = array(n).ravel()[0]
			print 'Run', run, 'has', n, 'rows'
			time.sleep(2)
		sys.exit(0)
	elif cmd == 'fail':
		failruns = args[3:]
		for f in failruns:
			r = int(f)
			print 'Marking run', r, 'as failed.'
			i = runs.index(r)
			statuses[i] = 'failed'
		state['status'] = statuses
		pickle.dump(state, open(statefile, 'w'))
		sys.exit(0)
			

	# Get list of available downloads
	# Download ones that haven't already been downloaded.
	# Delete the db when done.
	print 'Listing available downloads...'
	(urls,fns) = get_ready_outputs()
	for (url,fn) in zip(urls,fns):
		if os.path.exists(fn):
			#print 'Skipping ', fn
			continue
		for i,db in enumerate(dbnames):
			if not db in fn:
				continue
			print 'Download', fn, 'looks like it belongs to run', runs[i]
			print 'Downloading', url
			cmd = 'wget "%s"' % url
			os.system(cmd)
			#
			print 'Deleting db', db
			drop_table(db)
			statuses[i] = 'done'
			state['status'] = statuses
			pickle.dump(state, open(statefile, 'w'))

	# Find jobs that are finished and request outputs.
	print 'Finding finished jobs...'
	for i,stat in enumerate(statuses):
		if stat != 'waiting-query':
			continue
		jobstatus = get_job_status(jobids[i])
		if jobstatus in [ 'Failed', 'Cancelled']:
			print 'Error: job id', jobids[i], 'has status:', jobstatus
			statuses[i] = 'failed'
			state['status'] = statuses
			pickle.dump(state, open(statefile, 'w'))
			continue
		print 'Job id', jobids[i], '(run %i)' % runs[i], 'has status:', jobstatus
		if jobstatus != 'Finished':
			continue
		print 'Requesting output of run', runs[i]
		request_output(dbnames[i])
		statuses[i] = 'waiting-output'
		state['status'] = statuses
		pickle.dump(state, open(statefile, 'w'))
		
	bands = 'ugriz'
	ntarget = 1
	# Submit jobs to keep a target number in the "waiting-query" state.
	nrunning = sum([1 for s in statuses if s == 'waiting-query'])
	print 'Jobs running:', nrunning, ', target', ntarget
	for i,stat in enumerate(statuses):
		if nrunning >= ntarget:
			break
		if stat != 'ready':
			continue
		print 'Submitting query for run', runs[i]
		table = 'mydb.%s' % dbnames[i]
		sql = ('select into %s' % table
			   + ' run,field,camcol,rerun,'
			   + ','.join(['colc_%s' % b for b in bands] +
						  ['rowc_%s' % b for b in bands] +
						  ['%s' % b for b in bands])
			   + ' from PhotoObjAll where run=%i ' % runs[i]
			   + 'and mode in (1,2) and type in (3,6)')
		taskname = dbnames[i]
		jobid = submit_query(sql, table, taskname)
		nrunning += 1

		statuses[i] = 'waiting-query'
		jobids[i] = jobid
		state['status'] = statuses
		state['jobids'] = jobids
		pickle.dump(state, open(statefile, 'w'))
		
