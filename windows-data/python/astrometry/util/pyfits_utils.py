import pyfits
import numpy
from numpy import array, isscalar, ndarray

def add_nonstructural_headers(fromhdr, tohdr):
	for card in fromhdr.ascardlist():
		if ((card.key in ['SIMPLE','XTENSION', 'BITPIX', 'END', 'PCOUNT', 'GCOUNT',
						  'TFIELDS',]) or
			card.key.startswith('NAXIS') or
			card.key.startswith('TTYPE') or
			card.key.startswith('TFORM')):
			#card.key.startswith('TUNIT') or
			#card.key.startswith('TDISP')):
			#print 'skipping card', card.key
			continue
		#if tohdr.has_key(card.key):
		#	#print 'skipping existing card', card.key
		#	continue
		#print 'adding card', card.key
		#tohdr.update(card.key, card.value, card.comment, before='END')
		#tohdr.ascardlist().append(
		cl = tohdr.ascardlist()
		if 'END' in cl.keys():
			i = cl.index_of('END')
		else:
			i = len(cl)
		cl.insert(i, pyfits.Card(card.key, card.value, card.comment))


class tabledata(object):

	class td_iter(object):
		def __init__(self, td):
			self.td = td
			self.i = 0
		def __iter__(self):
			return self
		def next(self):
			if self.i >= len(self.td):
				raise StopIteration
			X = self.td[self.i]
			self.i += 1
			return X


	def __init__(self, header=None):
		self._length = 0
		self._header = header
		self._columns = []
	def __str__(self):
		return 'tabledata object with %i rows and %i columns' % (len(self), len([k for k in self.__dict__.keys() if not k.startswith('_')]))
	def about(self):
		keys = [k for k in self.__dict__.keys() if not k.startswith('_')]
		print 'tabledata object with %i rows and %i columns:' % (len(self),	 len(keys))
		keys.sort()
		for k in keys:
			print '	 ', k,
			v = self.get(k)
			print '(%s)' % (str(type(v))),
			if numpy.isscalar(v):
				print v
			elif hasattr(v, 'shape'):
				print 'shape', v.shape
			elif hasattr(v, '__len__'):
				print 'length', len(v)
			else:
				print v
			
	def __setattr__(self, name, val):
		object.__setattr__(self, name, val)
		#print 'set', name, 'to', val
		if (self._length == 0) and (not (name.startswith('_'))) and hasattr(val, '__len__') and len(val) != 0 and type(val) != str:
			self._length = len(val)
		if hasattr(self, '_columns') and not name in self._columns:
			self._columns.append(name)
	def set(self, name, val):
		self.__setattr__(name, val)
	def getcolumn(self, name):
		return self.__dict__[name]
		#except:
		#	return self.__dict__[name.lower()]
	def get(self, name):
		return self.getcolumn(name)
	# Returns the list of column names, as they were ordered in the input FITS or text table.
	def get_columns(self):
		return self._columns
	# Returns the original FITS header.
	def get_header(self):
		return self._header

	def columns(self):
		return [k for k in self.__dict__.keys() if not k.startswith('_')]
	def __len__(self):
		return self._length
	def delete_column(self, c):
		del self.__dict__[c]
		self._columns.remove(c)
	def __setitem__(self, I, O):
		if type(I) is slice:
			print 'I:', I
			# HACK... "[:]" -> slice(None, None, None)
			if I.start is None and I.stop is None and I.step is None:
				I = numpy.arange(len(self))
			else:
				I = numpy.arange(I.start, I.stop, I.step)
		for name,val in self.__dict__.items():
			if name.startswith('_'):
				continue
			# ?
			if numpy.isscalar(val):
				self.set(name, O.get(name))
				continue
			try:
				val[I] = O.get(name)
			except Exception:
				# HACK -- emulate numpy's boolean and int array slicing...
				ok = False
				#if type(I) == numpy.ndarray and hasattr(I, 'dtype') and I.dtype == bool:
				#	for i,b in enumerate(I):
				#		if b:
				#			val[i] = O.get(val)
				#	ok = True
				#if type(I) == numpy.ndarray and hasattr(I, 'dtype') and I.dtype == 'int':
				#	rtn.set(name, [val[i] for i in I])
				#	ok = True
				#if len(I) == 0:
				#	rtn.set(name, [])
				#	ok = True
				if not ok:
					print 'Error in slicing an astrometry.util.pyfits_utils.table_data object:'
					#print '  -->', e

					import pdb; pdb.set_trace()

					print 'While setting member:', name
					print ' setting elements:', I
					print ' from obj', O
					print ' target type:', type(O.get(name))
					print ' dest type:', type(val)
					print 'index type:', type(I)
					#if hasattr(val, 'shape'):
					#	print ' shape:', val.shape
					#if hasattr(I, 'shape'):
					#	print ' index shape:', I.shape
					if hasattr(I, 'dtype'):
						print '	 index dtype:', I.dtype
					print 'my length:', self._length
					raise Exception('error in fits_table indexing')

	def __getitem__(self, I):
		rtn = tabledata()
		if type(I) is slice:
			#print 'I:', I
			# Replace 'None' elements of the slice:
			i0 = I.start
			if i0 is None:
				i0 = 0
			i1 = I.stop
			if i1 is None:
				i1 = len(self)
			step = I.step
			if step is None:
				step = 1
			I = numpy.arange(i0, i1, step)
		for name,val in self.__dict__.items():
			if name.startswith('_'):
				continue
			if numpy.isscalar(val):
				rtn.set(name, val)
				continue
			if type(val) is numpy.ndarray:
			#try:
				rtn.set(name, val[I])
			elif type(val) is list and type(I) in [int, numpy.int64]:
				#print 'slice A', type(val), list, type(I)
				rtn.set(name, val[I])
				
			#except Exception as e:
			#except Exception:
			else:
				# HACK -- emulate numpy's boolean and int array slicing
				# (when "val" is a normal python list)
				ok = False
				if type(I) is numpy.ndarray and hasattr(I, 'dtype') and ((I.dtype.type in [bool, numpy.bool])
																		 or (I.dtype == bool)):
					#print 'slice C', name
					rtn.set(name, [val[i] for i,b in enumerate(I) if b])
					ok = True
				inttypes = [int, numpy.int64, numpy.int32, numpy.int]
				#if type(I) is numpy.ndarray and hasattr(I, 'dtype') and I.dtype.type in inttypes:
				if ok:
					pass
				elif type(I) is numpy.ndarray and all(I.astype(int) == I):
					#print 'slice D', name
					rtn.set(name, [val[i] for i in I])
					ok = True
				#if type(I) in inttypes:
				#	rtn.set(name, val[i])
				#	ok = True
				elif isscalar(I) and hasattr(I, 'dtype') and I.dtype in inttypes:
					#print 'slice E', name
					rtn.set(name, val[int(I)])
					ok = True
				elif hasattr(I, '__len__') and len(I) == 0:
					print 'slice F', name
					rtn.set(name, [])
					ok = True

				if not ok:
					print 'Error in slicing an astrometry.util.pyfits_utils.table_data object (__getitem__):'
					#print '  -->', e
					print 'While getting member:', name
					print ' by taking elements:', I
					#print ' from', val
					print ' from a:', type(val)
					if hasattr(val, 'shape'):
						print ' shape:', val.shape
					if hasattr(I, 'shape'):
						print ' index shape:', I.shape
					print 'index type:', type(I)
					if hasattr(I, 'dtype'):
						print '	 index dtype:', I.dtype
						print '	 index dtype has type:', type(I.dtype)
						print '	 index dtype.type:', I.dtype.type
						print 'options are', inttypes
						print 'I is numpy.ndarray?', (type(I) is numpy.ndarray)
						print 'has dtype?', (hasattr(I, 'dtype'))
						print 'I.dtype.type in inttypes:', (I.dtype.type in inttypes)
						print 'equal:', [(t, (I.dtype.type == t)) for t in inttypes]
						print 'I.astype(int)', I.astype(int)

						import pdb
						pdb.set_trace()

					print 'my length:', self._length
					raise Exception('error in fits_table indexing (table_data.__getitem__)')


			if isscalar(I):
				rtn._length = 1
			else:
				rtn._length = len(getattr(rtn, name))
		rtn._header = self._header
		if hasattr(self, '_columns'):
			rtn._columns = self._columns
		return rtn
	def __iter__(self):
		return tabledata.td_iter(self)

	def append(self, X):
		for name,val in self.__dict__.items():
			if name.startswith('_'):
				continue
			newX = numpy.append(val, X.getcolumn(name), axis=0)
			self.set(name, newX)
			self._length = len(newX)

	def write_to(self, fn, columns=None, header='default', primheader=None):
		if columns is None and hasattr(self, '_columns'):
			columns = self._columns
		T = pyfits.new_table(self.to_fits_columns(columns))
		if header == 'default':
			header = self._header
		if header is not None:
			add_nonstructural_headers(header, T.header)
		if primheader is not None:
			P = pyfits.PrimaryHDU()
			add_nonstructural_headers(primheader, P.header)
			pyfits.HDUList([P, T]).writeto(fn, clobber=True)
		else:
			T.writeto(fn, clobber=True)

	writeto = write_to

	def to_fits_columns(self, columns=None):
		cols = []

		fmap = {numpy.float64:'D',
				numpy.float32:'E',
				numpy.int32:'J',
				numpy.int64:'K',
				numpy.uint8:'B', #
				numpy.int16:'I',
				#numpy.bool:'X',
				#numpy.bool_:'X',
				numpy.bool:'L',
				numpy.bool_:'L',
				numpy.string_:'A',
				}

		if columns is None:
			columns = self.__dict__.keys()
				
		for name in columns:
			if name.startswith('_'):
				continue
			if not name in self.__dict__:
				continue
			val = self.__dict__.get(name)
			#print 'col', name, 'type', val.dtype, 'descr', val.dtype.descr
			#print repr(val.dtype)
			#print val.dtype.type
			#print repr(val.dtype.type)
			#print val.shape
			#print val.size
			#print val.itemsize
			if type(val) is list:
				val = array(val)
			fitstype = fmap.get(val.dtype.type, 'D')

			if fitstype == 'X':
				# pack bits...
				pass
			if len(val.shape) > 1:
				fitstype = '%i%s' % (val.shape[1], fitstype)
			elif fitstype == 'A' and val.itemsize > 1:
				# strings
				fitstype = '%i%s' % (val.itemsize, fitstype)
			else:
				fitstype = '1'+fitstype
			#print 'fits type', fitstype
			col = pyfits.Column(name=name, array=val, format=fitstype)
			cols.append(col)
			#print 'fits type', fitstype, 'column', col
			#print repr(col)
			#print 'col', name, ': data length:', val.shape
		return cols
		

def table_fields(dataorfn, rows=None, hdunum=1, header='default'):
	pf = None
	hdr = None
	if isinstance(dataorfn, str):
		pf = pyfits.open(dataorfn)
		data = pf[hdunum].data
		if header == 'default':
			hdr = pf[hdunum].header
	else:
		data = dataorfn

	if data is None:
		return None
	fields = tabledata(header=hdr)
	colnames = data.dtype.names
	for c in colnames:
		col = data.field(c)
		if rows is not None:
			col = col[rows]
		fields.set(c.lower(), col)
	fields._length = len(data)
	fields._columns = [c.lower() for c in colnames]
	if pf:
		pf.close()
	return fields

fits_table = table_fields

# ultra-brittle text table parsing.
def text_table_fields(forfn, text=None, skiplines=0, split=None, trycsv=True, maxcols=None):
	if text is None:
		f = None
		if isinstance(forfn, str):
			f = open(forfn)
			data = f.read()
			f.close()
		else:
			data = forfn.read()
	else:
		data = text
	txtrows = data.split('\n')

	txtrows = txtrows[skiplines:]

	# column names are in the first (un-skipped) line.
	txt = txtrows.pop(0)
	header = txt
	if header[0] == '#':
		header = header[1:]
	header = header.split()
	if len(header) == 0:
		raise Exception('Expected to find column names in the first row of text; got \"%s\".' % txt)
	#assert(len(header) >= 1)
	if trycsv and (split is None) and (len(header) == 1) and (',' in header[0]):
		# try CSV
		header = header[0].split(',')
	colnames = header

	fields = tabledata()
	txtrows = [r for r in txtrows if not r.startswith('#')]
	coldata = [[] for x in colnames]
	for i,r in enumerate(txtrows):
		if maxcols is not None:
			r = r[:maxcols]
		if split is None:
			cols = r.split()
		else:
			cols = r.split(split)
		if len(cols) == 0:
			continue
		if trycsv and (split is None) and (len(cols) != len(colnames)) and (',' in r):
			# try to parse as CSV.
			cols = r.split(',')
			
		if len(cols) != len(colnames):
			raise Exception('Expected to find %i columns of data to match headers (%s) in row %i; got %i\n	"%s"' % (len(colnames), ', '.join(colnames), i, len(cols), r))
		#assert(len(cols) == len(colnames))
		for i,c in enumerate(cols):
			coldata[i].append(c)

	for i,col in enumerate(coldata):
		isint = True
		isfloat = True
		for x in col:
			try:
				float(x)
			except:
				isfloat = False
				#isint = False
				#break
			try:
				int(x, 0)
			except:
				isint = False
				#break
			if not isint and not isfloat:
				break
		if isint:
			isfloat = False

		if isint:
			vals = [int(x, 0) for x in col]
		elif isfloat:
			vals = [float(x) for x in col]
		else:
			vals = col

		fields.set(colnames[i].lower(), array(vals))
		fields._length = len(vals)

	fields._columns = [c.lower() for c in colnames]

	return fields
