###### Access mixpanel api for raw data dump of all user actions. Each month broken into 6 sections due to size/memory issues
###### JSON conversion fails (formatting error w/in the data) for large, unfiltered data dumps so saving as raw string, to be converted.

import hashlib
import urllib
import urllib2
import time
import pickle

try:
    import json
except ImportError:
    import simplejson as json

class Mixpanel(object):

    ENDPOINT = 'http://data.mixpanel.com/api'
    VERSION = '2.0'

    def __init__(self, api_key, api_secret):
        self.api_key = api_key
        self.api_secret = api_secret

    def request(self, methods, params, format='json'):
        """
            methods - List of methods to be joined, e.g. ['events', 'properties', 'values']
                      will give us http://mixpanel.com/api/2.0/events/properties/values/
            params - Extra parameters associated with method
        """
        params['api_key'] = self.api_key
        params['expire'] = int(time.time()) + 600   # Grant this request 10 minutes.
        params['format'] = format
        if 'sig' in params: del params['sig']
        params['sig'] = self.hash_args(params)

        request_url = '/'.join([self.ENDPOINT, str(self.VERSION)] + methods) + '/?' + self.unicode_urlencode(params)

	print request_url

        request = urllib2.urlopen(request_url, timeout=120)
        data = request.read()

        return data # Returns a raw string of all data

    def unicode_urlencode(self, params):
        """
            Convert lists to JSON encoded strings, and correctly handle any
            unicode URL parameters.
        """
        if isinstance(params, dict):
            params = params.items()
        for i, param in enumerate(params):
            if isinstance(param[1], list):
                params[i] = (param[0], json.dumps(param[1]),)

        return urllib.urlencode(
            [(k, isinstance(v, unicode) and v.encode('utf-8') or v) for k, v in params]
        )


    def hash_args(self, args, secret=None):
        """
            Hashes arguments by joining key=value pairs, appending a secret, and
            then taking the MD5 hex digest.
        """
        for a in args:
            if isinstance(args[a], list): args[a] = json.dumps(args[a])

        args_joined = ''
        for a in sorted(args.keys()):
            if isinstance(a, unicode):
                args_joined += a.encode('utf-8')
            else:
                args_joined += str(a)

            args_joined += '='

            if isinstance(args[a], unicode):
                args_joined += args[a].encode('utf-8')
            else:
                args_joined += str(args[a])

        hash = hashlib.md5(args_joined)

        if secret:
            hash.update(secret)
        elif self.api_secret:
            hash.update(self.api_secret)
        return hash.hexdigest()

if __name__ == '__main__':
    api = Mixpanel(
        api_key = '...',
        api_secret = '...'
    )
    for month in range(1,10):
      data = api.request(['export'], {
        'from_date' : '2014-0' + str(month) + '-01',
        'to_date' : '2014-0' + str(month) + '-05',
      })
      fName = '/pathToData/EventsRawData' + str(month) + '_01-05.pickle'
      f = open(fName, 'w')
      pickle.dump([data],f)
      f.close()
      print month, '1/6'

      data = api.request(['export'], {
        'from_date' : '2014-0' + str(month) + '-06',
        'to_date' : '2014-0' + str(month) + '-10',
      })
      fName = '/pathToData/EventsRawData' + str(month) + '_06-10.pickle'
      f = open(fName, 'w')
      pickle.dump([data],f)
      f.close()
      print month, '2/6'

      data = api.request(['export'], {
        'from_date' : '2014-0' + str(month) + '-11',
        'to_date' : '2014-0' + str(month) + '-15',
      })
      fName = '/pathToData/EventsRawData' + str(month) + '_11-15.pickle'
      f = open(fName, 'w')
      pickle.dump([data],f)
      f.close()
      print month, '3/6'

      data = api.request(['export'], {
        'from_date' : '2014-0' + str(month) + '-16',
        'to_date' : '2014-0' + str(month) + '-20',
      })
      fName = '/pathToData/EventsRawData' + str(month) + '_16-20.pickle'
      f = open(fName, 'w')
      pickle.dump([data],f)
      f.close()
      print month, '4/6'

   
      data = api.request(['export'], {
        'from_date' : '2014-0' + str(month) + '-21',
        'to_date' : '2014-0' + str(month) + '-25',
      })
      fName = '/pathToData/EventsRawData' + str(month) + '_21-25.pickle'
      f = open(fName, 'w')
      pickle.dump([data],f)
      f.close()
      print month, '5/6'
     

      endDate = '31'
      if month == 4 or month == 6 or month == 9 or month == 11:
        endDate = '30'
      if month == 2:
        endDate = '28'
      data = api.request(['export'], {
        'from_date' : '2014-0' + str(month) + '-26',
        'to_date' : '2014-0' + str(month) + '-' + endDate,
      })
      fName = '/pathToData/EventsRawData' + str(month) + '_26-end.pickle'
      f = open(fName, 'w')
      pickle.dump([data],f)
      f.close()
      print month, '6/6'

    
