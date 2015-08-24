import urllib2
import json
import numpy as np
import zlib
import cStringIO


def loadOCPnpz(token):

    # get spatial domain
    try:
        f = urllib2.urlopen("http://brainviz1.cs.jhu.edu/ocp/ca/{}/info/".format(token))
    except urllib2.URLError, e:
        print e.code
        return 0


    metadata = json.loads(f.read())

    volumes = {}
    for res in metadata['dataset']['resolutions']:
        volumes[res] = metadata['dataset']['imagesize'][str(res)]

    # get cube
    cur_res = 2
    xmax = volumes[cur_res][0]
    ymax = volumes[cur_res][1]
    zmax = volumes[cur_res][2]

    try:
        url = "http://brainviz1.cs.jhu.edu/ocp/ca/{}/{}/npz/{}/0,{}/0,{}/0,{}/".format(token, 'rb_anti_synapsin', cur_res, xmax, ymax, zmax)
        f = urllib2.urlopen(url)
    except urllib2.URLError, e:
        print e.code
        return 0

    zdata = f.read()
    datastr = zlib.decompress( zdata[:] )
    datafobj = cStringIO.StringIO( datastr )
    cube = np.load(datafobj)
    return cube[0]


