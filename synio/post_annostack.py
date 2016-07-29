import os
import argparse

from PIL import Image
import numpy as np

import ndio
from ndio.remote import neurodata
# setup ndio (note that because of the /ocp redirect, we force /nd)
nd = neurodata(hostname='synaptomes.neurodata.io/nd/')

def create_annotation_channel(token, channel):
    try:
        ret = nd.create_channel(name=channel, token=token, channel_type='annotation', dtype='uint32', readonly=False)
        if ret == True:
            print "Created channel for {}".format(channel)
        else:
            print "Error creating channel {}".format(channel)
    except ndio.remote.RemoteDataUploadError, e:
        print e

def ingest_annotation_slice(filename, token, channel, zindex):
    imdata = Image.open(filename, 'r')
    imarr = np.asarray(imdata, dtype=np.uint32)
    imarr = np.transpose(imarr)
    imstack = imarr[:,:,np.newaxis]
    try:
        ret = nd.post_cutout(token=token, channel=channel, x_start=0, y_start=0, z_start=zindex, data=imstack, resolution=0)
        if ret == True:
            print "Posted slice {} for channel {}".format(zindex, channel)
        else:
            print "Error posting slice {} for channel {}".format(zindex, channel)
    except ndio.remote.RemoteDataUploadError, e:
        print e

def ingest_annotation(token, channel, folder, zoffset, fixedslice=None):
    files = os.listdir(folder)
    for file in files:
        filename = file.split('.')[0]
        file_ext = file.split('.')[-1]
        if file_ext == 'tif':
            slice = int(filename.split("_")[1])
            if fixedslice:
                if fixedslice != slice:
                    continue
            ingest_annotation_slice("{}/{}".format(folder, file), token, channel, slice + zoffset)
    print "Done"

def main():
    parser = argparse.ArgumentParser('Post a annotation tiff stack to NDStore.')
    parser.add_argument('token', action='store', help='The NDStore token to associate with the data.')
    parser.add_argument('channel', action='store', help='A NDStore channel to store the data in. If the channel does not exist, data will be added to it.')
    parser.add_argument('folder', action='store', help='The path to the folder containing the tiff files.')
    parser.add_argument('--slice', action='store', type=int, help='An optional integer valued slice (relative to the folder hierarchy) to ingest. Useful for testing. Note that the slice index passed here will be modified by zoffset, if specified!')
    parser.add_argument('--zoffset', action='store', type=int, default=0, help='An optional value to add to the zindex from the tiff stack to move from tiff slice indexing to dataset indexing (e.g. a 1-indexed tiff stack needs `--zoffset -1` if the dataset is 0-indexed).')
    result = parser.parse_args()

    create_annotation_channel(result.token, result.channel)
    if result.slice:
        ingest_annotation(result.token, result.channel, result.folder, result.zoffset, fixedslice=result.slice)
    else:
        ingest_annotation(result.token, result.channel, result.folder, result.zoffset)
        # begin propagate (downsample)
        nd.propagate(result.token, result.channel)

if __name__ == '__main__':
    main()
