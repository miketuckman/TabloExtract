#!/usr/bin/env python

# TabloExtractor, J. Kenney 2014
# Forked - M Tuckman 2014
# A script to pull videos off of the TabloTV OTA Recorder

# Based on information obtained at:
# 1. http://community.tablotv.com/discussion/226/can-i-pull-recorded-video-files-off-tablo
# 2. http://stackoverflow.com/questions/22676/how-do-i-download-a-file-over-http-using-python

# Usage: ./tablo2go.py <options> "search regex"
#  Options:    -tablo:IP_ADDR  tablo ip address (multiple tablos seperated by a colon)
#              -ffmpeg:PATH    path to ffmpeg (ex /bin/ffmpeg)
#              -db:file        Tablo Extractor Database File
#              -output:dir     Save final files here
#              -a              Reprocess ever 30 minutes, do not exit
#              -list           List videos on Tablo(s)
#              -handbrake      Post process with handbrake (and delete .mp4 file)
#  Note: Search Terms are optional and should be in a quote if more than one word.

# Example usage:
# ./tablo2go.py -db:/tmp/tablo2go.db -tablo:192.168.2.168 -ffmpeg:/src/ffmpeg/bin/ffmpeg -output:/share/Tablo -handbrake
# This will process all videos on the tablo at 192.168.2.168, further compress them, and store the in /share/Tablo/Series
# ./tablo2go.py -db:/tmp/tablo2go.db
# This will perform the same as the previous example as it will remember settings within the database
# ./tablo2go.py -db:/tmp/tablo2go.db -tablo:192.168.2.168:192.168.2.169 -ffmpeg:/src/ffmpeg/bin/ffmpeg -output:/share/Tablo Whose
# This will process the two tablos on 168, and 169 and only process those which match the search term "Whose"

# Process overview:
# 1. via get_list(IPADDR), view http://IPADDR:18080/pvr to determine what videos are available,
#    return a dictionary with {'ids': {'VIDEOID1':IPADDR, 'VIDEOID2':IPADDR, etc..}}
# 2. via get_meta(IPADDR, VIDEOID), view http://IPADDR:18080/pvr/VIDEOID/meta.txt to retrieve
#    associated metadata, returned in a dictionary {}
# 3. via proc_meta(IPADDR, VIDEOID, DATABASE), review metadata retrieved via step 2, searching
#    for series, season, episode, title, airdate, originalairdate, description, and recording
#    status and update database with this information.
# These are all called in turn via db_update(TABLOS, DB), which looks for new shows, aquires
# the metadata for only those shows that it has not processed before, processes the metadata,
# deletes shows that are no longer available from the database, revisists those that were
# recording previously.
# 4. if a search string is entered only process items that match, and that have finished recording,
#    and that have not been processed in the past.
# 5. via get_video(IPADDR, VIDEOID, DIRECTORY, FFMPEG, FILENAME), each .ts file at
#    http://IPADDR:18080/pvr/VIDEOID/segs is downloaded to tmp (with a prepended UUID), upon
#    completion, it is rebuild using the ffmpeg command noted on the discussion board
#    at http://community.tablotv.com/discussion/226/can-i-pull-recorded-video-files-off-tablo via
#    FFMPEG -i "concat:file1.ts|file2.ts|.." -bsf:a aac_adtstoasc -c copy "DIRECTORY/FILENAME.mp4"'
#    For TV shows this ends up as /DIRECTORY/Series_Name/Series_Name - S01E01 - Episode.mp4",
#    for Movies this would be /DIRECTORY/Movie_Name (Year).mp4".
# 6. if postprocessing is desired, HandBrakeCLI is called with (my traditional kmttg settings)
#    HandBrakeCLI -i "DIRECTORY/FILENAME.mp4" -f -a 1 -E copy -f mkv -O -e x264 -q 22.0
#    --loose-anamorphic --modulus 2 -m --x264-preset medium --h264-profile high --h264-level 4.1
#    --decomb --denoise=weak -v -o "'+DIRECTORY/FILENAME.mkv"
#    and the original mp4 is deleted.
# 7. Mark within the database as processed, move onto the next file, loop as necessary if
#    ran as a service.

#################################################################################################
# Import required libraries
VERSION = 0.23
import os,sys,string,time,urllib,uuid,re,subprocess,urllib2
global true, false
true, false = 1, 0
#DEBUG = true

#################################################################################################
# Function to get a list of video id's from a tablo - use pvr directory to get ids
# This will retrieve the list of video ids by parsing the directory names
def get_list(IPADDR):
    resp = urllib.urlopen('http://'+IPADDR+':18080/pvr').read()
    resp = string.splitfields(resp, '\n')
    videoids = {'ids':{}}
    for line in resp:
        if string.find(line, '<tr><td class="n"><a href="') == 0:
            line = string.splitfields(line, '<tr><td class="n"><a href="')[1]
            if line[0] != '.':
                line = string.splitfields(line, '/')[0]
                videoids['ids'][line] = IPADDR
    return videoids

#################################################################################################
# Function to get a metadata from a videoid from a specific tablo
def get_meta(IPADDR, VIDEOID):
    try:
        resp = urllib2.urlopen('http://'+IPADDR+':18080/pvr/'+str(VIDEOID)+'/meta.txt').read()
    except:
        resp= []
    metadata = ''
    for i in range(len(resp)):
        metadata = metadata + resp[i]
    metadata = eval(metadata)
    return metadata

#################################################################################################
# Function to download and rebuild the videofile
def get_video(IPADDR, VIDEOID, DIRECTORY, TEMPDIR, FFMPEG, FILENAME, DEBUG, TESTING, *ts):
    resp = urllib.urlopen('http://'+IPADDR+':18080/pvr/'+str(VIDEOID)+'/segs').read()
    final = string.splitfields(resp, '.ts')[:-1]
    final = string.splitfields(final[-1], '>')[-1]
    tmp = final
    while(tmp[0]) == '0':
        tmp = tmp[1:]
    final_int = eval(tmp)
    #temp_id = str(uuid.uuid4())+'-'
    temp_id = str(VIDEOID)+'-'
    counter = 1
    valid = 1
    concat = ''
    while valid:
        cmd = 'http://'+IPADDR+':18080/pvr/'+str(VIDEOID)+'/segs/'+string.zfill(counter,5)+'.ts'
        newfile = TEMPDIR+'/'+temp_id+string.zfill(counter,5)+'.ts'
        concat = concat + newfile+'|'
        if DEBUG: print '   - Retrieving '+cmd+ ' ('+str(int(float(counter)/float(final_int)*100.0))+'%)'
        urllib.urlretrieve(cmd, newfile)
        if string.zfill(counter,5) == final:
            valid = 0
        counter = counter + 1
        if TESTING and counter > 5:
            valid = 0 ## Only process first 5 segmant
    cmd = FFMPEG+' -y -i "concat:'+concat[:-1]+'" -bsf:a aac_adtstoasc -c copy "'+DIRECTORY+'/'+FILENAME+'.mp4"'
    if ts:
        cmd = FFMPEG+' -y -loglevel panic -i "concat:'+concat[:-1]+'" -c copy "'+DIRECTORY+'/'+FILENAME+'.ts"'
    if DEBUG: print cmd
    #os.system(cmd)
    subprocess.call(cmd)
    counter = 1
    valid = 1
    while valid:
        newfile = TEMPDIR+'/'+temp_id+string.zfill(counter,5)+'.ts'
        try:
            os.remove(newfile)
        except:
            ohwell = 1
            if DEBUG: print "Can't Delete " + newfile
        if string.zfill(counter,5) == final:
            valid = 0
        counter = counter + 1
    return 0

#################################################################################################
# Function to look at a dictionary
def print_dictionary(DICT, *LEVEL):
    if LEVEL:
        level = LEVEL[0]
    else:
        level = 0
    keys = DICT.keys()
    for key in keys:
        output = ''
        for i in range(level):
            output = output+'\t'
        if str(type(DICT[key])) == "<type 'dict'>":
            print output+key
            print_dictionary(DICT[key], level+1)
        else:
            print output+key+':'+str(DICT[key])

#################################################################################################
# Function to remove forbidden characters from a string.
def clean(VALUE):
    BAD_CHARS = {'"':' ', '&':'+', '/':'-', '\\':'-', '|':'-', "'":"", '?':'', ':':'-', ',':'', u'\u2026':'', '@':'at ', u'\u2019':'',u'\xf8':''}
    results = ''
    for char in VALUE:
        if BAD_CHARS.has_key(char):
            results = results + BAD_CHARS[char]
        else:
            results = results + char
    return results

#################################################################################################
# Get a value from a dictionary via an input like "a.b.c.d.e"
def get_value(DICT, VKEYS, DEFAULT):
    if str(type(DICT)) != "<type 'dict'>":
        return DEFAULT
    key_top = string.splitfields(VKEYS, '.')[0]
    if not DICT.has_key(key_top):
        return DEFAULT
    key_bottom = ''
    key_index = string.find(VKEYS, '.')
    if key_index != -1:
        key_bottom = VKEYS[key_index+1:]
    if key_bottom == '':
        return DICT[key_top]
    else:
        return get_value(DICT[key_top], key_bottom, DEFAULT)

#################################################################################################
# Get primary metadata fields (series name, episode name, season number, episode number, etc)
def proc_meta(IPADDR, VIDEOID, DB):
    PROC = {'transfered':0}
    metadata = DB[IPADDR][VIDEOID]
    PROC['status']   = get_value(metadata, 'recMovieAiring.jsonForClient.video.state','unknown')
    PROC['airdate']  = get_value(metadata, 'recMovieAiring.jsonForClient.airDate','')
    PROC['desc']     = get_value(metadata, 'recMovie.jsonForClient.plot', '')
    PROC['title']    = get_value(metadata, 'recMovie.jsonForClient.title','')
    PROC['date']     = get_value(metadata, 'recMovie.jsonForClient.releaseYear','')
    PROC['series']   = get_value(metadata, 'recSeries.jsonForClient.title',PROC['title'])
    PROC['season']   = get_value(metadata, 'recEpisode.jsonForClient.seasonNumber','0')
    PROC['episode']  = get_value(metadata, 'recEpisode.jsonForClient.episodeNumber','0')
    PROC['title']    = get_value(metadata, 'recEpisode.jsonForClient.title',PROC['title'])
    PROC['desc']     = get_value(metadata, 'recEpisode.jsonForClient.description',PROC['desc'])
    PROC['airdate']  = get_value(metadata, 'recEpisode.jsonForClient.originalAirDate',PROC['airdate'])
    PROC['date']     = get_value(metadata, 'recEpisode.jsonForClient.airDate',PROC['date'])
    PROC['status']   = get_value(metadata, 'recEpisode.jsonForClient.video.state', PROC['status'])
    if metadata.has_key('recSeason'):   # is a TV show!!!!
        PROC['type'] = 'tv'
        if string.zfill(PROC['episode'],2) == "00":
            PROC['name'] = PROC['series'] + ' - '+ PROC['date'][:10]
        else:
            PROC['name'] = PROC['series'] + ' - S'+string.zfill(PROC['season'],2)+'E'+string.zfill(PROC['episode'],2)
        if PROC['title'] != '':
            PROC['name'] = PROC['name'] + ' - '+PROC['title']
    else:                               # is a Movie!!
        PROC['type'] = 'movie'
        PROC['name'] = PROC['title']+ ' (' +str(PROC['date']) + ')'
    PROC['clean']    = clean(PROC['name'])
    return PROC

#################################################################################################
# Loop through tablos searching for videos and update database to reflect
def db_update(TABLOS, DB):
    found_count, add_count, del_count, proc_count = 0,0,0,0
    for IP in TABLOS:
        if not DB.has_key(IP):
            DB[IP] = {}
        videoids = get_list(IP)
        found_count = found_count + len(videoids['ids'])
        for ID in videoids['ids'].keys():
            if not DB[IP].has_key(ID):
                add_count = add_count + 1
                DB[IP][ID] = get_meta(IP, ID)
            elif DB[IP][ID]['proc']['status'] != 'finished':
                del(DB[IP][ID])
                DB[IP][ID] = get_meta(IP, ID)
        for ID in DB[IP].keys():
            if not videoids['ids'].has_key(ID):
                del_count = del_count + 1
                del(DB[IP][ID])
        for ID in DB[IP].keys():
            if not DB[IP][ID].has_key('proc'):
                proc_count = proc_count + 1
                metadata = proc_meta(IP,ID,DB)
                DB[IP][ID]['proc'] = proc_meta(IP, ID, DB)
            if DB[IP][ID]['proc']['status'] != 'finished':
                proc_count = proc_count + 1
                metadata = proc_meta(IP,ID,DB)
                DB[IP][ID]['proc'] = proc_meta(IP, ID, DB)
    return DB, found_count, add_count, del_count, proc_count

#################################################################################################
# Load database from the hard drive if already created, lets not query over and over
def db_load(DATABASE_FILE):
    try:
        tmp = open(DATABASE_FILE).readlines()
        DB = eval(tmp[0])
    except:
        DB = {'complete':{}}
    return DB

#################################################################################################
# Simple print of shows
def db_print(TABLOS, DB):
    fields = ['airdate', 'series', 'season', 'episode', 'desc', 'status', 'transfered']
    fields_size = [10,30,4,4,30,10,4]
    for IP in TABLOS:
        print 'TabloTV '+str(IP)
        keys = DB[IP].keys()
        keys.sort()
        print string.ljust('ID', 8),
        for i in range(len(fields)):
            fs= fields_size[i]
            print string.ljust(fields[i], fs),
        print
        for ID in keys:
            #print string.ljust(str(IP),15),
            print string.ljust(str(ID),8),
            for i in range(len(fields)):
                f = DB[IP][ID]['proc'][fields[i]]
                fs= fields_size[i]
                print string.ljust(str(f)[:fs], fs),
            print

#################################################################################################
# Scriptable print of shows
def db_print_script(TABLOS, DB,CSV):
    fields = ['airdate', 'series', 'season', 'episode', 'desc', 'status', 'transfered']
    fields_size = [10,30,4,4,30,10,4]
    for IP in TABLOS:
        keys = DB[IP].keys()
        keys.sort()
        for ID in keys:
            print CSV+str(IP),
            print CSV+str(ID),
            for i in range(len(fields)):
                f = DB[IP][ID]['proc'][fields[i]]
                fs= fields_size[i]
                print CSV+string.strip(str(f)),
            print

#################################################################################################
# Save database to hard drive
def db_save(DATABASE_FILE, DB):
    tmp = open(DATABASE_FILE, 'w')
    tmp.write(str(DB)+'\n')
    tmp.close()
    return DB

#################################################################################################
# Begin the program for command line use vice library access
if __name__ == '__main__':
    TABLOS = []
    DATABASE = ''
    DIRECTORY = ''
    TEMPDIR = ''
    FFMPEG = ''
    SEARCH = ''
    SLEEP = '1800'
    DEBUG = 0
    TESTING = 0
    LOOP = 0
    LIST = 0
    CREATE_DIR = 1
    HANDBRAKE = 0
    MOVIES = 1
    TV = 1
    CSV = 0
    ONLY = []
    COMPLETE = 0
    
    #################################################################################################
    # Determine Command Line options
    
    CMDLINE_OPTIONS = {}        # items flagged with a -
    CMDLINE_PROG = sys.argv[0]  # name of this program
    FAIL = 0
    
    for item in sys.argv[1:]:
        item = string.strip(item)
        if item[0] == '-':
            tmp = string.splitfields(item[1:],':',1)
            CMDLINE_OPTIONS[string.lower(tmp[0])] = tmp[1:]
        else:
            SEARCH = SEARCH+item

    if CMDLINE_OPTIONS.has_key('a'):
        LOOP = 1
    if CMDLINE_OPTIONS.has_key('list'):
        LIST = 1
        LOOP = 0
    if CMDLINE_OPTIONS.has_key('handbrake'):
        HANDBRAKE = 1
    if CMDLINE_OPTIONS.has_key('movie') or CMDLINE_OPTIONS.has_key('movies'):
        TV = 0
        MOVIES = 1
    if CMDLINE_OPTIONS.has_key('tv'):
        TV = 1
        MOVIES = 0
    if CMDLINE_OPTIONS.has_key('debug'):
        DEBUG = 1
    if CMDLINE_OPTIONS.has_key('testing'):
        TESTING = 1
    if CMDLINE_OPTIONS.has_key('csv'):
        try:
            CSV = CMDLINE_OPTIONS['csv'][0]
        except:
            CSV = '|'
        DEBUG = 0
    if CMDLINE_OPTIONS.has_key('proc'):
        ONLY = CMDLINE_OPTIONS['proc']
    if CMDLINE_OPTIONS.has_key('c') or CMDLINE_OPTIONS.has_key('complete'):
        COMPLETE = 1
    if CMDLINE_OPTIONS.has_key('db'):
        DATABASE = CMDLINE_OPTIONS['db'][0]
        DB = db_load(DATABASE)
        if DB.has_key('config') and false:
            if DB['config'].has_key('CMDLINE_OPTIONS'):
                CMDLINE_OPTIONS = DB['config']['CMDLINE_OPTIONS']
                for item in sys.argv[1:]:
                    item = string.strip(item)
                    if item[0] == '-':
                        tmp = string.splitfields(item[1:],':')
                        CMDLINE_OPTIONS[string.lower(tmp[0])] = tmp[1:]
    if CMDLINE_OPTIONS.has_key('tablo'):
        TABLOS = CMDLINE_OPTIONS['tablo']
    if CMDLINE_OPTIONS.has_key('ffmpeg'):
        FFMPEG = CMDLINE_OPTIONS['ffmpeg'][0]
    if CMDLINE_OPTIONS.has_key('output'):
        DIRECTORY = CMDLINE_OPTIONS['output'][0]
    if CMDLINE_OPTIONS.has_key('temp'):
        TEMPDIR = CMDLINE_OPTIONS['temp'][0]
    if CMDLINE_OPTIONS.has_key('sleep'):
        SLEEP = CMDLINE_OPTIONS['sleep'][0]

        if DEBUG: print 'DB:'+DATABASE
        if DEBUG: print TABLOS
        if DEBUG: print 'FFMPEG:'+FFMPEG
        if DEBUG: print 'DIR:'+DIRECTORY
        if DEBUG: print 'TEMP:'+TEMPDIR
    
    if FAIL or DATABASE == '' or TABLOS == [] or FFMPEG == '' or DIRECTORY == '' or TEMPDIR == '':
        print 'Tablo Extractor (Version '+str(VERSION)+')'
        print ' Usage: '+sys.argv[0]+' <options> "search regex"'
        print ' Options:    -tablo:IP_ADDR        tablo ip address (multiple tablos seperated by a colon)'
        print '             -ffmpeg:PATH          path to ffmpeg (ex /bin/ffmpeg)'
        print '             -db:file              Tablo Extractor Database File'
        print '             -output:dir           Save final files here'
        print '             -temp:dir             Temporary working directory for received files'
        print '             -a                    Reprocess ever 30 minutes, do not exit'
        print '             -list                 List videos on Tablo(s)'
        print '             -csv                  List videos on Tablo(s) in a script readable format'
        print '                                   note: this sets debug/printing to off.'
        print '             -handbrake            Post process with handbrake (and delete .mp4 file)'
        print '             -tv                   Process only TV shows'
        print '             -movies               Process only Movies'
        print '             -proc:IP_ADDR:VIDEOID Process only specified file (only one)'
        print '             -c                    Mark matched videos as complete/transfered'
        print '             -debug                Display all msgs'
        print '             -testing              Only processes 1 segmant from Tablo - to test directories, etc - Faster'
        print '             -sleep                Number of seconds to sleep, used with -a'
        print ' Note: Search Terms are optional and should be in a quote if more than one word.'
        sys.exit()
    try:
        SEARCH_proc = re.compile(SEARCH, re.IGNORECASE)
    except:
        print 'Invalid search specification'
        sys.exit()
        
    #################################################################################################
    # Loop through tablos searching for videos

    while LOOP != 2:
        if LOOP == 0:
            LOOP = 2
        if DEBUG: print ' - Downloading data from TabloTVs'
        DB = db_load(DATABASE)
        DB, found_count, add_count, del_count, proc_count = db_update(TABLOS, DB)
        if not DB.has_key('config'):
            DB['config'] = {}
        DB['config']['CMDLINE_OPTIONS'] = CMDLINE_OPTIONS
        DB = db_save(DATABASE, DB)
        if DEBUG: print ' - Found '+str(found_count)+' video(s)'
        if DEBUG: print ' - Loaded metadata for '+str(add_count)+' newly discovered video(s)'
        if DEBUG: print ' - Updated metadata for '+str(proc_count)+' video(s)'
        if DEBUG: print ' - Removed metadata for '+str(del_count)+' deleted video(s)'

        if CSV != 0:
            db_print_script(TABLOS, DB, CSV)
            sys.exit()

        if LIST:
            if DEBUG: print ' - Listing videos found on Tablo(s)'
            db_print(TABLOS, DB)
            sys.exit()
            
    
        QUEUE = []
        count_found = 0
        count_finished = 0
        count_unprocessed = 0
        count_recording = 0
        count_transfered = 0
        for IP in TABLOS:
            keys = DB[IP].keys()
            keys.sort()
            for ID in keys:
                PROC = DB[IP][ID]['proc']
                match_status = SEARCH_proc.match(PROC['name'])
                match_status_orig = SEARCH_proc.match(PROC['clean'])
                match_search = 0
                if string.find(string.lower(PROC['name']), string.lower(SEARCH)) != -1:
                    match_search = 1
                if string.find(string.lower(PROC['clean']), string.lower(SEARCH)) != -1:
                    match_search = 1
                if match_status or match_status_orig:
                    match_search = 1
                if PROC['type'] == 'movie' and not MOVIES:
                    match_search = 0
                if PROC['type'] == 'tv' and not TV:
                    match_search = 0
                if ONLY != []:
                    match_search = 0
                    if IP == ONLY[0] and ID == ONLY[1]:
                        match_search = 1
                        QUEUE.append([IP,ID,PROC['clean'],PROC['status'],PROC['transfered'], PROC])
                        print ' - Only processing requested video'
                elif match_search:
                    count_found = count_found + 1
                    if PROC['status'] != 'finished':
                        count_recording = count_recording + 1
                    elif PROC['transfered'] != 'complete':
                        count_unprocessed = count_unprocessed + 1
                        QUEUE.append([IP,ID,PROC['clean'],PROC['status'],PROC['transfered'], PROC])
                    else:
                        count_transfered = count_transfered + 1
    
        if DEBUG: print ' - Search found '+str(count_found)+' match(es), '+str(count_recording)+' still recording, '+str(count_transfered)+' already done, '+str(count_unprocessed)+' to be downloaded.'
        if 1:
            for item in QUEUE:
                print '   - Match: '+item[2]
                if DEBUG: print ' DIR: '+DIRECTORY
                NDIR = DIRECTORY
                if CREATE_DIR and not COMPLETE:
                    if item[5]['type'] == 'tv':
                        SERIES = item[5]['series']
                        NDIR = DIRECTORY+'/'+clean(SERIES)
                        try:   
                            os.mkdir(NDIR)
                        except:
                            already_exists = 1
                if HANDBRAKE and not COMPLETE:
                    get_video(item[0], item[1], NDIR, TEMPDIR, FFMPEG, item[2], DEBUG, TESTING)    
                    cmd = 'HandBrakeCLI -i "'+NDIR+'/'+item[2]+'.mp4" -f -a 1 -E copy -f mkv -O -e x264 -q 22.0 --loose-anamorphic --modulus 2 -m --x264-preset medium --h264-profile high --h264-level 4.1 --decomb --denoise=weak -v -o "'+NDIR+'/'+item[2]+'.mkv"'
                    os.system(cmd)
                    try:
                        os.remove(NDIR+'/'+item[2]+'.mp4')
                    except:
                        ohwell = 1
                elif not COMPLETE:                    
                    get_video(item[0], item[1], NDIR, TEMPDIR, FFMPEG, item[2], DEBUG, TESTING)
                DB[item[0]][item[1]]['proc']['transfered'] = 'complete'
                DB = db_save(DATABASE, DB)

        if LOOP == 1:
            if DEBUG: print ' - Sleeping.'
            time.sleep(float(SLEEP))
