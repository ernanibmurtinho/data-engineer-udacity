import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *



def process_song_file(cur, filepath):
    """
    A function to load the song and artist dimension tables
    """
    # open song file
    
    #Loading frame var to all of the function - Dataframe
    frame = pd.read_json( filepath, lines=True )
    
    # insert artist record - The order was changed, because of the relationship - FK artist_table
    artist_data = frame.loc[ 0, ['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude'] ]
    artist_data = artist_data.values.tolist()
    
    cur.execute(artist_table_insert, artist_data)
    
    # insert song record

    columns_song = ['song_id', 'title', 'artist_id', 'year', 'duration']
    song_data = frame.loc[0,columns_song]
    song_data['year'] = song_data['year'].astype(str)
    song_data = song_data.tolist()

    cur.execute(song_table_insert, song_data)




def process_log_file(cur, filepath):
    """
    A function to load the fact table, songplay table
    The dimensions time and user will be loaded too
    """
    # open log file
    
    #Loading frame var to all of the function - Dataframe
    frame = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = frame[frame.page.eq('NextSong')]

    # convert timestamp column to datetime
    
    #Loading the respective columns with the appropriate values
    
    t = pd.DataFrame()
    t['ts'] = df['ts']
    t['ts_hour'] = pd.to_datetime(df['ts']).apply(lambda ts_hour : (ts_hour.hour))
    t['ts_day'] = pd.to_datetime(df['ts']).apply(lambda ts_day : (ts_day.day))
    t['ts_weekofyear'] = pd.to_datetime(df['ts']).apply(lambda ts_weekofyear : (ts_weekofyear.week))
    t['ts_month'] = pd.to_datetime(df['ts']).apply(lambda ts_month : (ts_month.month))
    t['ts_year'] = pd.to_datetime(df['ts']).apply(lambda ts_year : (ts_year.year))
    t['ts_weekday'] = pd.to_datetime(df['ts']).apply(lambda ts_weekday : (ts_weekday.dayofweek))
    
    # insert time data records
    time_df = t

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_index = ['userId', 'firstName', 'lastName', 'gender', 'level']
    user_df = frame[user_index]

    # insert user records
    user_df_not_na = user_df[user_df.firstName.notnull()]

    for i, row in user_df_not_na.iterrows():
        cur.execute(user_table_insert, list(row))


    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")    
    conn.commit()
    columns_songplay = ['song', 'artist', 'length', 'ts', 'userId', 'level',  'sessionId', 'location', 'userAgent']
    df = frame.loc[:, columns_songplay]
    df['length'] = df['length'].astype(str)

    # insert songplay records
    for index, row in df.iterrows():


        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length, ))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
            #Loading the songplay_data variable with (a key for the table is the union of songid + artistid)
            #The other variables, will load together with the static songid and artistid
            songplay_data = [songid, artistid, row.ts, row.userId, row.level, row.sessionId, row.location, row.userAgent]

        else:
            songid, artistid, row.ts, row.userId, row.level, row.sessionId, row.location, row.userAgent = None, None, None, None, None, None, None, None


    # insert songplay record
    songplay_data = [songid, artistid, row.ts, row.userId, row.level, row.sessionId, row.location, row.userAgent]
    cur.execute(songplay_table_insert, songplay_data)



def process_data(cur, conn, filepath, func):
    """
    A function to read recursively the paths and find the respective datasets to load the tables
    The function call the principal functions, process_song_file and process_log_file, in the main
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def import_df(filepath):
    """
    The purpose of this function (doesn't apply to this exercise) was help me to proccess all of the dataframes with the same
    structure, running just one time and concatenating all of them.
    """
    li = []
    global frame
    for filename in process_data(filepath):
        df = pd.read_json(filename, lines=True)
        li.append(df)
    frame = pd.concat(li, axis=0, ignore_index=True)



def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()